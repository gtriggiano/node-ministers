import zmq from 'zmq'
import Rx from 'rxjs'
import uuid from 'uuid'
import EventEmitter from 'eventemitter3'
import Promise from 'bluebird'
import {
  noop,
  without,
  compact,
  head,
  last,
  uniq,
  each
} from 'lodash'
import sortByFp from 'lodash/fp/sortBy'
import getFp from 'lodash/fp/get'

// Messages
import {
  isClientMessage,
  isWorkerMessage,
  isMinisterMessage,

  isClientRequest,
  isWorkerPartialResponse,
  isWorkerFinalResponse,
  isWorkerErrorResponse,
  isMinisterHeartbeat,
  isMinisterDisconnect
} from './messages'

// Utils
import {
  prefixString
} from './utils'

// Ministers
import {
  discoverMinistersEndpoints,
  getMinisterLatency
} from './ministers'

// Constants
import {
  HEARTBEAT_LIVENESS,
  HEARTBEAT_INTERVAL
} from '../CONSTANTS'

let dMsg = prefixString('Connection interface: ')

export function ClientConnection ({type, endpoint, DNSDiscovery, security, debug, getInitialMessage, getHearbeatMessage, getDisconnectionMessage}) {
  let connection = new EventEmitter()

  let _active = false
  let _connected = false
  let _isClient = type === 'Client'

  let _onMinisterHeartbeat = () => {
    if (!_active) return
    if (!_connected) _onConnectionSuccess()
  }
  let _onMinisterDisconnect = (msg) => {
    _phoneBook = JSON.parse(msg[2])
    _onConnectionEnd()
    _attemptConnection()
  }

  // Dealer lifecycle management
  let _dealer
  let _unsubscribeFromDealer = noop
  let _setupDealer = () => {
    if (_dealer) _tearDownDealer()
    _dealer = zmq.socket('dealer')
    _dealer.linger = 1
    _dealer.identity = `${_isClient ? 'MC' : 'MW'}-${uuid.v4()}`

    if (security) {
      _dealer.curve_serverkey = security.serverPublicKey
      let clientKeys = security.secretKey
                          ? {public: security.publicKey, secret: security.secretKey}
                          : zmq.curveKeypair()
      _dealer.curve_publickey = clientKeys.public
      _dealer.curve_secretkey = clientKeys.secret
    }

    debug(dMsg(`Dealer socket created`))
  }
  let _tearDownDealer = () => {
    if (!_dealer) return
    _dealer.close()
    _dealer = null
    debug(dMsg(`Dealer socket destroyed`))
  }
  let _subscribeToDealer = () => {
    // Collect a map of subscriptions
    let subscriptions = {}

    let subject = new Rx.Subject()
    let messages = Rx.Observable.fromEvent(_dealer, 'message', (side, msgType, ...frames) => [
      side && side.toString(),
      msgType && msgType.toString(),
      ...frames
    ]).multicast(subject).refCount()

    subscriptions.everyMessage = messages.subscribe(() => _monitorConnection())

    let ministerMessages = messages.filter(isMinisterMessage)
    subscriptions.ministerHeartbeat = ministerMessages
      .filter(isMinisterHeartbeat).subscribe(_onMinisterHeartbeat)
    subscriptions.ministerDisconnect = ministerMessages
      .filter(isMinisterDisconnect).subscribe(_onMinisterDisconnect)

    switch (type) {
      case 'Client':
        let workerMessages = messages.filter(isWorkerMessage)
        subscriptions.workerPartialResponse = workerMessages
          .filter(isWorkerPartialResponse).subscribe(msg => {
            let uuid = msg[2].toString()
            connection.emit('worker:partial:response', {uuid, body: msg[3]})
          })
        subscriptions.workerFinalResponse = workerMessages
          .filter(isWorkerFinalResponse).subscribe(msg => {
            let uuid = msg[2].toString()
            connection.emit('worker:final:response', {uuid, body: msg[3]})
          })
        subscriptions.workerErrorResponse = workerMessages
          .filter(isWorkerErrorResponse).subscribe(msg => {
            let uuid = msg[2].toString()
            connection.emit('worker:error:response', {uuid, error: JSON.parse(msg[3])})
          })
        break
      case 'Worker':
        let clientMessages = messages.filter(isClientMessage)
        subscriptions.clientRequest = clientMessages
          .filter(isClientRequest).subscribe(msg => {
            let uuid = msg[2] && msg[2].toString()
            let options = msg[4] && JSON.parse(msg[4])
            let body = msg[5] && JSON.parse(msg[5])
            connection.emit('client:request', {uuid, options, body})
          })
        break
    }

    if (_unsubscribeFromDealer) _unsubscribeFromDealer()
    _unsubscribeFromDealer = () => {
      each(subscriptions, subscription => subscription.unsubscribe())
      _unsubscribeFromDealer = noop
    }
  }

  // Connection management
  let _connection = null
  let _connectionLiveness = 0
  let _connectionCheckInterval
  let _attemptedConnections = []
  let _phoneBook = DNSDiscovery ? [] : [endpoint]
  let [ _host, _port ] = DNSDiscovery ? endpoint.split(':') : [null, null]
  let _compilePhonebook = () => {
    if (!DNSDiscovery) return Promise.resolve()
    return discoverMinistersEndpoints({
      host: _host,
      port: _port
    })
    .then(endpoints => _phoneBook = endpoints)
  }
  let _attemptConnection = () => {
    let attemptNo = _attemptedConnections.length + 1
    if (!_active) {
      _attemptedConnections = []
      return debug(dMsg(`Connection attempt N° ${attemptNo} blocked because connection was deactivated`))
    }

    _compilePhonebook()
      .then(() => Promise.all(
        without(_phoneBook, ..._attemptedConnections.map(getFp('endpoint')))
        .map(endpoint =>
          getMinisterLatency(endpoint)
            .then(latency => ({endpoint, latency}))
            .catch(() => {
              return false
            })
        )
      ))
      .then(compact)
      .then(sortByFp(getFp('latency')))
      .then(head)
      .then(conn => {
        if (!_active) {
          _attemptedConnections = []
          return debug(dMsg(`Connection attempt N° ${attemptNo} blocked because client is not running`))
        }
        if (_connected) return debug(dMsg(`Connection attempt N° ${attemptNo} blocked because connection was deactivated`))
        if (conn) {
          _setupDealer()
          _subscribeToDealer()
          if (attemptNo === 1) connection.emit('connecting')
          _dealer.connect(conn.endpoint)
          _dealer.send(getInitialMessage(conn))
          _attemptedConnections.push(conn)
          debug(dMsg(`Connection attempt N° ${attemptNo} started`))
          setTimeout(() => {
            if (!_connected && _active) {
              debug(dMsg(`Connection attempt N° ${attemptNo} is taking too long. Trying next...`))
              _attemptConnection()
            }
          }, HEARTBEAT_INTERVAL)
        } else {
          _onConnectionFail()
        }
      })
  }
  let _onConnectionSuccess = () => {
    _connected = true
    _connection = last(_attemptedConnections)
    _attemptedConnections = []
    _monitorConnection()
    debug(dMsg(`Connected to a minister at ${_connection.endpoint}`))
    _startHeartbeats()
    connection.emit('connection')
  }
  let _onConnectionFail = () => {
    _attemptedConnections = []
    _unsubscribeFromDealer()
    _tearDownDealer()
    debug(dMsg(`Could not connect to any minister`))
    if (!DNSDiscovery) {
      _phoneBook.push(endpoint)
      _phoneBook = uniq(_phoneBook)
    }
    connection.emit('connection:fail')
    if (_active) setTimeout(_attemptConnection, 1000)
  }
  let _onConnectionEnd = () => {
    _unsubscribeFromDealer()
    _unmonitorConnection()
    _stopHeartbeats()
    debug(dMsg(`Disconnected from minister at ${_connection.endpoint}`))
    _connected = false
    _connection = null
    connection.emit('disconnection')
  }
  let _monitorConnection = () => {
    _unmonitorConnection()
    _connectionLiveness = HEARTBEAT_LIVENESS
    // debug(eMsg`${_connectionLiveness} remaining lives for connection`)
    _connectionCheckInterval = setInterval(() => {
      _connectionLiveness--
      // debug(eMsg`${_connectionLiveness} remaining lives for connection`)
      if (!_connectionLiveness) {
        _onConnectionEnd()
        _attemptConnection()
      }
    }, HEARTBEAT_INTERVAL)
  }
  let _unmonitorConnection = () => {
    if (_connectionCheckInterval) {
      clearInterval(_connectionCheckInterval)
      _connectionCheckInterval = null
    }
  }

  // Heartbeats
  let _heartbeatsInterval
  let _startHeartbeats = () => {
    _sendHeartbeat()
    _heartbeatsInterval = setInterval(_sendHeartbeat, HEARTBEAT_INTERVAL)
  }
  let _stopHeartbeats = () => clearInterval(_heartbeatsInterval)
  let _sendHeartbeat = () => {
    if (_dealer) {
      debug(dMsg(`Sending hearbeat`))
      _dealer.send(getHearbeatMessage())
    }
  }

  let activate = () => {
    if (_active) return connection
    _active = true
    process.nextTick(() => {
      _attemptConnection()
    })
    debug(dMsg(`activated`))
    return connection
  }
  let deactivate = () => {
    if (!_active) return connection
    _active = false

    if (_connected) {
      _onConnectionEnd()
      debug(dMsg(`sending disconnection message to minister`))
      _dealer.send(getDisconnectionMessage())
      setTimeout(_tearDownDealer, 200)
    }

    debug(dMsg(`deactivated`))
    return connection
  }
  let send = (msg) => {
    if (!_connected) return false
    _dealer.send(msg)
    return true
  }

  return Object.defineProperties(connection, {
    activate: {value: activate},
    deactivate: {value: deactivate},
    send: {value: send},
    isConnected: {get () { return _connected }},
    isActive: {get () { return _active }}
  })
}
