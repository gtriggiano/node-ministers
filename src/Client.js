import D from 'debug'
import zmq from 'zmq'
import Rx from 'rxjs'
import uuid from 'uuid'
import EventEmitter from 'eventemitter3'
import {
  isPlainObject,
  isInteger,
  isString,
  pull,
  without,
  compact,
  uniq,
  head,
  last,
  each,
  noop
} from 'lodash'
import sortByFp from 'lodash/fp/sortBy'
import getFp from 'lodash/fp/get'

// Utils
import {
  isValidEndpoint,
  isValidHostAndPort,
  isValidCurveKey,
  isValidCurveKeyPair,
  prefixString
} from './helpers/utils'

// Messages
import {
  isWorkerMessage,
  isMinisterMessage,

  isWorkerPartialResponse,
  isWorkerFinalResponse,
  isWorkerErrorResponse,
  isMinisterHeartbeat,
  isMinisterDisconnect,

  clientHelloMessage,
  clientHeartbeatMessage,
  clientDisconnectionMessage
} from './helpers/messages'

// Ministers
import {
  discoverMinistersEndpoints,
  getMinisterLatency
} from './helpers/ministers'

// Requests
import {
  getClientRequestInstance,
  findRequestByUUID,
  findUnassignedRequests,
  findIdempotentRequests
} from './helpers/requests'

// Constants
import {
  HEARTBEAT_LIVENESS,
  HEARTBEAT_INTERVAL,
  RESPONSE_LOST_WORKER
} from './CONSTANTS'

const Client = (settings) => {
  let debug = D('ministers:client')
  let client = new EventEmitter()

  let _settings = {...defaultSettings, ...settings}
  validateSettings(_settings)

  // Private API
  // Client state
  let _running = false

  // Requests
  let _requests = []
  let _requestByUUID = findRequestByUUID(_requests)
  let _requestsUnassigned = findUnassignedRequests(_requests)
  let _requestsIdempotent = findIdempotentRequests(_requests)

  // Worker messages
  let _onWorkerPartialResponse = (msg) => {
    _monitorConnection()
    let uuid = msg[2].toString()
    let request = _requestByUUID(uuid)
    if (request) request.givePartialResponse(msg[3])
  }
  let _onWorkerFinalResponse = (msg) => {
    _monitorConnection()
    let uuid = msg[2].toString()
    let request = _requestByUUID(uuid)
    if (request) request.giveFinalResponse(msg[3])
  }
  let _onWorkerErrorResponse = (msg) => {
    _monitorConnection()
    let uuid = msg[2].toString()
    let request = _requestByUUID(uuid)
    if (request) {
      let error = JSON.parse(msg[3])
      if (
        error === RESPONSE_LOST_WORKER &&
        request.isIdempotent &&
        (request.isClean || request.canReconnectStream)
      ) {
        request.reschedule()
        return _dispatchRequests()
      }
      request.giveErrorResponse(msg[3])
    }
  }
  // Minister messages
  let _onMinisterHeartbeat = () => {
    if (!_running) return
    if (!_connected) _onConnectionSuccess()
    _monitorConnection()
  }
  let _onMinisterDisconnect = (msg) => {
    _knownEndpoints = JSON.parse(msg[2])
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
    _dealer.identity = `MC-${uuid.v4()}`

    if (_settings.security) {
      _dealer.curve_serverkey = _settings.security.serverPublicKey
      let clientKeys = _settings.security.secretKey
                          ? {public: _settings.security.publicKey, secret: _settings.security.secretKey}
                          : zmq.curveKeypair()
      _dealer.curve_publickey = clientKeys.public
      _dealer.curve_secretkey = clientKeys.secret
    }

    debug('Dealer created')
  }
  let _tearDownDealer = () => {
    if (!_dealer) return
    _dealer.close()
    _dealer = null
    debug('Dealer destroyed')
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
    let workerMessages = messages.filter(isWorkerMessage)
    let ministerMessages = messages.filter(isMinisterMessage)

    subscriptions.workerPartialResponse = workerMessages
      .filter(isWorkerPartialResponse).subscribe(_onWorkerPartialResponse)
    subscriptions.workerFinalResponse = workerMessages
      .filter(isWorkerFinalResponse).subscribe(_onWorkerFinalResponse)
    subscriptions.workerErrorResponse = workerMessages
      .filter(isWorkerErrorResponse).subscribe(_onWorkerErrorResponse)

    subscriptions.ministerHeartbeat = ministerMessages
      .filter(isMinisterHeartbeat).subscribe(_onMinisterHeartbeat)
    subscriptions.ministerDisconnect = ministerMessages.filter(isMinisterDisconnect).subscribe(_onMinisterDisconnect)

    if (_unsubscribeFromDealer) _unsubscribeFromDealer()
    _unsubscribeFromDealer = () => {
      each(subscriptions, subscription => subscription.unsubscribe())
      _unsubscribeFromDealer = noop
    }
  }

  // Connection management
  let _connected = false
  let _lastConnectedEndpoint
  let _dialedEndpoints = []
  let _knownEndpoints = _settings.dnsEndpoint ? [] : [_settings.endpoint]
  let _getMinistersEndpoints = () => {
    if (!_settings.dnsEndpoint) return Promise.resolve()
    let [ host, port ] = _settings.endpoint.split(':')
    return discoverMinistersEndpoints({
      host,
      port,
      excludedEndpoint: _lastConnectedEndpoint
    })
    .then(endpoints => _knownEndpoints = endpoints)
  }
  let _attemptConnection = () => {
    if (!_running) {
      _dialedEndpoints = []
      return debug('Connection attempt blocked because client is not running')
    }
    let attemptNo = _dialedEndpoints.length + 1

    _getMinistersEndpoints()
      .then(() => Promise.all(
        without(_knownEndpoints, ..._dialedEndpoints)
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
      .then(getFp('endpoint'))
      .then(endpoint => {
        if (!_running) {
          _dialedEndpoints = []
          return debug(`Connection attempt N° ${attemptNo} blocked because client is not running`)
        }
        if (_connected) return debug(`Connection attempt N° ${attemptNo} blocked because client is already connected`)
        if (endpoint) {
          _setupDealer()
          _subscribeToDealer()
          if (attemptNo === 1) client.emit('connecting')
          _dealer.connect(endpoint)
          _dealer.send(clientHelloMessage())
          _dialedEndpoints.push(endpoint)
          debug(`Connection attempt N° ${attemptNo} started`)
          setTimeout(() => {
            if (!_connected && _running) {
              debug(`Connection attempt N° ${attemptNo} is taking too long`)
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
    _lastConnectedEndpoint = last(_dialedEndpoints)
    _dialedEndpoints = []
    _monitorConnection()
    _startHeartbeats()
    _dispatchRequests()
    debug(`Connected to a minister`)
    client.emit('connection')
  }
  let _onConnectionFail = () => {
    _dialedEndpoints = []
    _unsubscribeFromDealer()
    _tearDownDealer()
    debug(`Could not connect to any minister`)
    if (!_settings.dnsEndpoint) {
      _knownEndpoints.push(_settings.endpoint)
      _knownEndpoints = uniq(_knownEndpoints)
    }
    client.emit('connection fail')
    if (_running) setTimeout(_attemptConnection, 1000)
  }
  let _onConnectionEnd = () => {
    _connected = false
    _unsubscribeFromDealer()
    _unmonitorConnection()
    _stopHeartbeats()
    _handleRequestsOnDisconnection()
    debug(`Disconnected from minister`)
    client.emit('disconnection')
  }

  // Connection monitoring
  let _connectionLiveness = 0
  let _connectionCheckInterval
  let _monitorConnection = () => {
    _unmonitorConnection()
    _connectionLiveness = HEARTBEAT_LIVENESS
    // debug(`${_connectionLiveness} remaining lives for connection`)
    _connectionCheckInterval = setInterval(() => {
      _connectionLiveness--
      // debug(`${_connectionLiveness} remaining lives for connection`)
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
  let _heartbeatMessage = clientHeartbeatMessage()
  let _startHeartbeats = () => {
    _heartbeatsInterval = setInterval(_sendHeartbeat, HEARTBEAT_INTERVAL)
  }
  let _stopHeartbeats = () => clearInterval(_heartbeatsInterval)
  let _sendHeartbeat = () => {
    if (_dealer) {
      debug('Sending hearbeat')
      _dealer.send(_heartbeatMessage)
    }
  }

  // Requests dispatching
  let _dispatchRequests = () => {
    if (!_connected) return
    _requestsUnassigned()
      .forEach(request => {
        request.assignee = true
        _dealer.send(request.frames)
      })
  }
  let _handleRequestsOnDisconnection = () => {
    let idempotentRequests = _requestsIdempotent()
    let notIdempotentRequests = without(_requests, ...idempotentRequests)

    idempotentRequests.forEach(request => {
      if (request.isClean || request.canReconnectStream) {
        request.reschedule()
      } else {
        request.signalLostWorker()
      }
    })

    notIdempotentRequests.forEach(request => request.signalLostWorker())
  }

  // Public API
  function start () {
    if (_running) return client
    _running = true
    process.nextTick(() => _attemptConnection())
    debug('Start')
    client.emit('start')
    return client
  }
  function stop () {
    if (!_running) return client
    _running = false
    _requests.forEach(request => request.abort())

    if (_connected) {
      _onConnectionEnd()
      debug('Sending disconnection message')
      _dealer.send(clientDisconnectionMessage())
      setTimeout(_tearDownDealer(), 500)
    }

    client.emit('stop')
    return client
  }
  function request (service, body, reqOptions) {
    if (!service || !isString(service)) throw new Error('service MUST be a nonempty string')
    let _eBody
    let _body = body instanceof Buffer ? body : null
    if (!_body) { try { _body = new Buffer(JSON.stringify(body)) } catch (e) { _eBody = e } }
    if (!_body) { try { _body = new Buffer(body) } catch (e) { _eBody = e } }
    if (!body) throw _eBody

    reqOptions = isPlainObject(reqOptions) ? reqOptions : {}
    let options = {...defaultRequestOptions, reqOptions}
    options.timeout = isInteger(options.timeout) && options.timeout > 0
      ? options.timeout : 0
    options.idempotent = !!options.idempotent
    options.reconnectStream = !!options.reconnectStream

    let request = getClientRequestInstance({
      service,
      options,
      body: _body,
      onFinished: () => pull(_requests, request)
    })

    _requests.push(request)
    process.nextTick(() => _dispatchRequests())
    return request.readableInterface
  }

  return Object.defineProperties(client, {
    start: {value: start},
    stop: {value: stop},
    request: {value: request}
  })
}

let defaultSettings = {}

let defaultRequestOptions = {
  timeout: 60000,
  idempotent: false,
  reconnectStream: false
}

const eMsg = prefixString('Minister(settings): ')
function validateSettings (settings) {
  let {endpoint, security} = settings

  // Endpoint
  let endpointErrorMessage = eMsg('settings.endpoint MUST be a string, representing either \'hostname:port\', where hostname will be resolved through DNS, or a valid TCP endpoint, in the form of \'tcp://IP:port\'')
  if (
    !isValidHostAndPort(endpoint) &&
    !isValidEndpoint(endpoint)
  ) throw new Error(endpointErrorMessage)
  settings.dnsEndpoint = isValidHostAndPort(endpoint)

  // Security
  if (security) {
    if (!isPlainObject(security)) throw new Error(eMsg('settings.security should be either `undefined` or a plain object'))
    if (!isValidCurveKey(security.serverPublicKey)) throw new Error(eMsg('settings.security.serverPublicKey MUST be a z85 encoded Curve25519 public key'))
    if (
      (security.secretKey || security.publicKey) &&
      !isValidCurveKeyPair(security.secretKey, security.publicKey)
    ) throw new Error(eMsg('settings.security.publicKey and settings.security.secretKey MUST be a valid z85 encoded Curve25519 keypair. You can generate them through zmq.curveKeypair()'))
  }
}

export default Client
