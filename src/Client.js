import debug from 'debug'
import zmq from 'zmq'
import Rx from 'rxjs'
import uuid from 'uuid'
import EventEmitter from 'eventemitter3'
import { isString, isPlainObject, pull, compact, head, each } from 'lodash'
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
  findUnassignedRequests
} from './helpers/requests'

const Client = (settings) => {
  let log = debug('ministers:client')
  let client = new EventEmitter()

  let _settings = {...defaultSettings, ...settings}
  validateSettings(_settings)

  // Private API
  let _running = false
  let _starting = false
  let _stopping = false
  let _connected = false

  let _ministers = _settings.dnsMinister ? _settings.minister : [_settings.minister]
  let _lastMinisterEndpoint

  let _requests = []
  let _requestByUUID = findRequestByUUID(_requests)
  let _requestsUnassigned = findUnassignedRequests(_requests)

  // Worker messages
  let _onWorkerPartialResponse = (msg) => {
    let uuid = msg[2].toString()
    let request = _requestByUUID(uuid)
    if (request) request.givePartialResponse(msg[3])
  }
  let _onWorkerFinalResponse = (msg) => {
    let uuid = msg[2].toString()
    let request = _requestByUUID(uuid)
    if (request) request.giveFinalResponse(msg[3])
  }
  let _onWorkerErrorResponse = (msg) => {
    let uuid = msg[2].toString()
    let request = _requestByUUID(uuid)
    if (request) request.giveErrorResponse(msg[3])
  }
  // Minister messages
  let _onMinisterHeartbeat = () => {}
  let _onMinisterDisconnect = () => {
    _onMinisterLost()
  }

  // Dealer lifecycle management
  let _dealer
  let _unsubscribeFromDealer
  let _setupDealer = () => {
    _dealer = zmq.socket('dealer')
    _dealer.linger = 1
    _dealer.identity = `MC-${uuid.v4()}`
    _dealer.monitor(10, 0)

    if (_settings.security) {
      _dealer.curve_serverkey = _settings.security.serverPublicKey
      let clientKeys = _settings.security.secretKey
                          ? {public: _settings.security.publicKey, secret: _settings.security.secretKey}
                          : zmq.curveKeypair()
      _dealer.curve_publickey = clientKeys.public
      _dealer.curve_secretkey = clientKeys.secret
    }
    log('Dealer created')
  }
  let _tearDownDealer = () => {
    _dealer.unmonitor()
    _dealer.close()
    _dealer = null
    _lastMinisterEndpoint = null
    log('Dealer destroyed')
  }
  let _observeDealer = () => {
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

    let unsubscribe = () => each(subscriptions, subscription => subscription.unsubscribe())
    return unsubscribe
  }
  let _attemptConnectionToMinister = () => {
    _setupDealer()
    let getMinistersEndpoints
    if (isString(_ministers)) {
      let [ host, port ] = _ministers.split(':')
      getMinistersEndpoints = discoverMinistersEndpoints({
        host,
        port,
        excludedEndpoint: _lastMinisterEndpoint
      })
    } else {
      getMinistersEndpoints = Promise.resolve(_ministers)
    }

    getMinistersEndpoints
      .then(endpoints =>
        Promise.all(
          endpoints.map(endpoint => getMinisterLatency(endpoint)
            .then(latency => ({endpoint, latency}))
            .catch(() => false)
          )
        ).then(compact)
      )
      .then(sortByFp(getFp('latency'))).then(head)
      .then(({endpoint}) => {
        _lastMinisterEndpoint = endpoint
        _dealer.connect(endpoint)
        _dealer.once('connect', _onMinisterConnection)
        setTimeout(() => {
          if (_connected) return
          _tearDownDealer()
          _attemptConnectionToMinister()
        }, 1000)
      })
  }
  let _onMinisterConnection = () => {
    _connected = true
    client.emit('connection')
    _unsubscribeFromDealer = _observeDealer()
    _dealer.send(clientHelloMessage())
    _dispatchRequests()
  }
  let _onMinisterLost = () => {
    _connected = false
    client.emit('disconnection')
    _unsubscribeFromDealer()
    _unsubscribeFromDealer = null
    _attemptConnectionToMinister()
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

  // Public API
  function start () {
    if (_running || _starting) return client
    _starting = true
    _attemptConnectionToMinister()
    client.once('connection', () => {
      _starting = false
      _running = true
      client.emit('start')
    })
    return client
  }
  function stop () {
    if (!_running || _stopping) return client
    _stopping = true
    log('Sending disconnection message')
    _dealer.send(clientDisconnectionMessage())
    setTimeout(() => {
      _tearDownDealer()
      _stopping = false
      _running = false
      client.emit('stop')
    }, 1000)
    return client
  }
  function request (service, body, reqOptions) {
    reqOptions = isPlainObject(reqOptions) ? reqOptions : {}
    let options = {...defaultRequestOptions, reqOptions}
    let request = getClientRequestInstance({
      service,
      body,
      options,
      onFinished: () => pull(_requests, request)
    })
    _requests.push(request)
    process.nextTick(() => _dispatchRequests())
    return request
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
  retryOnLostWorker: false,
  retryOnLostConnection: false
}

const eMsg = prefixString('Minister(settings): ')
function validateSettings (settings) {
  let {minister, security} = settings

  // Minister
  let ministerErrorMessage = eMsg('settings.minister MUST be a string, representing either a DNS resolvable hostname or a valid TCP endpoint, in the form of \'tcp://IP:port\'')
  if (
    !isValidHostAndPort(minister) &&
    !isValidEndpoint(minister)
  ) throw new Error(ministerErrorMessage)
  settings.dnsMinister = isValidHostAndPort(minister)

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
