import D from 'debug'
import EventEmitter from 'eventemitter3'
import {
  isPlainObject,
  isInteger,
  isString,
  isFunction,
  pull,
  without,
  intersection,
  noop
} from 'lodash'

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
  clientHelloMessage,
  clientHeartbeatMessage,
  clientDisconnectMessage,
  clientDeactivateRequestMessage
} from './helpers/messages'

// Requests
import {
  getClientRequestInstance,
  findRequestByUUID,
  findNotDispatchedRequests,
  findDispatchedRequests,
  findIdempotentRequests
} from './helpers/requests'

// Constants
import {
  REQUEST_LOST_WORKER
} from './CONSTANTS'

import { ConnectionInterface } from './helpers/ConnectionInterface'

const Client = (settings) => {
  let debug = D('ministers:client')
  let client = new EventEmitter()

  let _settings = {...settings}
  validateSettings(_settings)

  // Private API
  let _active = false
  let _connection = ConnectionInterface({
    type: 'Client',
    endpoint: _settings.endpoint,
    DNSDiscovery: _settings.haveHostAndPortEndpoint,
    security: _settings.security,
    debug,
    getInitialMessage: clientHelloMessage,
    getHearbeatMessage: clientHeartbeatMessage,
    getDisconnectMessage: clientDisconnectMessage
  })

  // Requests
  let _requests = []
  let _requestByUUID = findRequestByUUID(_requests)
  let _requestsNotDispatched = findNotDispatchedRequests(_requests)
  let _requestsDispatched = findDispatchedRequests(_requests)
  let _requestsIdempotent = findIdempotentRequests(_requests)

  _connection.on('worker:partial:response', ({uuid, body}) => {
    let request = _requestByUUID(uuid)
    if (request) request.givePartialResponse(body)
  })
  _connection.on('worker:final:response', ({uuid, body}) => {
    let request = _requestByUUID(uuid)
    if (request) request.giveFinalResponse(body)
  })
  _connection.on('worker:error:response', ({uuid, error}) => {
    let request = _requestByUUID(uuid)
    if (request) {
      if (
        error === REQUEST_LOST_WORKER &&
        request.isIdempotent &&
        (request.isClean || request.canReconnectStream)
      ) {
        request.reschedule()
        return process.nextTick(() => _dispatchRequests())
      }
      request.giveErrorResponse(error)
    }
  })
  _connection.on('connection', (minister) => {
    _dispatchRequests()
    client.emit('connection', minister)
  })
  _connection.on('disconnection', minister => {
    let dispatchedRequests = _requestsDispatched()
    let idempotentRequests = _requestsIdempotent()
    let couldRescheduleRequests = intersection(dispatchedRequests, idempotentRequests)
    couldRescheduleRequests.forEach(request => {
      if (request.isClean || request.canReconnectStream) {
        request.reschedule()
      } else {
        request.lostWorker()
      }
    })
    let abortingRequests = without(dispatchedRequests, ...idempotentRequests)
    abortingRequests.forEach(request => request.lostWorker())
    process.nextTick(() => _dispatchRequests())
    client.emit('disconnection', minister)
  })

  // Requests dispatching
  let _dispatchRequests = () => {
    _requestsNotDispatched()
      .forEach(request => {
        request.isDispatched = _connection.send(request.frames)
        if (request.isDispatched) debug(`Dispatched request ${request.shortId}`)
      })
  }

  // Public API
  function start () {
    if (_active) return client
    _active = true
    debug('Start')
    _connection.activate()
    client.emit('start')
    return client
  }
  function stop () {
    if (!_active) return client
    _active = false
    _connection.deactivate()
    debug('Stop')
    client.emit('stop')
    return client
  }
  function request (service, reqBody, reqOptions) {
    if (!service || !isString(service)) throw new Error('service MUST be a nonempty string')

    let options = {...defaultRequestOptions, ...reqOptions}
    options.timeout = isInteger(options.timeout) && options.timeout > 0
      ? options.timeout : 0
    options.idempotent = !!options.idempotent
    options.reconnectStream = !!options.reconnectStream
    if (!options.idempotent) options.reconnectStream = false
    if (!isFunction(options.partialCallback)) options.partialCallback = noop
    if (!isFunction(options.finalCallback)) options.finalCallback = noop

    let request = getClientRequestInstance({
      service,
      options,
      bodyBuffer: JSON.stringify({...reqBody}),
      onFinished: () => pull(_requests, request),
      onDeactivation: () => {
        if (request.isDispatched) _connection.send(clientDeactivateRequestMessage(request.uuid))
      },
      debug
    })

    _requests.push(request)
    debug(`Created request ${request.shortId}`)
    process.nextTick(() => _dispatchRequests())
    return request.readableInterface
  }

  return Object.defineProperties(client, {
    start: {value: start},
    stop: {value: stop},
    request: {value: request}
  })
}

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
  settings.haveHostAndPortEndpoint = isValidHostAndPort(endpoint)

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
