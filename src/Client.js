import D from 'debug'
import EventEmitter from 'eventemitter3'
import {
  isPlainObject,
  isInteger,
  isString,
  pull,
  without,
  intersection
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
  clientDisconnectionMessage
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
  RESPONSE_LOST_WORKER
} from './CONSTANTS'

import { ClientConnection } from './helpers/ClientConnection'

const Client = (settings) => {
  let debug = D('ministers:client')
  let client = new EventEmitter()

  let _settings = {...defaultSettings, ...settings}
  validateSettings(_settings)

  // Private API
  let _active = false
  let _connection = ClientConnection({
    type: 'Client',
    endpoint: _settings.endpoint,
    DNSDiscovery: _settings.haveHostAndPortEndpoint,
    security: _settings.security,
    debug,
    getInitialMessage: clientHelloMessage,
    getHearbeatMessage: clientHeartbeatMessage,
    getDisconnectionMessage: clientDisconnectionMessage
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
        error === RESPONSE_LOST_WORKER &&
        request.isIdempotent &&
        (request.isClean || request.canReconnectStream)
      ) {
        request.reschedule()
        return process.nextTick(() => _dispatchRequests())
      }
      request.giveErrorResponse(error)
    }
  })
  _connection.on('connection', () => {
    _dispatchRequests()
    client.emit('connection')
  })
  _connection.on('disconnection', () => {
    let dispatchedRequests = _requestsDispatched()
    let idempotentRequests = _requestsIdempotent()
    let couldRescheduleRequests = intersection(dispatchedRequests, idempotentRequests)
    couldRescheduleRequests.forEach(request => {
      if (request.isClean || request.canReconnectStream) {
        request.reschedule()
      } else {
        request.signalLostWorker()
      }
    })
    let abortingRequests = without(dispatchedRequests, ...idempotentRequests)
    abortingRequests.forEach(request => request.signalLostWorker())
    client.emit('disconnection')
  })

  // Requests dispatching
  let _dispatchRequests = () => {
    _requestsNotDispatched()
      .forEach(request => request.isDispatched = _connection.send(request.frames))
  }

  // Public API
  function activate () {
    if (_active) return client
    _active = true
    _connection.activate()
    return client
  }
  function deactivate () {
    if (!_active) return client
    _active = false
    _connection.deactivate()
    return client
  }
  function request (service, reqBody, reqOptions) {
    if (!service || !isString(service)) throw new Error('service MUST be a nonempty string')

    let body = {}
    try { body = isPlainObject(reqBody) ? JSON.stringify(reqBody) : body } catch (e) {}

    reqOptions = isPlainObject(reqOptions) ? reqOptions : {}
    let options = {...defaultRequestOptions, reqOptions}
    options.timeout = isInteger(options.timeout) && options.timeout > 0
      ? options.timeout : 0
    options.idempotent = !!options.idempotent
    options.reconnectStream = !!options.reconnectStream

    let request = getClientRequestInstance({
      service,
      options,
      body,
      onFinished: () => pull(_requests, request)
    })

    _requests.push(request)
    process.nextTick(() => _dispatchRequests())
    return request.readableInterface
  }

  return Object.defineProperties(client, {
    activate: {value: activate},
    deactivate: {value: deactivate},
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
