import D from 'debug'
import EventEmitter from 'eventemitter3'
import {
  isString,
  isInteger,
  isPlainObject,
  pull
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
  workerReadyMessage,
  workerHeartbeatMessage,
  workerDisconnectMessage
} from './helpers/messages'

// Requests
import {
  getWorkerRequestInstance,
  findRequestByUUID
} from './helpers/requests'

import { ConnectionInterface } from './helpers/ConnectionInterface'

const Worker = (settings) => {
  let debug = D('ministers:worker')
  let worker = new EventEmitter()

  let _settings = {...defaultSettings, ...settings}
  validateSettings(_settings)

  // Private API
  let _active = false
  let _concurrency = _settings.concurrency
  let _connection = ConnectionInterface({
    type: 'Worker',
    endpoint: _settings.endpoint,
    DNSDiscovery: _settings.haveHostAndPortEndpoint,
    security: _settings.security,
    debug,
    getInitialMessage: ({latency}) => workerReadyMessage(JSON.stringify({
      service: _settings.service,
      concurrency: _concurrency,
      latency
    })),
    getHearbeatMessage: () => workerHeartbeatMessage(JSON.stringify({
      concurrency: _concurrency,
      pendingRequests: _requests.length
    })),
    getDisconnectMessage: workerDisconnectMessage
  })
  let _requests = []
  let _requestByUUID = findRequestByUUID(_requests)

  _connection.on('client:request', ({uuid, options, body}) => {
    let instance = getWorkerRequestInstance({
      connection: _connection,
      uuid,
      options,
      body,
      onFinished: () => {
        pull(_requests, instance)
        _connection.send(workerHeartbeatMessage(JSON.stringify({
          concurrency: _concurrency,
          pendingRequests: _requests.length
        })))
      }
    })
    _requests.push(instance)
    worker.emit('request', instance.request, instance.response)
  })
  _connection.on('request:lost:stakeholder', ({uuid}) => {
    let request = _requestByUUID(uuid)
    if (request) request.lostStakeholder()
  })
  _connection.on('connection', (minister) => {
    worker.emit('connection', minister)
  })
  _connection.on('disconnection', minister => {
    _requests.forEach(request => request.lostStakeholder())
    worker.emit('disconnection', minister)
  })

  // Public API
  let start = () => {
    if (_active) return worker
    _active = true
    debug('Start')
    _connection.activate()
    worker.emit('start')
    return worker
  }
  let stop = () => {
    if (!_active) return worker
    _active = false
    _connection.deactivate()
    debug('Stop')
    worker.emit('stop')
    return worker
  }

  return Object.defineProperties(worker, {
    start: {value: start},
    stop: {value: stop},
    concurrency: {
      get () { return _concurrency },
      set (val) {
        if (isInteger(val)) {
          _concurrency = val
          _connection.send(workerHeartbeatMessage(JSON.stringify({
            concurrency: _concurrency,
            pendingRequests: _requests.length
          })))
        }
        return _concurrency
      }
    }
  })
}

const defaultSettings = {
  concurrency: 1
}

const eMsg = prefixString('Worker(settings): ')
function validateSettings (settings) {
  let {service, concurrency, endpoint, security} = settings

  // Service
  if (!service || !isString(service)) throw new TypeError(eMsg('settings.service MUST be a nonempty string'))

  // Concurrency
  if (!isInteger(concurrency)) throw new TypeError(eMsg('settings.concurrency MUST be an integer'))

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

export default Worker
