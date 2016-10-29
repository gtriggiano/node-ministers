import D from 'debug'
import zmq from 'zmq'
import Rx from 'rxjs'
import EventEmitter from 'eventemitter3'

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
  isClientMessage,
  isMinisterMessage,

  isClientRequest,
  isMinisterHeartbeat,
  isMinisterDisconnect,

  workerReadyMessage,
  workerHeartbeatMessage,
  workerDisconnectMessage,
  workerPartialResponseMessage,
  workerFinalResponseMessage,
  workerFinalResponseMessage
} from './helpers/messages'

// Ministers
import {
  discoverMinistersEndpoints,
  getMinisterLatency
} from './helpers/ministers'

// Requests
import {
  getClientRequestInstance,
  findRequestByUUID
} from './helpers/requests'

// Constants
import {
  HEARTBEAT_LIVENESS,
  HEARTBEAT_INTERVAL,
  RESPONSE_LOST_WORKER
} from './CONSTANTS'

const Worker = (settings) => {}

const defaultSettings = {
  concurrency: 0
}

const eMsg = prefixString('Worker(settings): ')
function validateSettings (settings) {}

export default Worker
