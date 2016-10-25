import CONSTANTS from '../CONSTANTS'

const isClientMessage = msg => msg[1] === CONSTANTS.CLIENT
const isWorkerMessage = msg => msg[1] === CONSTANTS.WORKER
const isMinisterMessage = msg => msg[1] === CONSTANTS.MINISTER
const isMinisterNotifierMessage = msg => msg[1] === CONSTANTS.MINISTER_NOTIFIER

const isClientHello = msg => msg[2] === CONSTANTS.C_HELLO
const isClientHeartbeat = msg => msg[2] === CONSTANTS.C_HEARTBEAT
const isClientDisconnect = msg => msg[2] === CONSTANTS.C_DISCONNECT
const isClientRequest = msg => msg[2] === CONSTANTS.C_REQUEST

const isWorkerReady = msg => msg[2] === CONSTANTS.W_READY
const isWorkerHeartbeat = msg => msg[2] === CONSTANTS.W_HEARTBEAT
const isWorkerDisconnect = msg => msg[2] === CONSTANTS.W_DISCONNECT
const isWorkerPartialResponse = msg => msg[2] === CONSTANTS.W_PARTIAL_RESPONSE
const isWorkerFinalResponse = msg => msg[2] === CONSTANTS.W_FINAL_RESPONSE
const isWorkerErrorResponse = msg => msg[2] === CONSTANTS.W_ERROR_RESPONSE

const isMinisterHello = msg => msg[2] === CONSTANTS.M_HELLO
const isMinisterWorkersAvailability = msg => msg[2] === CONSTANTS.M_WORKERS_AVAILABILITY
const isMinisterDisconnect = msg => msg[2] === CONSTANTS.M_DISCONNECT

const isMinisterNotifierNewMinisterConnected = msg => msg[2] === CONSTANTS.MN_NEW_MINISTER_CONNECTED

export {
  isClientMessage,
  isWorkerMessage,
  isMinisterMessage,
  isMinisterNotifierMessage,

  isClientHello,
  isClientHeartbeat,
  isClientDisconnect,
  isClientRequest,

  isWorkerReady,
  isWorkerHeartbeat,
  isWorkerDisconnect,
  isWorkerPartialResponse,
  isWorkerFinalResponse,
  isWorkerErrorResponse,

  isMinisterHello,
  isMinisterWorkersAvailability,
  isMinisterDisconnect,

  isMinisterNotifierNewMinisterConnected
}
