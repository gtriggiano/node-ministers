import MINISTERS from '../MINISTERS'

const isClientMessage = msg => msg[1] === MINISTERS.CLIENT
const isWorkerMessage = msg => msg[1] === MINISTERS.WORKER
const isMinisterMessage = msg => msg[1] === MINISTERS.MINISTER
const isMinisterNotifierMessage = msg => msg[1] === MINISTERS.MINISTER_NOTIFIER

const isClientHello = msg => msg[2] === MINISTERS.C_HELLO
const isClientHeartbeat = msg => msg[2] === MINISTERS.C_HEARTBEAT
const isClientDisconnect = msg => msg[2] === MINISTERS.C_DISCONNECT
const isClientRequest = msg => msg[2] === MINISTERS.C_REQUEST

const isWorkerReady = msg => msg[2] === MINISTERS.W_READY
const isWorkerHeartbeat = msg => msg[2] === MINISTERS.W_HEARTBEAT
const isWorkerDisconnect = msg => msg[2] === MINISTERS.W_DISCONNECT
const isWorkerPartialResponse = msg => msg[2] === MINISTERS.W_PARTIAL_RESPONSE
const isWorkerFinalResponse = msg => msg[2] === MINISTERS.W_FINAL_RESPONSE
const isWorkerErrorResponse = msg => msg[2] === MINISTERS.W_ERROR_RESPONSE

const isMinisterHello = msg => msg[2] === MINISTERS.M_HELLO
const isMinisterWorkersAvailability = msg => msg[2] === MINISTERS.M_WORKERS_AVAILABILITY
const isMinisterDisconnect = msg => msg[2] === MINISTERS.M_DISCONNECT

const isMinisterNotifierNewMinisterConnected = msg => msg[2] === MINISTERS.MN_NEW_MINISTER_CONNECTED

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
