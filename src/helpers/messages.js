import * as CONSTANTS from '../CONSTANTS'

export let isClientMessage = msg => msg[1] === CONSTANTS.CLIENT
export let isWorkerMessage = msg => msg[1] === CONSTANTS.WORKER
export let isMinisterMessage = msg => msg[1] === CONSTANTS.MINISTER
export let isMinisterNotifierMessage = msg => msg[1] === CONSTANTS.MINISTER_NOTIFIER

export let isClientHello = msg => msg[2] === CONSTANTS.C_HELLO
export let isClientHeartbeat = msg => msg[2] === CONSTANTS.C_HEARTBEAT
export let isClientDisconnect = msg => msg[2] === CONSTANTS.C_DISCONNECT
export let isClientRequest = msg => msg[2] === CONSTANTS.C_REQUEST

export let isWorkerReady = msg => msg[2] === CONSTANTS.W_READY
export let isWorkerHeartbeat = msg => msg[2] === CONSTANTS.W_HEARTBEAT
export let isWorkerDisconnect = msg => msg[2] === CONSTANTS.W_DISCONNECT
export let isWorkerPartialResponse = msg => msg[2] === CONSTANTS.W_PARTIAL_RESPONSE
export let isWorkerFinalResponse = msg => msg[2] === CONSTANTS.W_FINAL_RESPONSE
export let isWorkerErrorResponse = msg => msg[2] === CONSTANTS.W_ERROR_RESPONSE

export let isMinisterHello = msg => msg[2] === CONSTANTS.M_HELLO
export let isMinisterHeartbeat = msg => msg[2] === CONSTANTS.M_HEARTBEAT
export let isMinisterWorkersAvailability = msg => msg[2] === CONSTANTS.M_WORKERS_AVAILABILITY
export let isMinisterDisconnect = msg => msg[2] === CONSTANTS.M_DISCONNECT

export let isMinisterNotifierNewMinisterConnected = msg => msg[2] === CONSTANTS.MN_NEW_MINISTER_CONNECTED

export let clientHelloMessage = () => [
  CONSTANTS.CLIENT,
  CONSTANTS.C_HELLO
]
export let clientHeartbeatMessage = () => [
  CONSTANTS.CLIENT,
  CONSTANTS.C_HEARTBEAT
]
export let clientDisconnectMessage = () => [
  CONSTANTS.CLIENT,
  CONSTANTS.C_DISCONNECT
]
export let clientRequestMessage = (reqUUID, service, options, body) => [
  CONSTANTS.CLIENT,
  CONSTANTS.C_REQUEST,
  reqUUID,
  service,
  options,
  body
]

export let workerReadyMessage = (infos) => [
  CONSTANTS.WORKER,
  CONSTANTS.W_READY,
  infos
]
export let workerHeartbeatMessage = (concurrency) => [
  CONSTANTS.WORKER,
  CONSTANTS.W_HEARTBEAT,
  concurrency
]
export let workerDisconnectMessage = () => [
  CONSTANTS.WORKER,
  CONSTANTS.W_DISCONNECT
]
export let workerPartialResponseMessage = (reqUUID, body) => [
  CONSTANTS.WORKER,
  CONSTANTS.W_PARTIAL_RESPONSE,
  reqUUID,
  body
]
export let workerFinalResponseMessage = (reqUUID, body) => [
  CONSTANTS.WORKER,
  CONSTANTS.W_FINAL_RESPONSE,
  reqUUID,
  body
]
export let workerErrorResponseMessage = (reqUUID, body) => [
  CONSTANTS.WORKER,
  CONSTANTS.W_ERROR_RESPONSE,
  reqUUID,
  body
]

export let ministerHelloMessage = (infos) => [
  CONSTANTS.MINISTER,
  CONSTANTS.M_HELLO,
  infos
]
export let ministerHeartbeatMessage = () => [
  CONSTANTS.MINISTER,
  CONSTANTS.M_HEARTBEAT
]
export let ministerWorkersAvailabilityMessage = (workers) => [
  CONSTANTS.MINISTER,
  CONSTANTS.M_WORKERS_AVAILABILITY,
  workers
]
export let ministerDisconnectMessage = (altentativeMinistersEndpoints) => [
  CONSTANTS.MINISTER,
  CONSTANTS.M_DISCONNECT,
  altentativeMinistersEndpoints
]

export let notifierNewMinisterConnectedMessage = (infos) => [
  CONSTANTS.MINISTER_NOTIFIER,
  CONSTANTS.MN_NEW_MINISTER_CONNECTED,
  infos
]
