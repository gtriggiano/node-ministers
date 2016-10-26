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
export let isMinisterWorkersAvailability = msg => msg[2] === CONSTANTS.M_WORKERS_AVAILABILITY
export let isMinisterDisconnect = msg => msg[2] === CONSTANTS.M_DISCONNECT

export let isMinisterNotifierNewMinisterConnected = msg => msg[2] === CONSTANTS.MN_NEW_MINISTER_CONNECTED

export let ministerHelloMessage = (infos) => [
  CONSTANTS.MINISTER,
  CONSTANTS.M_HELLO,
  infos
]
export let ministerConnectedMessage = (infos) => [
  CONSTANTS.MINISTER_NOTIFIER,
  CONSTANTS.MN_NEW_MINISTER_CONNECTED,
  infos
]
export let ministerWorkersAvailabilityMessage = (workers) => [
  CONSTANTS.MINISTER,
  CONSTANTS.M_WORKERS_AVAILABILITY,
  workers
]
export let ministerDisconnectionMessage = (altentativeMinistersEndpoints) => [
  CONSTANTS.MINISTER,
  CONSTANTS.M_DISCONNECT,
  altentativeMinistersEndpoints
]
