export const CLIENT = 'MC'
export const WORKER = 'MW'
export const MINISTER = 'MM'
export const MINISTER_NOTIFIER = 'MMN'

export const C_HELLO = '1'
export const C_HEARTBEAT = '2'
export const C_DISCONNECT = '3'
export const C_REQUEST = '4'

export const W_READY = '1'
export const W_HEARTBEAT = '2'
export const W_DISCONNECT = '3'
export const W_PARTIAL_RESPONSE = '4'
export const W_FINAL_RESPONSE = '5'
export const W_ERROR_RESPONSE = '6'

export const M_HELLO = '1'
export const M_HEARTBEAT = '2'
export const M_WORKERS_AVAILABILITY = '3'
export const M_DISCONNECT = '4'
export const M_REQUEST_LOST_STAKEHOLDER = '5'

export const MN_NEW_MINISTER_CONNECTED = '1'

export const HEARTBEAT_LIVENESS = 3
export const HEARTBEAT_INTERVAL = 1000

export const RESPONSE_TIMEOUT = 'RTO'
export const RESPONSE_LOST_WORKER = 'RLW'
