import { curry } from 'lodash'
import compose from 'lodash/fp/compose'
import isEqualFp from 'lodash/fp/isEqual'
import getFp from 'lodash/fp/get'

import MINISTERS from '../MINISTERS'

// Internals
const clientHasId = (clientId) => compose(isEqualFp(clientId), getFp('id'))

// External
const getClientInstance = (router, clientId) => {
  let client = {
    type: 'Client',
    id: clientId,
    liveness: MINISTERS.HEARTBEAT_LIVENESS
  }

  Object.defineProperty(client, 'send', {value: (...frames) => router.send([clientId, ...frames])})

  return client
}

const findClientById = curry((clients, clientId) => clients.find(clientHasId(clientId)))

export {
  getClientInstance,
  findClientById
}
