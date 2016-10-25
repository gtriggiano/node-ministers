import { curry } from 'lodash'
import compose from 'lodash/fp/compose'
import isEqualFp from 'lodash/fp/isEqual'
import getFp from 'lodash/fp/get'

// Internals
const clientHasId = (clientId) => compose(isEqualFp(clientId), getFp('id'))

// External
const getClientInstance = ({router, id}) => {
  let client = {
    type: 'Client',
    id
  }

  Object.defineProperty(client, 'send', {value: (...frames) => router.send([id, ...frames])})

  return client
}

const findClientById = curry((clients, clientId) => clients.find(clientHasId(clientId)))

export {
  getClientInstance,
  findClientById
}
