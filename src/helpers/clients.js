import { curry } from 'lodash'
import compose from 'lodash/fp/compose'
import isEqualFp from 'lodash/fp/isEqual'
import getFp from 'lodash/fp/get'

// Internals
const clientHasId = (clientId) => compose(isEqualFp(clientId), getFp('id'))

// External
const getClientInstance = ({router, id}) => {
  let client = {}

  return Object.defineProperties(client, {
    type: {value: 'Client', enumerable: true},
    id: {value: id, enumerable: true},
    name: {value: id.substring(0, 11), enumerable: true},
    send: {value: (frames) => router.send([id, ...frames])}
  })
}

const findClientById = curry((clients, clientId) => clients.find(clientHasId(clientId)))

export {
  getClientInstance,
  findClientById
}
