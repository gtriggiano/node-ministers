import { curry } from 'lodash'
import compose from 'lodash/fp/compose'
import isEqualFp from 'lodash/fp/isEqual'
import getFp from 'lodash/fp/get'

// Internals
const clientHasId = (clientId) => compose(isEqualFp(clientId), getFp('id'))

// External
export let getClientInstance = ({router, id}) => {
  let client = {}

  return Object.defineProperties(client, {
    type: {value: 'client'},
    id: {value: id, enumerable: true},
    name: {value: id.substring(0, 11), enumerable: true},
    send: {value: (frames) => router.send([id, ...frames])},
    toJS: {value: () => ({id})}
  })
}

export let findClientById = curry((clients, clientId) => clients.find(clientHasId(clientId)))
