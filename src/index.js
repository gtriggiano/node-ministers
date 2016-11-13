import Client from './Client'
import Minister from './Minister'
import Worker from './Worker'
import * as CONSTANTS from './CONSTANTS'

let lib = {}
Object.defineProperties(lib, {
  Client: {enumerable: true, value: Client},
  Minister: {enumerable: true, value: Minister},
  Worker: {enumerable: true, value: Worker}
})
Object.keys(CONSTANTS).forEach(constName =>
  Object.defineProperty(lib, constName, {
    enumerable: true,
    value: CONSTANTS[constName]
  })
)

module.exports = lib
