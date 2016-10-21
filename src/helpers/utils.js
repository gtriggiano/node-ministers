import os from 'os'
import { curry, isInteger, isString } from 'lodash'

// Internals
const IPv4RegEx = /^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/

// Exported
const getOSNetworkExternalInterface = () => {
  let ifaces = os.networkInterfaces()
  return Object.keys(ifaces).reduce((ips, ifaceName) => {
    return ips.concat(ifaces[ifaceName].filter(address => !address.internal && address.family === 'IPv4'))
  }, []).map(({address}) => address)[0] || undefined
}

const isValidIPv4 = (str) => IPv4RegEx.test(str)

const isValidEndpoint = (endpoint) => {
  if (!isString(endpoint) || !endpoint) return false

  let [ transport, address ] = endpoint.split('://')
  if (!transport || !address) return false

  let [ ip, port ] = address.split(':')
  port = parseInt(port, 10)

  return transport === 'tcp' &&
          isValidIPv4(ip) &&
          isInteger(port) &&
          port > 0
}

const prefixString = curry((prefix, str) => `${prefix}${str}`)

export {
  getOSNetworkExternalInterface,
  isValidIPv4,
  isValidEndpoint,
  prefixString
}
