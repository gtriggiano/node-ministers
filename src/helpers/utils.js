import net from 'net'
import os from 'os'
import nacl from 'tweetnacl'
import z85 from 'z85'
import zmq from 'zmq'
import { curry, isInteger, isString } from 'lodash'

// Exported
const getOSNetworkExternalInterface = () => {
  let ifaces = os.networkInterfaces()
  return Object.keys(ifaces).reduce((ips, ifaceName) => {
    return ips.concat(ifaces[ifaceName].filter(address => !address.internal && address.family === 'IPv4'))
  }, []).map(({address}) => address)[0] || undefined
}

const isValidEndpoint = (endpoint) => {
  if (!isString(endpoint) || !endpoint) return false

  let [ transport, address ] = endpoint.split('://')
  if (!transport || !address) return false

  let [ ip, port ] = address.split(':')
  port = parseInt(port, 10)

  return transport === 'tcp' &&
          net.isIPv4(ip) &&
          isInteger(port) &&
          port > 0
}

const isValidCurveKey = (key) => isString(key) && key.length === 40

const isValidCurveKeyPair = (secretKey, publicKey) => {
  if (!isValidCurveKey(secretKey) || !isValidCurveKey(publicKey)) return false
  var secretKey32 = z85.decode(secretKey)
  return publicKey === z85.encode(nacl.box.keyPair.fromSecretKey(secretKey32).publicKey)
}

const supportingCurveSecurity = () => {
  let socket = zmq.socket('router')
  try {
    socket.curve_server = 0
    socket.close()
    return true
  } catch (e) {
    socket.close()
    return false
  }
}

const prefixString = curry((prefix, str) => `${prefix}${str}`)

const parseEndpoint = (endpoint) => {
  let [ transport, address ] = endpoint.split('://')
  let [ ip, port ] = address.split(':')
  return {
    transport,
    ip,
    port: parseInt(port, 10)
  }
}

export {
  getOSNetworkExternalInterface,
  isValidEndpoint,
  isValidCurveKey,
  isValidCurveKeyPair,
  supportingCurveSecurity,
  prefixString,
  parseEndpoint
}
