import net from 'net'
import os from 'os'
import nacl from 'tweetnacl'
import z85 from 'z85'
import zmq from 'zeromq'
import Promise from 'bluebird'
import { curry, isInteger, isString } from 'lodash'

let validHostnameRegex = /^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$/

// Exported
export let getOSNetworkExternalInterface = () => {
  let ifaces = os.networkInterfaces()
  return Object.keys(ifaces).reduce((ips, ifaceName) => {
    return ips.concat(ifaces[ifaceName].filter(address => !address.internal && address.family === 'IPv4'))
  }, []).map(({address}) => address)[0] || undefined
}

export let isValidEndpoint = (endpoint) => {
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

export let isValidHostname = (str) => validHostnameRegex.test(str)

export let isValidHostAndPort = (str) => {
  if (!str || !isString(str)) return false
  let [host, port] = str.split(':')
  port = parseInt(port, 10)
  return isValidHostname(host) && isInteger(port) && port > 0
}

export let isValidCurveKey = (key) => isString(key) && key.length === 40

export let isValidCurveKeyPair = (secretKey, publicKey) => {
  if (!isValidCurveKey(secretKey) || !isValidCurveKey(publicKey)) return false
  var secretKey32 = z85.decode(secretKey)
  return publicKey === z85.encode(nacl.box.keyPair.fromSecretKey(secretKey32).publicKey)
}

export let supportingCurveSecurity = () => {
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

export let prefixString = curry((prefix, str) => `${prefix}${str}`)

export let parseEndpoint = (endpoint) => {
  let [ transport, address ] = endpoint.split('://')
  let [ ip, port ] = address.split(':')
  return {
    transport,
    ip,
    port: parseInt(port, 10)
  }
}

export let defer = () => {
  let resolve, reject
  let promise = new Promise((rs, rj) => {
    resolve = rs
    reject = rj
  })
  return {resolve, reject, promise}
}
