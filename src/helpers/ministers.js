import dns from 'dns'
import Promise from 'bluebird'
import tcpPing from 'tcp-ping'
import { curry } from 'lodash'
import compose from 'lodash/fp/compose'
import isEqualFp from 'lodash/fp/isEqual'
import getFp from 'lodash/fp/get'
import filterFp from 'lodash/fp/filter'
import negateFp from 'lodash/fp/negate'

import MINISTERS from '../MINISTERS'

import {
  workerDoesService,
  workerCanWork
} from './workers'

// Internals
const concatPortToIPs = curry((port, ips) => ips.map(ip => `${ip}:${port}`))

const prependTransportToAddresses = curry((transport, addresses) => addresses.map(address => `${transport}://${address}`))

const discoverMinistersIPsByHost = (host) => new Promise((resolve, reject) => {
  dns.lookup(host, {family: 4, all: false}, (err, addresses) => {
    if (err) return reject(err)
    resolve(addresses.map(({address}) => address))
  })
})

const ministerHasId = (ministerId) => compose(isEqualFp(ministerId), getFp('id'))

const takeAddressesDifferentFromAddress = (address) => filterFp(compose(negateFp, isEqualFp(address)))

const ministerCanDoService = curry((service, minister) =>
  minister.workers.length &&
  minister.workers.filter(workerDoesService(service)).filter(workerCanWork)
)

// Exported
const getMinisterInstance = (router, id, latency) => {
  let minister = {
    type: 'Minister',
    id,
    latency,
    liveness: MINISTERS.HEARTBEAT_LIVENESS,
    workers: []
  }

  Object.defineProperty(minister, 'send', {value: (frames) => router.send([id, ...frames])})
  return minister
}

const findMinisterById = curry((ministers, ministerId) => ministers.find(ministerHasId(ministerId)))

const findAvailableMinisterForService = curry((ministers, service) =>
  ministers.filter(ministerCanDoService(service)).sort((m1, m2) =>
    m1.latency < m2.latency
      ? -1
      : m1.latency > m2.latency
        ? 1
        : 0
  )[0]
)

const discoverOtherMinistersEndpoints = ({host, port, ownAddress}) =>
  discoverMinistersIPsByHost(host)
  .then(concatPortToIPs(port))
  .then(prependTransportToAddresses('tcp'))
  .then(takeAddressesDifferentFromAddress(ownAddress))

const getMinisterLatency = (ministerEndpoint) => new Promise((resolve, reject) => {
  let [ address, port ] = ministerEndpoint.split('//')[1].split(':')
  tcpPing.ping({address, port, attempts: 5}, (err, {avg} = {}) => {
    if (err) return reject(err)
    resolve(Math.round(avg))
  })
})

export {
  getMinisterInstance,
  findMinisterById,
  findAvailableMinisterForService,
  discoverOtherMinistersEndpoints,
  getMinisterLatency
}
