import dns from 'dns'
import Promise from 'bluebird'
import tcpPing from 'tcp-ping'
import { curry } from 'lodash'
import compose from 'lodash/fp/compose'
import isEqualFp from 'lodash/fp/isEqual'
import getFp from 'lodash/fp/get'
import filterFp from 'lodash/fp/filter'
import negateFp from 'lodash/fp/negate'

import {
  parseEndpoint
} from './utils'

import {
  workerDoesService
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

const filterEndpointsDifferentFrom = (endpoint) => filterFp(compose(negateFp, isEqualFp(endpoint)))

const ministerDoesService = curry((service, minister) =>
  !!~minister.workers.map(({service}) => service).indexOf(service)
)

// Exported
export let getMinisterInstance = ({router, id, latency, endpoint}) => {
  let minister = {
    type: 'Minister',
    id,
    latency,
    endpoint,
    workers: [],
    assignedRequests: 0
  }

  Object.defineProperty(minister, 'send', {value: (frames) => router.send([id, ...frames])})
  Object.defineProperty(minister, 'slotsForService', {
    value: (service) =>
      minister.workers.filter(workerDoesService(service)).reduce((slots, {freeSlots}) => slots + freeSlots, 0)
  })
  return minister
}

export let findMinisterById = curry((ministers, ministerId) => ministers.find(ministerHasId(ministerId)))

export let findMinisterForService = curry((ministers, service) =>
  ministers.filter(ministerDoesService(service)).sort((m1, m2) => {
    let slots1 = m1.slotsForService(service)
    let slots2 = m2.slotsForService(service)

    return slots1 && !slots2
      ? -1
      : !slots1 && slots2
        ? 1
        : m1.latency < m2.latency
          ? -1
          : m1.latency > m2.latency
            ? 1
            : 0
  })[0]
)

export let discoverMinistersEndpoints = ({host, port, excludedEndpoint}) =>
  discoverMinistersIPsByHost(host)
  .then(concatPortToIPs(port))
  .then(prependTransportToAddresses('tcp'))
  .then(filterEndpointsDifferentFrom(excludedEndpoint))

export let getMinisterLatency = (ministerEndpoint) => new Promise((resolve, reject) => {
  let { ip, port } = parseEndpoint(ministerEndpoint)
  tcpPing.probe(ip, port, (err, available) => {
    if (err) return reject(err)
    if (!available) return reject()

    tcpPing.ping({address: ip, port, attempts: 5}, (err, {avg} = {}) => {
      if (err) return reject(err)
      resolve(Math.round(avg))
    })
  })
})
