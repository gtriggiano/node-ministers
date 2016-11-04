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
  dns.lookup(host, {family: 4, all: true}, (err, addresses) => {
    if (err) return reject(err)
    resolve(addresses.map(({address}) => address))
  })
})

const ministerHasId = (ministerId) => compose(isEqualFp(ministerId), getFp('id'))

const filterEndpointsDifferentFrom = (endpoint) => filterFp(negateFp(isEqualFp(endpoint)))

const ministerDoesService = curry((service, minister) =>
  !!~minister.workers.map(({service}) => service).indexOf(service)
)

// Exported
export let getMinisterInstance = ({router, id, latency, endpoint}) => {
  let minister = {
    workers: []
  }

  return Object.defineProperties(minister, {
    type: {value: 'Minister'},
    id: {value: id, enumerable: true},
    name: {value: id.substring(0, 11), enumerable: true},
    latency: {value: latency, enumerable: true},
    endpoint: {value: endpoint, enumerable: true},
    toJS: {value: () => ({id, latency, endpoint, workers: minister.workers})},
    send: {value: (frames) => router.send([id, ...frames])},
    hasSlotsForService: {value: (service) =>
      minister.workers
        .filter(workerDoesService(service))
        .filter(({concurrency, pendingRequests}) =>
          concurrency < 0 || concurrency > pendingRequests)
        .length > 0
    }
  })
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
  .catch(() => [])

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
