import { curry, max } from 'lodash'
import compose from 'lodash/fp/compose'
import isEqualFp from 'lodash/fp/isEqual'
import getFp from 'lodash/fp/get'

// Internals
export let workerHasId = (workerId) => compose(isEqualFp(workerId), getFp('id'))

// Exported
export let getWorkerInstance = ({router, id, service, concurrency, latency}) => {
  let worker = {
    concurrency,
    pendingRequests: 0
  }

  return Object.defineProperties(worker, {
    type: {value: 'worker'},
    id: {value: id, enumerable: true},
    name: {value: id.substring(0, 11), enumerable: true},
    service: {value: service, enumerable: true},
    latency: {value: latency, enumerable: true},
    toJS: {value: () => ({
      id,
      service,
      latency,
      concurrency: worker.concurrency,
      pendingRequests: worker.pendingRequests
    })},
    send: {value: (frames) => router.send([id, ...frames])},
    freeSlots: {get: () => worker.concurrency >= 0 ? max([0, worker.concurrency - worker.pendingRequests]) : Infinity}
  })
}

export let findWorkerById = curry((workers, workerId) =>
  workers.find(workerHasId(workerId)))
export let workerDoesService = (service) =>
  compose(isEqualFp(service), getFp('service'))
export let findWorkerForService = curry((workers, service) =>
  workers.filter(workerDoesService(service)).sort((w1, w2) => {
    let slots1 = w1.freeSlots
    let slots2 = w2.freeSlots

    return slots1 > slots2
      ? -1
      : slots1 < slots2
        ? 1
        : w1.latency < w2.latency
          ? -1
          : w1.latency > w2.latency
            ? 1
            : 0
  })[0])
