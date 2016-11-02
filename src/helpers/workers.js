import { curry, max } from 'lodash'
import compose from 'lodash/fp/compose'
import isEqualFp from 'lodash/fp/isEqual'
import getFp from 'lodash/fp/get'
import pickFp from 'lodash/fp/pick'

// Internals
const workerHasId = (workerId) => compose(isEqualFp(workerId), getFp('id'))

const workerDoesService = (service) => compose(isEqualFp(service), getFp('service'))

// Exported
const getWorkerInstance = ({router, id, service, concurrency, latency}) => {
  let worker = {
    concurrency,
    assignedRequests: 0
  }

  return Object.defineProperties(worker, {
    type: {value: 'Minister', enumerable: true},
    id: {value: id, enumerable: true},
    name: {value: id.substring(0, 11), enumerable: true},
    service: {value: service, enumerable: true},
    latency: {value: latency, enumerable: true},
    send: {value: (...frames) => router.send([id, ...frames])},
    freeSlots: {get: () => worker.concurrency >= 0 ? max([0, worker.concurrency - worker.assignedRequests]) : 1000000}
  })
}

const findWorkerById = curry((workers, workerId) => workers.find(workerHasId(workerId)))

const workerToState = pickFp(['id', 'service', 'concurrency', 'pendingRequests'])

const findWorkerForService = curry((workers, service) =>
  workers.filter(workerDoesService(service)).sort((w1, w2) => {
    let slots1 = w1.freeSlots
    let slots2 = w2.freeSlots

    return slots1 && !slots2
      ? -1
      : !slots1 && slots2
        ? 1
        : w1.latency < w2.latency
          ? -1
          : w1.latency > w2.latency
            ? 1
            : 0
  })[0]
)

export {
  getWorkerInstance,
  findWorkerById,
  workerToState,
  findWorkerForService
}
