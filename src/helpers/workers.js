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
    type: 'Worker',
    id,
    service,
    concurrency,
    assignedRequests: 0,
    latency
  }

  Object.defineProperty(worker, 'send', {value: (...frames) => router.send([id, ...frames])})
  Object.defineProperty(worker, 'freeSlots', {get: () => max([0, worker.concurrency - worker.assignedRequests])})

  return worker
}

const findWorkerById = curry((workers, workerId) => workers.find(workerHasId(workerId)))

const workerToState = pickFp(['id', 'service', 'freeSlots'])

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
