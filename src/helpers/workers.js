import { curry } from 'lodash'
import compose from 'lodash/fp/compose'
import isEqualFp from 'lodash/fp/isEqual'
import getFp from 'lodash/fp/get'
import pickFp from 'lodash/fp/pick'

import MINISTERS from '../MINISTERS'

// Internals
const workerHasId = (workerId) => compose(isEqualFp(workerId), getFp('id'))

const workerDoesService = (serviceName) => compose(isEqualFp(serviceName), getFp('service'))

const workerCanWork = compose(freeSlots => freeSlots > 0, getFp('freeSlots'))

// Exported
const getWorkerInstance = (router, workerId, service, freeSlots) => {
  let worker = {
    type: 'Worker',
    id: workerId,
    service,
    freeSlots,
    liveness: MINISTERS.HEARTBEAT_LIVENESS
  }

  Object.defineProperty(worker, 'send', {value: (...frames) => router.send([workerId, ...frames])})

  return worker
}

const findWorkerById = curry((workers, workerId) => workers.find(workerHasId(workerId)))

const workerState = pickFp(['id', 'service', 'freeSlots'])

const findAvailableWorkerForService = curry((workers, service) =>
  workers.filter(workerDoesService(service)).filter(workerCanWork).sort((w1, w2) =>
    w1.freeSlots > w2.freeSlots
      ? -1
      : w1.freeSlots < w2.freeSlots
        ? 1
        : 0
  )[0])

export {
  getWorkerInstance,
  findWorkerById,
  workerState,
  findAvailableWorkerForService
}
