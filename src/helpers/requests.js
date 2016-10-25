import { curry, isInteger } from 'lodash'
import compose from 'lodash/fp/compose'
import eqFp from 'lodash/fp/eq'
import isEqualFp from 'lodash/fp/isEqual'
import negateFp from 'lodash/fp/negate'
import getFp from 'lodash/fp/get'

import CONSTANTS from '../CONSTANTS'

// Internals
const requestIsNotAssigned = compose(negateFp, getFp('assignee'))
// const requestIsAssigned = compose(negateFp, requestIsNotAssigned)
const getStakeholderType = compose(getFp('type'), getFp('stakeholder'))
const getAssigneeType = compose(getFp('type'), getFp('assignee'))
const stakeholderIsClient = compose(isEqualFp('Client'), getStakeholderType)
const stakeholderIsMinister = compose(isEqualFp('Minister'), getStakeholderType)
const assigneeIsWorker = compose(isEqualFp('Worker'), getAssigneeType)
const assigneeIsMinister = compose(isEqualFp('Minister'), getAssigneeType)
const requestHasUUID = (uuid) => compose(isEqualFp(uuid), getFp('uuid'))

// Exported
const getMinisterRequestInstance = ({stakeholder, uuid, service, timeout, frames, onFinished}) => {
  let _accomplished = false
  let _failed = false
  let _timedout = false
  let _finished = false
  let _timeout

  let request = {}

  if (isInteger(timeout) && timeout > 0) {
    _timeout = setTimeout(() => {
      request.sendErrorResponse(CONSTANTS.RESPONSE_TIMEOUT)
      _timedout = true
    }, timeout)
  }

  return Object.defineProperties(request, {
    uuid: {value: uuid},
    service: {value: service},
    stakeholder: {value: stakeholder},
    frames: {value: frames},
    isAccomplished: {value: () => _accomplished},
    isFailed: {value: () => _failed},
    isTimedout: {value: () => _timedout},
    isFinished: {value: () => _finished},
    sendPartialResponse: {value: (body) => {
      if (_finished) return
      clearTimeout(_timeout)
      stakeholder.send(['MW', CONSTANTS.W_PARTIAL_RESPONSE, uuid, body])
    }},
    sendFinalResponse: {value: (body) => {
      if (_finished) return
      _accomplished = true
      _finished = true
      clearTimeout(_timeout)
      onFinished()
      stakeholder.send(['MW', CONSTANTS.W_FINAL_RESPONSE, uuid, body])
    }},
    sendErrorResponse: {value: (body) => {
      if (_finished) return
      _failed = true
      _finished = true
      clearTimeout(_timeout)
      onFinished()
      stakeholder.send(['MW', CONSTANTS.W_ERROR_RESPONSE, uuid, body])
    }}
  })
}
const findRequestsByClientStakeholder = curry((requests, client) =>
  requests.filter(stakeholderIsClient).filter(compose(eqFp(client.id), getFp('id'), getFp('stakeholder'))))
const findRequestsByMinisterStakeholder = curry((requests, minister) =>
  requests.filter(stakeholderIsMinister).filter(compose(eqFp(minister.address), getFp('address'), getFp('stakeholder'))))
const findRequestsByWorkerAssignee = curry((requests, worker) =>
  requests.filter(assigneeIsWorker).filter(compose(eqFp(worker.id), getFp('id'), getFp('assignee'))))
const findRequestsByMinisterAssignee = curry((requests, minister) =>
  requests.filter(assigneeIsMinister).filter(compose(eqFp(minister.address), getFp('address'), getFp('assignee'))))
const findRequestByUUID = curry((requests, uuid) => requests.find(requestHasUUID(uuid)))
const findUnassignedRequests = (requests) => () => requests.filter(requestIsNotAssigned)

export {
  getMinisterRequestInstance,
  findUnassignedRequests,
  findRequestsByClientStakeholder,
  findRequestsByMinisterStakeholder,
  findRequestsByWorkerAssignee,
  findRequestsByMinisterAssignee,
  findRequestByUUID
}
