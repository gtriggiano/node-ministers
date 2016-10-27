import EventEmitter from 'eventemitter3'
import { curry, isInteger } from 'lodash'
import compose from 'lodash/fp/compose'
import eqFp from 'lodash/fp/eq'
import isEqualFp from 'lodash/fp/isEqual'
import negateFp from 'lodash/fp/negate'
import getFp from 'lodash/fp/get'

import {
  RESPONSE_TIMEOUT,
  RESPONSE_LOST_WORKER
} from '../CONSTANTS'

import {
  workerPartialResponseMessage,
  workerFinalResponseMessage,
  workerErrorResponseMessage
} from './messages'

// Internals
const requestIsNotAssigned = negateFp(getFp('assignee'))
const requestIsAssigned = negateFp(requestIsNotAssigned)
const getStakeholderType = compose(getFp('type'), getFp('stakeholder'))
const getAssigneeType = compose(getFp('type'), getFp('assignee'))
const stakeholderIsClient = compose(isEqualFp('Client'), getStakeholderType)
const stakeholderIsMinister = compose(isEqualFp('Minister'), getStakeholderType)
const assigneeIsWorker = compose(isEqualFp('Worker'), getAssigneeType)
const assigneeIsMinister = compose(isEqualFp('Minister'), getAssigneeType)
const requestHasUUID = (uuid) => compose(isEqualFp(uuid), getFp('uuid'))

// Exported
export let getMinisterRequestInstance = ({stakeholder, uuid, service, timeout, frames, onFinished}) => {
  let _accomplished = false
  let _failed = false
  let _timedout = false
  let _finished = false
  let _lostStakeholder = false
  let _lostWorker = false
  let _timeout

  let request = {}

  if (isInteger(timeout) && timeout > 0) {
    _timeout = setTimeout(() => {
      request.sendErrorResponse(RESPONSE_TIMEOUT)
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
    lostStakeholder: {value: () => {
      _lostStakeholder = true
      clearTimeout(_timeout)
    }},
    lostWorker: {value: () => {
      if (_lostWorker) return
      _lostWorker = true
      request.sendErrorResponse(RESPONSE_LOST_WORKER)
    }},
    sendPartialResponse: {value: (body) => {
      if (_finished) return
      clearTimeout(_timeout)
      _lostStakeholder || stakeholder.send(workerPartialResponseMessage(uuid, body))
    }},
    sendFinalResponse: {value: (body) => {
      if (_finished) return
      _accomplished = true
      _finished = true
      clearTimeout(_timeout)
      onFinished()
      _lostStakeholder || stakeholder.send(workerFinalResponseMessage(uuid, body))
    }},
    sendErrorResponse: {value: (body) => {
      if (_finished) return
      _failed = true
      _finished = true
      clearTimeout(_timeout)
      onFinished()
      _lostStakeholder || stakeholder.send(workerErrorResponseMessage(uuid, body))
    }}
  })
}
export let getClientRequestInstance = ({service, body, options}) => {
  let request = new EventEmitter()

  return request
}
export let findRequestsByClientStakeholder = curry((requests, client) =>
  requests.filter(stakeholderIsClient).filter(compose(eqFp(client.id), getFp('id'), getFp('stakeholder'))))
export let findRequestsByMinisterStakeholder = curry((requests, minister) =>
  requests.filter(stakeholderIsMinister).filter(compose(eqFp(minister.address), getFp('address'), getFp('stakeholder'))))
export let findRequestsByWorkerAssignee = curry((requests, worker) =>
  requests.filter(assigneeIsWorker).filter(compose(eqFp(worker.id), getFp('id'), getFp('assignee'))))
export let findRequestsByMinisterAssignee = curry((requests, minister) =>
  requests.filter(assigneeIsMinister).filter(compose(eqFp(minister.address), getFp('address'), getFp('assignee'))))
export let findRequestByUUID = curry((requests, uuid) => requests.find(requestHasUUID(uuid)))
export let findUnassignedRequests = (requests) => () => requests.filter(requestIsNotAssigned)
export let findAssignedRequests = (requests) => () => requests.filter(requestIsAssigned)
