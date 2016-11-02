import stream from 'readable-stream'
import uuid from 'uuid'
import { curry, noop, isString } from 'lodash'
import compose from 'lodash/fp/compose'
import eqFp from 'lodash/fp/eq'
import isEqualFp from 'lodash/fp/isEqual'
import negateFp from 'lodash/fp/negate'
import getFp from 'lodash/fp/get'

// Constants
import {
  RESPONSE_TIMEOUT,
  RESPONSE_LOST_WORKER
} from '../CONSTANTS'

// Messages
import {
  clientRequestMessage,

  workerPartialResponseMessage,
  workerFinalResponseMessage,
  workerErrorResponseMessage
} from './messages'

// Internals
const requestIsNotAssigned = negateFp(getFp('assignee'))
const requestIsAssigned = negateFp(requestIsNotAssigned)
const requestIsNotDispatched = negateFp(getFp('isDispatched'))
const requestIsDispatched = negateFp(requestIsNotDispatched)
const requestIsIdempotent = getFp('isIdempotent')
const getStakeholderType = compose(getFp('type'), getFp('stakeholder'))
const getAssigneeType = compose(getFp('type'), getFp('assignee'))
const stakeholderIsClient = compose(isEqualFp('Client'), getStakeholderType)
const stakeholderIsMinister = compose(isEqualFp('Minister'), getStakeholderType)
const assigneeIsWorker = compose(isEqualFp('Worker'), getAssigneeType)
const assigneeIsMinister = compose(isEqualFp('Minister'), getAssigneeType)
const requestHasUUID = (uuid) => compose(isEqualFp(uuid), getFp('uuid'))

// Exported
export let getMinisterRequestInstance = ({stakeholder, uuid, service, frames, options, onFinished}) => {
  let request = {}
  let {idempotent, reconnectStream} = JSON.parse(options)

  let _isClean = true
  let _isAccomplished = false
  let _isFailed = false
  let _isFinished = false
  let _hasLostStakeholder = false
  let _hasLostWorker = false

  return Object.defineProperties(request, {
    uuid: {value: uuid},
    service: {value: service},
    stakeholder: {get: () => _hasLostStakeholder ? null : stakeholder},
    frames: {value: frames},
    isAccomplished: {value: () => _isAccomplished},
    isFailed: {value: () => _isFailed},
    isFinished: {value: () => _isFinished},
    isClean: {get: () => _isClean},
    isIdempotent: {value: idempotent},
    canReconnectStream: {value: reconnectStream},
    lostStakeholder: {value: () => { _hasLostStakeholder = true }},
    lostWorker: {value: () => {
      if (_hasLostWorker) return
      _hasLostWorker = true
      request.sendErrorResponse(RESPONSE_LOST_WORKER)
    }},
    sendPartialResponse: {value: (body) => {
      if (_isFinished) return
      _isClean = false
      if (!_hasLostStakeholder) stakeholder.send(workerPartialResponseMessage(uuid, body))
    }},
    sendFinalResponse: {value: (body) => {
      if (_isFinished) return
      _isClean = false
      _isAccomplished = true
      _isFinished = true
      onFinished()
      if (!_hasLostStakeholder) stakeholder.send(workerFinalResponseMessage(uuid, body))
    }},
    sendErrorResponse: {value: (errorMessage) => {
      if (_isFinished) return
      _isClean = false
      _isFailed = true
      _isFinished = true
      onFinished()
      if (!_hasLostStakeholder) stakeholder.send(workerErrorResponseMessage(uuid, errorMessage))
    }}
  })
}
export let getClientRequestInstance = ({service, body, options, onFinished}) => {
  let request = {readableInterface: new stream.Readable({read: noop})}
  let {timeout, idempotent, reconnectStream} = options

  let _isClean = true
  let _isAccomplished = false
  let _isTimedout = false
  let _isFailed = false
  let _isFinished = false
  let _receivedBytes = 0
  let _uuid = new Buffer(uuid.v4())
  let _options = JSON.stringify(options)

  let _timeoutHandle
  let _setupTimeout = () => _timeoutHandle = setTimeout(() => {
    _isTimedout = true
    request.giveErrorResponse(RESPONSE_TIMEOUT)
  }, timeout)
  if (timeout) _setupTimeout()

  return Object.defineProperties(request, {
    uuid: {get: () => _uuid},
    isClean: {get: () => _isClean},
    isAccomplished: {get: () => _isAccomplished},
    isTimedout: {get: () => _isTimedout},
    isFailed: {get: () => _isFailed},
    isFinished: {get: () => _isFinished},
    receivedBytes: {get: () => _receivedBytes},
    isIdempotent: {value: idempotent},
    canReconnectStream: {value: reconnectStream},
    frames: {get: () => clientRequestMessage(_uuid, service, _options, body)},
    givePartialResponse: {value: (buffer) => {
      if (_isFinished) return
      clearTimeout(_timeoutHandle)
      _isClean = false
      _receivedBytes += buffer.length
      request.readableInterface.push(buffer)
    }},
    giveFinalResponse: {value: (buffer) => {
      if (_isFinished) return
      clearTimeout(_timeoutHandle)
      _isClean = false
      _isAccomplished = true
      _isFinished = true
      _receivedBytes += buffer.length
      onFinished()
      request.readableInterface.push(buffer)
      request.readableInterface.push(null)
    }},
    giveErrorResponse: {value: (buffer) => {
      if (_isFinished) return
      clearTimeout(_timeoutHandle)
      _isClean = false
      _isFailed = true
      _isFinished = true
      onFinished()
      request.readableInterface.emit('error', new Error(buffer))
      request.readableInterface.push(null)
    }},
    reschedule: {value: () => {
      if (_isFinished) return
      delete request.assignee
      _uuid = new Buffer(uuid.v4())
      if (timeout && !_timeoutHandle) _setupTimeout()
    }},
    signalLostWorker: {value: () => {
      request.giveErrorResponse(RESPONSE_LOST_WORKER)
    }}
  })
}
export let getWorkerRequestInstance = ({connection, uuid, body, options, onFinished}) => {
  let _ended = false
  let _ending = false
  let _endingWithNull = false
  let _isError = false
  let _hasStakeholder = true

  let request = Object.defineProperties({}, {
    body: {value: {...body}, enumerable: true},
    options: {value: {...options}, enumerable: true},
    isActive: {get () { return _hasStakeholder }}
  })

  let response = new stream.Writable({
    objectMode: true,
    write (chunk, encoding, cb) {
      cb()
      if (_ended) return

      if (_hasStakeholder) {
        let body
        try {
          body = encoding === 'buffer'
            ? chunk
            : _isError
              ? new Buffer(JSON.stringify(chunk))
              : isString(chunk)
                ? new Buffer(chunk)
                : new Buffer(JSON.stringify(chunk))
        } catch (e) {}
        body = _endingWithNull ? null : (body || null)

        let msg = _isError
          ? workerErrorResponseMessage(uuid, body)
          : _ending
            ? workerFinalResponseMessage(uuid, body)
            : workerPartialResponseMessage(uuid, body)
        connection.send(msg)
      }

      if (_ending) {
        _ended = true
        onFinished()
      }
    }
  })
  let responseEnd = response.end
  response.end = (chunk, encoding, cb) => {
    _ending = true
    if (chunk === undefined || chunk === null) {
      chunk = ''
      _endingWithNull = true
    }
    return responseEnd.apply(response, [chunk, encoding, cb])
  }
  response.send = (chunk) => response.end(chunk)
  response.error = (chunk) => {
    _isError = true
    response.end(chunk)
  }

  return Object.defineProperties({}, {
    uuid: {value: uuid},
    request: {value: request},
    response: {value: response},
    lostStakeholder () {
      _hasStakeholder = false
    }
  })
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
export let findNotDispatchedRequests = (requests) => () => requests.filter(requestIsNotDispatched)
export let findDispatchedRequests = (requests) => () => requests.filter(requestIsDispatched)
export let findIdempotentRequests = (requests) => () => requests.filter(requestIsIdempotent)
