import stream from 'readable-stream'
import uuid from 'uuid'
import { curry, noop, isString } from 'lodash'
import compose from 'lodash/fp/compose'
import eqFp from 'lodash/fp/eq'
import isEqualFp from 'lodash/fp/isEqual'
import negateFp from 'lodash/fp/negate'
import getFp from 'lodash/fp/get'

import {
  defer
} from './utils'

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
  workerErrorResponseMessage,

  ministerRequestLostStakeholder
} from './messages'

// Internals
const requestIsNotAssigned = negateFp(getFp('assignee'))
const requestIsAssigned = negateFp(requestIsNotAssigned)
const requestIsNotDispatched = negateFp(getFp('isDispatched'))
const requestIsDispatched = negateFp(requestIsNotDispatched)
const requestIsIdempotent = getFp('isIdempotent')
const requestHasUUID = (uuid) => compose(isEqualFp(uuid), getFp('uuid'))

// Exported
export let getMinisterRequestInstance = ({stakeholder, uuid, service, frames, options, onFinished, debug}) => {
  let request = {}
  let {idempotent, reconnectStream} = options

  let _isClean = true
  let _isAccomplished = false
  let _isFailed = false
  let _isFinished = false
  let _hasLostStakeholder = false
  let _hasLostWorker = false

  return Object.defineProperties(request, {
    uuid: {value: uuid},
    shortId: {value: uuid.substring(0, 8)},
    service: {value: service},
    stakeholder: {get: () => _hasLostStakeholder ? null : stakeholder},
    frames: {value: frames},
    isAccomplished: {value: () => _isAccomplished},
    isFailed: {value: () => _isFailed},
    isFinished: {value: () => _isFinished},
    isClean: {get: () => _isClean},
    isIdempotent: {value: idempotent},
    canReconnectStream: {value: reconnectStream},
    lostStakeholder: {value: () => {
      if (_hasLostStakeholder) return
      _hasLostStakeholder = true
      if (request.assignee) request.assignee.send(ministerRequestLostStakeholder(uuid))
    }},
    lostWorker: {value: () => {
      if (_hasLostWorker) return
      _hasLostWorker = true
      request.sendErrorResponse(JSON.stringify(RESPONSE_LOST_WORKER))
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
export let getClientRequestInstance = ({service, options, bodyBuffer, onFinished, debug}) => {
  let request = {}
  let readableInterface = new stream.Readable({read: noop})
  // Let's prevent the `Uncaught Error` behaviour which could happen in case
  // the user does not register an handler for the `error` event
  readableInterface.on('error', noop)
  let {timeout, idempotent, reconnectStream} = options

  let _isClean = true
  let _isAccomplished = false
  let _isTimedout = false
  let _isFailed = false
  let _isFinished = false
  let _receivedBytes = 0
  let _uuid = uuid.v4()
  let _options = JSON.stringify(options)
  let _deferred

  let _timeoutHandle
  let _setupTimeout = () => _timeoutHandle = setTimeout(() => {
    _isTimedout = true
    request.giveErrorResponse(RESPONSE_TIMEOUT)
  }, timeout)
  if (timeout) _setupTimeout()

  Object.defineProperty(readableInterface, 'promise', {
    value: () => {
      _deferred = _deferred || defer()
      return _deferred.promise
    }
  })

  return Object.defineProperties(request, {
    uuid: {get: () => _uuid},
    shortId: {get: () => _uuid.substring(0, 8)},
    isClean: {get: () => _isClean},
    isAccomplished: {get: () => _isAccomplished},
    isTimedout: {get: () => _isTimedout},
    isFailed: {get: () => _isFailed},
    isFinished: {get: () => _isFinished},
    receivedBytes: {get: () => _receivedBytes},
    isIdempotent: {value: idempotent},
    canReconnectStream: {value: reconnectStream},
    readableInterface: {value: readableInterface},
    frames: {get: () => clientRequestMessage(_uuid, service, _options, bodyBuffer)},
    givePartialResponse: {value: (buffer) => {
      if (_isFinished) return
      clearTimeout(_timeoutHandle)
      _timeoutHandle = null
      _isClean = false
      _receivedBytes += buffer.length
      readableInterface.push(buffer)
      options.partialCallback(buffer)
    }},
    giveFinalResponse: {value: (buffer) => {
      if (_isFinished) return
      clearTimeout(_timeoutHandle)
      _timeoutHandle = null
      _isClean = false
      _isAccomplished = true
      _isFinished = true
      _receivedBytes += buffer.length
      onFinished()
      debug(`Request ${request.shortId} had final response`)
      readableInterface.push(buffer)
      readableInterface.push(null)
      options.finalCallback(null, buffer)
      if (_deferred) _deferred.resolve(buffer)
    }},
    giveErrorResponse: {value: (error) => {
      if (_isFinished) return
      clearTimeout(_timeoutHandle)
      _timeoutHandle = null
      _isClean = false
      _isFailed = true
      _isFinished = true
      onFinished()
      debug(`Request ${request.shortId} had error response: ${error}`)
      let errMsg = isString(error)
        ? error
        : error.message || `request failed`
      let e = isString(error)
        ? new Error(errMsg)
        : Object.assign(
          new Error(errMsg),
          error
        )
      readableInterface.emit('error', e)
      readableInterface.push(null)
      options.finalCallback(e)
      if (_deferred) _deferred.reject(e)
    }},
    reschedule: {value: () => {
      if (_isFinished) return
      delete request.isDispatched
      let oldId = request.shortId
      _uuid = uuid.v4()
      debug(`Rescheduling request ${oldId} -> ${request.shortId}`)
      if (timeout && !_timeoutHandle) _setupTimeout()
    }},
    lostWorker: {value: () => {
      debug(`Request ${request.shortId} lost connection with worker`)
      request.giveErrorResponse(RESPONSE_LOST_WORKER)
    }}
  })
}
export let getWorkerRequestInstance = ({connection, uuid, body, options, onFinished, debug}) => {
  let _ended = false
  let _ending = false
  let _endingWithNull = false
  let _isError = false
  let _isActive = true

  let request = Object.defineProperties({}, {
    body: {value: {...body}, enumerable: true},
    options: {value: {...options}, enumerable: true},
    isActive: {get () { return _isActive }}
  })

  let response = new stream.Writable({
    objectMode: true,
    write (chunk, encoding, cb) {
      cb()

      if (_ended) return

      if (_isActive) {
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
        body = _endingWithNull ? new Buffer(0) : (body || new Buffer(0))

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
    shortId: {value: uuid.substring(0, 8)},
    request: {value: request},
    response: {value: response},
    lostStakeholder: {value: () => {
      _isActive = false
      response.on('error', noop)
      process.nextTick(() => response.end())
    }}
  })
}

export let findRequestsByStakeholder = curry((requests, stakeholder) =>
  requests.filter(compose(eqFp(stakeholder.id), getFp('id'), getFp('stakeholder'))))
export let findRequestsByAssignee = curry((requests, assignee) =>
  requests.filter(compose(eqFp(assignee.id), getFp('id'), getFp('assignee'))))
export let findRequestByUUID = curry((requests, uuid) => requests.find(requestHasUUID(uuid)))
export let findUnassignedRequests = (requests) => () => requests.filter(requestIsNotAssigned)
export let findAssignedRequests = (requests) => () => requests.filter(requestIsAssigned)
export let findNotDispatchedRequests = (requests) => () => requests.filter(requestIsNotDispatched)
export let findDispatchedRequests = (requests) => () => requests.filter(requestIsDispatched)
export let findIdempotentRequests = (requests) => () => requests.filter(requestIsIdempotent)
