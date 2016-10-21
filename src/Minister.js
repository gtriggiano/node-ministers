import { isString, isInteger, isArray, each, every, pull } from 'lodash'
import debug from 'debug'
import zmq from 'zmq'
import Rx from 'rxjs'
import uuid from 'uuid'
import Promise from 'bluebird'
import EventEmitter from 'eventemitter3'

/*
  Helpers
 */

// Utils
import {
  getOSNetworkExternalInterface,
  isValidIPv4,
  isValidEndpoint,
  prefixString
} from './helpers/utils'

// Messages
import {
  isClientMessage,
  isWorkerMessage,
  isMinisterMessage,
  isMinisterNotifierMessage,

  isClientHello,
  isClientHeartbeat,
  isClientDisconnect,
  isClientRequest,

  isWorkerReady,
  isWorkerHeartbeat,
  isWorkerDisconnect,
  isWorkerPartialResponse,
  isWorkerFinalResponse,
  isWorkerErrorResponse,

  isMinisterHello,
  isMinisterWorkersAvailability,
  isMinisterDisconnect,

  isMinisterNotifierNewMinisterConnected
} from './helpers/messages'

// Clients
import {
  getClientInstance,
  findClientById
} from './helpers/clients'

// Workers
import {
  getWorkerInstance,
  findWorkerById,
  workerState,
  findAvailableWorkerForService
} from './helpers/workers'

// Ministers
import {
  getMinisterInstance,
  findMinisterById,
  findAvailableMinisterForService,
  discoverOtherMinistersEndpoints,
  getMinisterLatency
} from './helpers/ministers'

// Requests
import {
  getMinisterRequestInstance,
  findUnassignedRequests,
  findRequestsByClientStakeholder,
  findRequestsByMinisterStakeholder,
  findRequestsByWorkerAssignee,
  findRequestsByMinisterAssignee,
  findRequestByUUID
} from './helpers/requests'

import MINISTERS from './MINISTERS'

const Minister = (settings) => {
  let log = debug('ministers:minister')
  let minister = new EventEmitter()

  let _settings = {...defaultSettings, ...settings}
  validateSettings(_settings)

  // Private API
  let _connected = false
  let _togglingConnection = false
  let _ministersWorkersUpdateInterval
  let _requestAssigningInterval

  let _clients = []
  let _clientById = findClientById(_clients)

  let _workers = []
  let _workerById = findWorkerById(_workers)
  let _workerForService = findAvailableWorkerForService(_workers)

  let _ministers = []
  let _ministerById = findMinisterById(_ministers)
  let _ministerForService = findAvailableMinisterForService(_ministers)

  let _requests = []
  let _requestByUUID = findRequestByUUID(_requests)
  let _requestsFromClient = findRequestsByClientStakeholder(_requests)
  let _requestsFromMinister = findRequestsByMinisterStakeholder(_requests)
  let _requestsAssignedToWorker = findRequestsByWorkerAssignee(_requests)
  let _requestsAssignedToMinister = findRequestsByMinisterAssignee(_requests)
  let _requestsUnassigned = findUnassignedRequests(_requests)

  // Client messages
  let _onClientHello = (msg) => {
    let clientId = msg[0]
    let client = _clientById(clientId)
    if (!client) {
      client = getClientInstance(_bindingRouter, clientId)
      _monitor(client)
      _clients.push(client)
    }
  }
  let _onClientHeartbeat = (msg) => {
    let clientId = msg[0]
    let client = _clientById(clientId)
    if (client) {
      client.liveness = MINISTERS.HEARTBEAT_LIVENESS
    }
  }
  let _onClientDisconnect = (msg) => {
    let clientId = msg[0]
    let client = _clientById(clientId)
    if (client) _onClientLost(client)
  }
  let _onClientRequest = (msg) => {
    let stakeholderId = msg[0]
    let stakeholder = _clientById(stakeholderId) || _ministerById(stakeholderId)
    let uuid = msg[3].toString()
    let service = msg[4].toString()
    let options = JSON.parse(msg[5])
    let frames = msg.slice(1)

    let request = getMinisterRequestInstance({
      stakeholder,
      uuid,
      service,
      options,
      frames
    })

    _requests.push(request)
    _assignRequests()
  }
  // Worker messages
  let _onWorkerReady = (msg) => {
    let workerId = msg[0]
    let serviceName = msg[3].toString()
    let freeSlots = JSON.parse(msg[4])
    let worker = _workerById(workerId)
    if (!worker) {
      worker = getWorkerInstance(_bindingRouter, workerId, serviceName, freeSlots)
      _monitor(worker)
      _workers.push(worker)
    }
  }
  let _onWorkerHeartbeat = (msg) => {
    let workerId = msg[0]
    let worker = _workerById(workerId)
    if (worker) worker.liveness = MINISTERS.HEARTBEAT_LIVENESS
  }
  let _onWorkerDisconnect = (msg) => {
    let workerId = msg[0]
    let worker = _workerById(workerId)
    if (worker) _onWorkerLost(worker)
  }
  let _onWorkerPartialResponse = (msg) => {
    let senderId = msg[0]
    let sender = _workerById(senderId) || _ministerById(senderId)
    if (sender) {
      sender.liveness = MINISTERS.HEARTBEAT_LIVENESS
      let uuid = msg[3].toString()
      let body = msg[4]
      let request = _requestByUUID(uuid)
      if (request) request.sendPartialResponse(body)
    }
  }
  let _onWorkerFinalResponse = (msg) => {
    let senderId = msg[0]
    let sender = _workerById(senderId) || _ministerById(senderId)
    if (sender) {
      sender.liveness = MINISTERS.HEARTBEAT_LIVENESS
      let uuid = msg[3].toString()
      let body = msg[4]
      let request = _requestByUUID(uuid)
      if (request) request.sendFinalResponse(body)
    }
  }
  let _onWorkerErrorResponse = (msg) => {
    let senderId = msg[0]
    let sender = _workerById(senderId) || _ministerById(senderId)
    if (sender) {
      sender.liveness = MINISTERS.HEARTBEAT_LIVENESS
      let uuid = msg[3].toString()
      let body = msg[4]
      let request = _requestByUUID(uuid)
      if (request) request.sendErrorResponse(body)
    }
  }
  // Minister messages
  let _onMinisterHello = (msg) => {
    let ministerId = msg[0]
    let {binding, latency, endpoint} = JSON.parse(msg[3])

    let minister = _ministerById(ministerId)
    if (!minister) {
      if (binding) log(`Communicating with minister bound at ${endpoint}.`)
      if (!binding) log(`Communicating with connected minister.`)

      log(`Minister ID: ${ministerId}`)
      log(`Minister latency: ${latency} milliseconds\n`)

      minister = getMinisterInstance(binding ? _connectingRouter : _bindingRouter, ministerId, latency)
      _monitor(minister)
      _ministers.push(minister)
      minister.send([
        'MM',
        MINISTERS.M_HELLO,
        JSON.stringify({
          latency,
          binding: !binding
        })
      ])
    }
  }
  let _onMinisterWorkersAvailability = (msg) => {
    let ministerId = msg[0]
    let workers = JSON.parse(msg[3])
    let minister = _ministerById(ministerId)
    if (minister) {
      minister.workers = workers
      minister.liveness = MINISTERS.HEARTBEAT_LIVENESS
    }
  }
  let _onMinisterDisconnect = (msg) => {
    let ministerId = msg[0]
    let minister = _ministerById(ministerId)
    if (minister) _onMinisterLost(minister)
  }
  // MinisterNotifier messages
  let _onNewMinisterConnected = (msg) => {
    let { identity, latency } = JSON.parse(msg[3])
    log(`Received notification of a new connected minister (${identity}). Sending HELLO message.\n`)
    _bindingRouter.send([
      identity,
      'MM',
      MINISTERS.M_HELLO,
      JSON.stringify({
        latency,
        binding: true,
        endpoint: _bindingRouter.endpoint
      })
    ])
  }

  // Routers lifecycle management
  let _bindingRouter
  let _setupBindingRouter = () => new Promise((resolve, reject) => {
    _bindingRouter = zmq.socket('router')
    _bindingRouter.linger = 1
    _bindingRouter.identity = `MM-${uuid.v4()}`
    log('Binding Router created')

    let bindIp = _settings.ip || '0.0.0.0'
    let osExternalIp = getOSNetworkExternalInterface()
    let bindEndpoint = `tcp://${bindIp}:${_settings.port}`
    _bindingRouter.bind(bindEndpoint, (err) => {
      if (err) return reject(err)

      let routerEndpoint = _settings.ip
        ? bindEndpoint
        : osExternalIp && `tcp://${osExternalIp}:${_settings.port}`

      if (routerEndpoint) {
        log(`Binding Router bound to ${routerEndpoint}`)
        _bindingRouter.endpoint = routerEndpoint
        return resolve()
      }
      _bindingRouter.unbindSync(bindEndpoint)
      _bindingRouter.close()
      _bindingRouter = null
      reject(new Error('Could not determine the router endpoint'))
    })
  })
  let _tearDownBindingRouter = () => {
    _unsubscribeFromBindingRouter()
    _unsubscribeFromBindingRouter = null
    _bindingRouter.close()
    _bindingRouter = null
    log('Binding Router destroyed')
  }
  let _connectingRouter
  let _connectingRouterConnections
  let _setupConnectingRouter = () => {
    _connectingRouter = zmq.socket('router')
    _connectingRouter.linger = 1
    _connectingRouter.identity = _bindingRouter.identity
    _connectingRouter.monitor(10, 0)
    log('Connecting Router created')
  }
  let _tearDownConnectingRouter = () => {
    _unsubscribeFromConnectingRouter()
    _unsubscribeFromConnectingRouter = null
    _connectingRouterConnections = null
    _connectingRouter.unmonitor()
    _connectingRouter.close()
    _connectingRouter = null
    log('Connecting Router destroyed')
  }
  let _unsubscribeFromBindingRouter
  let _unsubscribeFromConnectingRouter
  let _observeRouter = (router, isBindingRouter) => {
    // Collect a map of subscriptions
    let subscriptions = {}

    let subject = new Rx.Subject()
    let messages = Rx.Observable.fromEvent(router, 'message', (id, side, msgType, ...frames) => [
      id.toString(),
      side && side.toString(),
      msgType && msgType.toString(),
      ...frames
    ]).multicast(subject).refCount()
    let clientMessages = messages.filter(isClientMessage)
    let workerMessages = messages.filter(isWorkerMessage)
    let ministerMessages = messages.filter(isMinisterMessage)
    let ministerNotifierMessages = messages.filter(isMinisterNotifierMessage)

    subscriptions.clientHello = clientMessages
      .filter(isClientHello).subscribe(_onClientHello)
    subscriptions.clientHeartbeat = clientMessages
      .filter(isClientHeartbeat).subscribe(_onClientHeartbeat)
    subscriptions.clientDisconnect = clientMessages
      .filter(isClientDisconnect).subscribe(_onClientDisconnect)
    subscriptions.clientRequests = clientMessages
      .filter(isClientRequest).subscribe(_onClientRequest)

    subscriptions.workerReady = workerMessages
      .filter(isWorkerReady).subscribe(_onWorkerReady)
    subscriptions.workerHeartbeat = workerMessages
      .filter(isWorkerHeartbeat).subscribe(_onWorkerHeartbeat)
    subscriptions.workerDisconnect = workerMessages
      .filter(isWorkerDisconnect).subscribe(_onWorkerDisconnect)
    subscriptions.workerPartialResponse = workerMessages
      .filter(isWorkerPartialResponse).subscribe(_onWorkerPartialResponse)
    subscriptions.workerFinalResponse = workerMessages
      .filter(isWorkerFinalResponse).subscribe(_onWorkerFinalResponse)
    subscriptions.workerErrorResponse = workerMessages
      .filter(isWorkerErrorResponse).subscribe(_onWorkerErrorResponse)

    subscriptions.ministerHello = ministerMessages
      .filter(isMinisterHello).subscribe(_onMinisterHello)
    subscriptions.ministerWorkersAvailability = ministerMessages
      .filter(isMinisterWorkersAvailability).subscribe(_onMinisterWorkersAvailability)
    subscriptions.ministerDisconnect = ministerMessages.filter(isMinisterDisconnect).subscribe(_onMinisterDisconnect)

    subscriptions.ministerNotifierNewMinisterConnected = ministerNotifierMessages
      .filter(isMinisterNotifierNewMinisterConnected).subscribe(_onNewMinisterConnected)

    if (!isBindingRouter) {
      let monitorSubject = new Rx.Subject()
      _connectingRouterConnections = Rx.Observable.fromEvent(router, 'connect', (_, ep) => ep).multicast(monitorSubject)
      subscriptions.connectinRouterConnections = _connectingRouterConnections.connect()
    }

    let unsubscribe = () => each(subscriptions, subscription => subscription.unsubscribe())
    return unsubscribe
  }

  // Peers management
  let _monitor = (peer) => {
    _unmonitor(peer)
    peer.heartbeatCheck = setInterval(() => {
      peer.liveness--
      if (!peer.liveness) {
        switch (peer.type) {
          case 'Client': return _onClientLost(peer)
          case 'Worker': return _onWorkerLost(peer)
          case 'Minister': return _onMinisterLost(peer)
        }
      }
    }, MINISTERS.HEARTBEAT_CHECK_INTERVAL)
  }
  let _unmonitor = (peer) => {
    if (peer.heartbeatCheck) {
      clearInterval(peer.heartbeatCheck)
      delete peer.heartbeatCheck
    }
  }
  let _onClientLost = (client) => {
    _unmonitor(client)
  }
  let _onWorkerLost = (worker) => {
    _unmonitor(worker)
  }
  let _onMinisterLost = (minister) => {
    log(`Lost connection with minister ${minister.id}\n`)
    _unmonitor(minister)
    pull(_ministers, [minister])
  }
  let _presentToMinisters = () => {
    let getMinistersEndpoints
    if (isString(_settings.ministers)) {
      getMinistersEndpoints = discoverOtherMinistersEndpoints({
        host: _settings.ministers,
        port: settings.port,
        ownEndpoint: _bindingRouter.endpoint
      })
    } else {
      getMinistersEndpoints = Promise.resolve(_settings.ministers)
    }

    getMinistersEndpoints
      .then(endpoints => {
        endpoints.forEach(endpoint => {
          log(`Found potential minister at ${endpoint}`)
          getMinisterLatency(endpoint)
            .then(latency => {
              log(`Minister at ${endpoint} is reachable with a latency of ${latency}ms. Connecting...\n`)
              // Establish a connection to the peer minister
              _connectingRouter.connect(endpoint)
              //  Establish a notifier connection
              let notifier = zmq.socket('dealer')
              notifier.connect(endpoint)

              let connectionsSubscription = _connectingRouterConnections.subscribe(ep => {
                if (ep === endpoint) {
                  connectionsSubscription.unsubscribe()
                  log(`Connected to minister at ${ep}.`)
                  log(`Presenting myself (${_connectingRouter.identity}) through notifier.\n`)
                  // Notify the minister about myself
                  notifier.send([
                    'MMN',
                    MINISTERS.MN_NEW_MINISTER_CONNECTED,
                    JSON.stringify({
                      identity: _connectingRouter.identity,
                      latency
                    })
                  ])
                  notifier.close()
                }
              })
            })
            .catch(() => {
              log(`Could not reach peer at ${endpoint}\n`)
            })
        })
      })
  }
  let _broadcastWorkersState = () => {
    log(`Broadcasting state of ${_workers.length} workers to ${_ministers.length} ministers\n`)
    _ministers.forEach(minister => minister.send([
      'MM',
      MINISTERS.M_WORKERS_AVAILABILITY,
      JSON.stringify(_workers.map(workerState))
    ]))
  }
  let _sendDisconnectionSignals = () => {
    log('Broadcasting disconnection signal')
    let disconnectioSignal = ['MM', MINISTERS.M_DISCONNECT]
    _ministers.forEach(minister => minister.send(disconnectioSignal))
    _workers.forEach(worker => worker.send(disconnectioSignal))
    _clients.forEach(client => client.send(disconnectioSignal))
  }

  // Requests dispatching
  let _assignRequests = () => {
    _requestsUnassigned()
      .forEach(request => {
        let assignee = _workerForService(request.service) || _ministerForService(request.service)
        if (assignee) {
          assignee.send(request.frames)
          request.assignee = assignee
        }
      })
  }

  // Public API
  function start () {
    if (_connected || _togglingConnection) return minister
    _togglingConnection = true
    log('Connecting...')
    // Create the bound router
    _setupBindingRouter()
      .then(() => {
        _setupConnectingRouter()
        _unsubscribeFromBindingRouter = _observeRouter(_bindingRouter, true)
        _unsubscribeFromConnectingRouter = _observeRouter(_connectingRouter, false)

        _presentToMinisters()

        // Periodically try to assign unassigned requests
        _requestAssigningInterval = setInterval(() => _assignRequests(), 500)
        // Periodically notify other ministers about own workers state
        _ministersWorkersUpdateInterval = setInterval(() => _broadcastWorkersState(), MINISTERS.M_WORKERS_UPDATE_INTERVAL)

        _togglingConnection = false
        _connected = true
        log('Connected')
        minister.emit('connection')
      })
      .catch(e => {
        log(e)
        log('Conncetion failed.')
        _togglingConnection = false
        minister.emit('error', e)
      })
  }
  function stop () {
    if (!_connected || _togglingConnection) return minister
    _togglingConnection = true
    log('Disconnecting...')

    // Stop request assigning routine
    clearInterval(_requestAssigningInterval)
    _requestAssigningInterval = null
    // Stop notifications to other ministers about own workers state
    clearInterval(_ministersWorkersUpdateInterval)
    _ministersWorkersUpdateInterval = null

    // Declare unavaliability
    _sendDisconnectionSignals()
    // Stop taking messages
    _unsubscribeFromBindingRouter()
    _unsubscribeFromConnectingRouter()

    setTimeout(function () {
      _tearDownBindingRouter()
      _tearDownConnectingRouter()
      _clients.length = 0
      _workers.length = 0
      _ministers.length = 0
      _togglingConnection = false
      _connected = false
      log('Disconnected')
      minister.emit('disconnection')
    }, 500)
  }

  Object.defineProperties(minister, {
    start: {value: start},
    stop: {value: stop}
  })
  return minister
}

const defaultSettings = {
  ip: null,
  port: 25255,
  ministers: []
}

const eMsg = prefixString('Minister(settings): ')
function validateSettings (settings) {
  let {ip, port, ministers} = settings
  if (
    ip &&
    !isValidIPv4(ip)
  ) throw new Error(eMsg('settings.ip should be a valid IPv4 address'))
  if (!isInteger(port) || port < 1) throw new Error(eMsg('settings.port should be a positive integer'))
  if (!ministers) ministers = []
  if (!isString(ministers) && ip) throw new Error('if your ministers\'addresses are resolvable via DNS you should not set settings.ip')
  if (
    isArray(ministers) &&
    !every(ministers, isValidEndpoint)
  ) throw new Error(eMsg('settings.ministers should be either a string, representing a hostname, or an array of 0 or more valid TCP endpoints, in the form of \'tcp://IP:port\''))
}

export default Minister
