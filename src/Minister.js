import net from 'net'
import debug from 'debug'
import zmq from 'zmq'
import z85 from 'z85'
import Rx from 'rxjs'
import uuid from 'uuid'
import Promise from 'bluebird'
import EventEmitter from 'eventemitter3'
import { isString, isInteger, isArray, isPlainObject, each, every, pull, max } from 'lodash'

/*
  Helpers
 */

// Utils
import {
  getOSNetworkExternalInterface,
  isValidEndpoint,
  isValidCurveKey,
  isValidCurveKeyPair,
  supportingCurveSecurity,
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

  isMinisterNotifierNewMinisterConnected,

  ministerHelloMessage,
  ministerConnectedMessage,
  ministerWorkersAvailabilityMessage,
  ministerDisconnectionMessage
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
  workerToState,
  findWorkerForService
} from './helpers/workers'

// Ministers
import {
  getMinisterInstance,
  findMinisterById,
  findMinisterForService,
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

// Constants
import {
  HEARTBEAT_LIVENESS,
  HEARTBEAT_INTERVAL
} from './CONSTANTS'

const Minister = (settings) => {
  let log = debug('ministers:minister')
  let minister = new EventEmitter()

  let _settings = {...defaultSettings, ...settings}
  validateSettings(_settings)

  // Private API
  let _connected = false
  let _togglingConnection = false
  let _ministersUpdateWorkersAvailabilityInterval
  let _requestAssigningInterval

  let _clients = []
  let _clientById = findClientById(_clients)

  let _workers = []
  let _workerById = findWorkerById(_workers)
  let _workerForService = findWorkerForService(_workers)

  let _ministers = []
  let _ministerById = findMinisterById(_ministers)
  let _ministerForService = findMinisterForService(_ministers)

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
      client = getClientInstance({
        router: _bindingRouter,
        id: clientId
      })
      _monitor(client)
      _clients.push(client)
    }
  }
  let _onClientHeartbeat = (msg) => {
    let client = _clientById(msg[0])
    if (client) _monitor(client)
  }
  let _onClientDisconnect = (msg) => {
    let client = _clientById(msg[0])
    if (client) _onClientLost(client)
  }
  let _onClientRequest = (msg) => {
    let stakeholder = _clientById(msg[0]) || _ministerById(msg[0])
    if (stakeholder) {
      let uuid = msg[3].toString()
      let service = msg[4].toString()
      let options = JSON.parse(msg[5])
      let frames = msg.slice(1)

      let request = getMinisterRequestInstance({
        stakeholder,
        uuid,
        service,
        timeout: options.timeout,
        frames,
        onFinished: () => pull(_requests, request)
      })
      _requests.push(request)
      _assignRequests()
    }
  }
  // Worker messages
  let _onWorkerReady = (msg) => {
    let workerId = msg[0]
    let {service, concurrency, latency} = JSON.parse(msg[2])
    let worker = _workerById(workerId)
    if (!worker) {
      worker = getWorkerInstance({
        router: _bindingRouter,
        id: workerId,
        service,
        concurrency,
        latency
      })
      _monitor(worker)
      _workers.push(worker)
      _broadcastWorkersAvailability()
    }
  }
  let _onWorkerHeartbeat = (msg) => {
    let worker = _workerById(msg[0])
    if (worker) {
      _monitor(worker)
      let workerDeclaredConcurrency = JSON.parse(msg[3])
      if (worker.concurrency !== workerDeclaredConcurrency) {
        worker.concurrency = workerDeclaredConcurrency
        _broadcastWorkersAvailability()
      }
    }
  }
  let _onWorkerDisconnect = (msg) => {
    let workerId = msg[0]
    let worker = _workerById(workerId)
    if (worker) _onWorkerLost(worker)
  }
  let _onWorkerPartialResponse = (msg) => {
    let sender = _workerById(msg[0]) || _ministerById(msg[0])
    if (sender) {
      _monitor(sender)
      let uuid = msg[3].toString()
      let request = _requestByUUID(uuid)
      if (
        request &&
        request.assignee === sender
      ) request.sendPartialResponse(msg[4])
    }
  }
  let _onWorkerFinalResponse = (msg) => {
    let sender = _workerById(msg[0]) || _ministerById(msg[0])
    if (sender) {
      sender.assignedRequests--
      _monitor(sender)
      let uuid = msg[3].toString()
      let request = _requestByUUID(uuid)
      if (
        request &&
        request.assignee === sender
      ) request.sendFinalResponse(msg[4])
    }
  }
  let _onWorkerErrorResponse = (msg) => {
    let sender = _workerById(msg[0]) || _ministerById(msg[0])
    if (sender) {
      sender.assignedRequests--
      _monitor(sender)
      let uuid = msg[3].toString()
      let request = _requestByUUID(uuid)
      if (
        request &&
        request.assignee === sender
      ) request.sendErrorResponse(msg[4])
    }
  }
  // Minister messages
  let _onMinisterHello = (msg) => {
    let ministerId = msg[0]
    let {binding, latency, endpoint} = JSON.parse(msg[3])

    let minister = _ministerById(ministerId)
    if (!minister) {
      if (binding) log(`Communicating with minister bound at ${endpoint}`)
      if (!binding) log(`Communicating with connected minister`)

      log(`Minister ID: ${ministerId}`)
      log(`Minister latency: ${latency} milliseconds\n`)

      minister = getMinisterInstance({
        router: binding ? _connectingRouter : _bindingRouter,
        id: ministerId,
        latency,
        endpoint
      })
      _monitor(minister)
      _ministers.push(minister)
      minister.send(ministerHelloMessage(JSON.stringify({
        binding: !binding,
        latency,
        endpoint: _bindingRouter.endpoint
      })))
    }
  }
  let _onMinisterWorkersAvailability = (msg) => {
    let minister = _ministerById(msg[0])
    if (minister) {
      _monitor(minister)
      minister.workers = JSON.parse(msg[3])
      _assignRequests()
    }
  }
  let _onMinisterDisconnect = (msg) => {
    let minister = _ministerById(msg[0])
    if (minister) _onMinisterLost(minister)
  }
  // MinisterNotifier messages
  let _onNewMinisterConnected = (msg) => {
    let { identity, latency } = JSON.parse(msg[3])
    log(`Received notification of a new connected minister (${identity}). Sending HELLO message\n`)
    _bindingRouter.send(ministerHelloMessage(JSON.stringify({
      binding: true,
      latency,
      endpoint: _bindingRouter.endpoint
    })))
  }

  // Routers lifecycle management
  let _bindingRouter
  let _zapRouter
  let _setupBindingRouter = () => {
    _bindingRouter = zmq.socket('router')
    _bindingRouter.linger = 1
    _bindingRouter.identity = `MM-${uuid.v4()}`

    if (_settings.security) {
      _bindingRouter.zap_domain = 'minister'
      _bindingRouter.curve_server = 1
      _bindingRouter.curve_secretkey = _settings.security.serverSecretKey
      _zapRouter = zmq.socket('router')
      _zapRouter.on('message', (...frames) => {
        let statusCode, statusText

        if (_settings.security.allowedClientKeys) {
          let clientPublicKey = z85.encode(frames[8])
          statusCode = ~_settings.security.allowedClientKeys.indexOf(clientPublicKey) ? '200' : '400'
        } else {
          statusCode = '200'
        }

        if (statusCode === '200') {
          statusText = 'OK'
          log('Authorizing minister connection')
        } else {
          statusText = 'Unauthorized public key'
          log('Not authorizing minister connection')
        }

        _zapRouter.send([
          frames[0],
          frames[1],
          frames[2],
          frames[3],
          new Buffer(statusCode),
          new Buffer(statusText),
          new Buffer(0),
          new Buffer(0)
        ])
      })
      _zapRouter.bindSync('inproc://zeromq.zap.01')
      log('ZAP with CURVE Mechanism enabled')
    }

    let bindIp = _settings.ip || '0.0.0.0'
    let osExternalIp = getOSNetworkExternalInterface()
    let bindEndpoint = `tcp://${bindIp}:${_settings.port}`
    let routerEndpoint = _settings.ip
      ? bindEndpoint
      : osExternalIp && `tcp://${osExternalIp}:${_settings.port}`

    if (routerEndpoint) {
      _bindingRouter.endpoint = routerEndpoint
      _bindingRouter.bindSync(bindEndpoint)
      log(`Binding Router bound to ${routerEndpoint}`)
      return true
    } else {
      _bindingRouter.close()
      _bindingRouter = null
      if (_zapRouter) {
        _zapRouter.close()
        _zapRouter = null
      }
      log(`Could not determine the OS external IP address, aborting...`)
      return false
    }
  }
  let _tearDownBindingRouter = () => {
    if (_zapRouter) {
      _zapRouter.close()
      _zapRouter = null
      log('ZAP server destroyed')
    }
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

    if (_settings.security) {
      _connectingRouter.curve_serverkey = _settings.security.serverPublicKey
      let clientKeys = _settings.security.secretKey
                          ? {public: _settings.security.publicKey, secret: _settings.security.secretKey}
                          : zmq.curveKeypair()
      _connectingRouter.curve_publickey = clientKeys.public
      _connectingRouter.curve_secretkey = clientKeys.secret
    }

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
    peer.liveness = HEARTBEAT_LIVENESS
    peer.heartbeatCheck = setInterval(() => {
      peer.liveness--
      log(`${peer.liveness} lives for ${peer.type} ${peer.id}`)
      if (!peer.liveness) {
        switch (peer.type) {
          case 'Client': return _onClientLost(peer)
          case 'Worker': return _onWorkerLost(peer)
          case 'Minister': return _onMinisterLost(peer)
        }
      }
    }, HEARTBEAT_INTERVAL)
  }
  let _unmonitor = (peer) => {
    if (peer.heartbeatCheck) {
      clearInterval(peer.heartbeatCheck)
      delete peer.heartbeatCheck
    }
  }
  let _onClientLost = (client) => {
    _unmonitor(client)
    let pendingReceivedRequests = _requestsFromClient(client)
    log(`Lost connection with client ${client.id}.
      Discarding ${pendingReceivedRequests.length} requests.`)
    pendingReceivedRequests.forEach(request => request.lostStakeholder())
    pull(_requests, ...pendingReceivedRequests)
    pull(_clients, client)
  }
  let _onWorkerLost = (worker) => {
    _unmonitor(worker)
    let pendingAssignedRequests = _requestsAssignedToWorker(worker)
    log(`Lost connection with worker ${worker.id}
      Discarding ${pendingAssignedRequests.length} assigned requests.`)
    pendingAssignedRequests.forEach(request => request.lostWorker())
    pull(_requests, ...pendingAssignedRequests)
    pull(_workers, worker)
  }
  let _onMinisterLost = (minister) => {
    _unmonitor(minister)
    let pendingReceivedRequests = _requestsFromMinister(minister)
    let pendingAssignedRequests = _requestsAssignedToMinister(minister)
    log(`Lost connection with minister ${minister.id}
      Discarding ${pendingReceivedRequests.length} received requests.
      Discarding ${pendingAssignedRequests.length} assigned requests.`)

    pendingReceivedRequests.forEach(request => request.lostStakeholder())
    pendingAssignedRequests.forEach(request => request.lostWorker())
    pull(_requests, ...pendingReceivedRequests, ...pendingAssignedRequests)
    pull(_ministers, minister)
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
          log(`Potential minister at ${endpoint}`)
          getMinisterLatency(endpoint)
            .then(latency => {
              log(`Minister at ${endpoint} is reachable with a latency of ${latency}ms. Connecting...`)
              // Establish a connection to the peer minister
              _connectingRouter.connect(endpoint)
              //  Establish a notifier connection
              let notifier = zmq.socket('dealer')
              if (_settings.security) {
                notifier.curve_serverkey = _settings.security.serverPublicKey
                let clientKeys = _settings.security.secretKey
                                    ? {public: _settings.security.publicKey, secret: _settings.security.secretKey}
                                    : zmq.curveKeypair()
                notifier.curve_publickey = clientKeys.public
                notifier.curve_secretkey = clientKeys.secret
              }
              notifier.connect(endpoint)

              let connectionsSubscription = _connectingRouterConnections.delay(latency * 5).subscribe(ep => {
                if (ep === endpoint) {
                  connectionsSubscription.unsubscribe()
                  log(`Connected to minister at ${ep}`)
                  log(`Presenting myself (${_connectingRouter.identity}) through notifier\n`)
                  // Notify the minister about myself
                  notifier.send(ministerConnectedMessage(JSON.stringify({
                    identity: _connectingRouter.identity,
                    latency
                  })))
                  notifier.close()
                }
              })
            })
            .catch(() => {
              log(`Could not reach minister at ${endpoint}\n`)
            })
        })
      })
  }
  let _broadcastWorkersAvailability = () => {
    log(`Broadcasting workers availability to ministers (${_ministers.length})`)
    let workers = JSON.stringify(_workers.map(workerToState))
    _ministers.forEach(minister => minister.send(ministerWorkersAvailabilityMessage(workers)))
  }
  let _sendDisconnectionMessages = () => {
    log('Broadcasting disconnection signal')
    let disconnectionMessage = ministerDisconnectionMessage(JSON.stringify(
      _ministers.map(({endpoint}) => endpoint))
    )
    _ministers.forEach(minister => minister.send(disconnectionMessage))
    _workers.forEach(worker => worker.send(disconnectionMessage))
    _clients.forEach(client => client.send(disconnectionMessage))
  }

  // Requests assignment
  let _assignRequests = () => {
    _requestsUnassigned()
      .forEach(request => {
        let worker = _workerForService(request.service)
        let minister = _ministerForService(request.service)
        let assignee = worker && worker.freeSlots
                        ? worker
                        : minister && minister.slotsForService(request.service)
                          ? minister
                          : null
        if (assignee) {
          assignee.send(request.frames)
          request.assignee = assignee
          assignee.assignedRequests++
        }
      })
  }

  // Public API
  function start () {
    if (_connected || _togglingConnection) return minister
    log('Connecting...')
    if (_setupBindingRouter()) {
      _setupConnectingRouter()
      _unsubscribeFromBindingRouter = _observeRouter(_bindingRouter, true)
      _unsubscribeFromConnectingRouter = _observeRouter(_connectingRouter, false)

      // Periodically try to assign unassigned requests
      _requestAssigningInterval = setInterval(
        () => _assignRequests(),
        200
      )
      // Periodically notify other ministers about own workers state
      _ministersUpdateWorkersAvailabilityInterval = setInterval(
        () => _broadcastWorkersAvailability(),
        HEARTBEAT_INTERVAL
      )

      _connected = true
      log('Connected.')
      process.nextTick(() => {
        minister.emit('connection')
        _presentToMinisters()
      })
    } else {
      log('Connection failed.')
    }
  }
  function stop () {
    if (!_connected || _togglingConnection) return minister
    _togglingConnection = true

    log('Disconnecting...')

    // Stop request assign routine
    clearInterval(_requestAssigningInterval)
    _requestAssigningInterval = null
    // Stop notifications to other ministers about services availability
    clearInterval(_ministersUpdateWorkersAvailabilityInterval)
    _ministersUpdateWorkersAvailabilityInterval = null

    // Declare unavaliability to connected clients, workers, and ministers
    _sendDisconnectionMessages()
    // Stop taking messages
    _unsubscribeFromBindingRouter()
    _unsubscribeFromConnectingRouter()
    // Unmonitor peers
    _clients.forEach(_unmonitor)
    _workers.forEach(_unmonitor)
    _ministers.forEach(_unmonitor)

    let farthestPeerLatency = max(
      _clients.map(({latency}) => latency)
      .concat(
        _workers.map(({latency}) => latency)
      )
      .concat(
        _ministers.map(({latency}) => latency)
      )
    ) || 1
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
    }, farthestPeerLatency * 2)
  }

  Object.defineProperties(minister, {
    start: {value: start},
    stop: {value: stop}
  })
  return minister
}

const defaultSettings = {
  ip: null,
  port: 5555,
  ministers: []
}

const eMsg = prefixString('Minister(settings): ')
function validateSettings (settings) {
  let {ip, port, ministers, security} = settings

  // Ip
  if (
    ip &&
    !net.isIPv4(ip)
  ) throw new Error(eMsg('settings.ip MUST be either `undefined` or a valid IPv4 address'))

  // Port
  if (!isInteger(port) || port < 1) throw new Error(eMsg('settings.port MUST be a positive integer'))

  // Ministers
  let ministersErrorMessage = eMsg('settings.ministers MUST be either a string, representing a hostname, or an array of 0 or more valid TCP endpoints, in the form of \'tcp://IP:port\'')
  if (
    !ministers ||
    (!isString(ministers) && !isArray(ministers))
  ) throw new Error(ministersErrorMessage)
  if (
    isArray(ministers) &&
    !every(ministers, isValidEndpoint)
  ) throw new Error(ministersErrorMessage)
  if (isString(ministers) && ip) throw new Error(eMsg(`if your ministers\'addresses are resolvable via DNS at ${ministers} you MUST NOT set settings.ip`))

  // Security
  if (security) {
    if (!supportingCurveSecurity()) throw new Error(eMsg('cannot setup curve security. libsodium seems to be missing.'))
    if (!isPlainObject(security)) throw new Error(eMsg('settings.security should be either `undefined` or a plain object'))
    if (!isValidCurveKeyPair(security.serverSecretKey, security.serverPublicKey)) throw new Error(eMsg('settings.security.serverPublicKey and settings.security.serverSecretKey MUST be a valid z85 encoded Curve25519 keypair. You can generate them through zmq.curveKeypair()'))
    if (security.allowedClientKeys) {
      if (
        !isArray(security.allowedClientKeys) ||
        !every(security.allowedClientKeys, isValidCurveKey)
      ) throw new Error(eMsg('settings.security.allowedClientKeys MUST be either `undefined` or a list of 40 chars strings'))
    }
    if (
      (security.secretKey || security.publicKey) &&
      !isValidCurveKeyPair(security.secretKey, security.publicKey)
    ) throw new Error(eMsg('settings.security.publicKey and settings.security.secretKey MUST be a valid z85 encoded Curve25519 keypair. You can generate them through zmq.curveKeypair()'))
  }
}

export default Minister
