import net from 'net'
import D from 'debug'
import zmq from 'zeromq'
import z85 from 'z85'
import Rx from 'rxjs'
import uuid from 'uuid'
import Promise from 'bluebird'
import EventEmitter from 'eventemitter3'
import {
  isString,
  isInteger,
  isArray,
  isPlainObject,
  each,
  every,
  pull,
  max,
  tail
} from 'lodash'
import compose from 'lodash/fp/compose'

// Utils
import {
  // getOSNetworkExternalInterface,
  isValidHostname,
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
  isClientDeactivateRequest,

  isWorkerReady,
  isWorkerHeartbeat,
  isWorkerDisconnect,
  isWorkerPartialResponse,
  isWorkerFinalResponse,
  isWorkerErrorResponse,

  isMinisterHello,
  isMinisterWorkersAvailability,
  isMinisterDisconnect,
  isMinisterRequestLostStakeholder,

  isMinisterNotifierNewMinisterConnecting,

  ministerHelloMessage,
  ministerHeartbeatMessage,
  ministerWorkersAvailabilityMessage,
  ministerDisconnectMessage,

  notifierNewMinisterConnectingMessage
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
  findWorkerForService
} from './helpers/workers'

// Ministers
import {
  getMinisterInstance,
  findMinisterById,
  findMinisterForService,
  discoverMinistersEndpoints,
  getMinisterLatency
} from './helpers/ministers'

// Requests
import {
  getMinisterRequestInstance,
  findUnassignedRequests,
  findRequestsByStakeholder,
  findRequestsByAssignee,
  findRequestByUUID
} from './helpers/requests'

// Constants
import {
  HEARTBEAT_LIVENESS,
  HEARTBEAT_INTERVAL
} from './CONSTANTS'

const Minister = (settings) => {
  let debug = D('ministers:minister')
  let minister = new EventEmitter()

  let _settings = {...defaultSettings, ...settings}
  validateSettings(_settings)

  // Private API
  let _connected = false
  let _togglingConnection = false
  let _heartbeatsInterval
  let _heartbeatMessage = []
  let _requestAssigningInterval

  let _clients = []
  let _clientById = findClientById(_clients)

  let _workers = []
  let _workerById = findWorkerById(_workers)
  let _workerForService = findWorkerForService(_workers)

  let _ministers = []
  let _potentialPeersByConnectionToken = {}
  let _ministerById = findMinisterById(_ministers)
  let _ministerForService = findMinisterForService(_ministers)

  let _requests = []
  let _requestByUUID = findRequestByUUID(_requests)
  let _requestsFrom = findRequestsByStakeholder(_requests)
  let _requestsAssignedTo = findRequestsByAssignee(_requests)
  // let _requestsFromClient = findRequestsByClientStakeholder(_requests)
  // let _requestsFromMinister = findRequestsByMinisterStakeholder(_requests)
  // let _requestsAssignedToWorker = findRequestsByWorkerAssignee(_requests)
  // let _requestsAssignedToMinister = findRequestsByMinisterAssignee(_requests)
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
      _clients.push(client)
      client.send(_heartbeatMessage)
      debug(`Client connected ${client.name}`)
      _monitor(client)
      minister.emit('client:connection', client.toJS())
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
        frames,
        options,
        onFinished: () => pull(_requests, request)
      })
      _requests.push(request)
      debug(`Received request ${request.shortId} from ${stakeholder.type} ${stakeholder.name}`)
      _assignRequests()
    }
  }
  let _onClientDeactivateRequest = (msg) => {
    let client = _clientById(msg[0])
    let uuid = msg[3].toString()
    let request = _requestByUUID(uuid)
    if (
      request &&
      request.stakeholder === client
    ) request.lostStakeholder()
  }
  // Worker messages
  let _onWorkerReady = (msg) => {
    let workerId = msg[0]
    let {service, concurrency, latency} = JSON.parse(msg[3])
    let worker = _workerById(workerId)
    if (!worker) {
      worker = getWorkerInstance({
        router: _bindingRouter,
        id: workerId,
        service,
        concurrency,
        latency
      })
      _workers.push(worker)
      worker.send(_heartbeatMessage)
      _broadcastWorkersAvailability()
      debug(`Worker connected ${worker.name}`)
      _monitor(worker)
      _assignRequests()
      minister.emit('worker:connection', worker.toJS())
    }
  }
  let _onWorkerHeartbeat = (msg) => {
    let worker = _workerById(msg[0])
    if (worker) {
      _monitor(worker)
      let { concurrency, pendingRequests } = JSON.parse(msg[3])
      if (
        worker.concurrency !== concurrency ||
        worker.pendingRequests !== pendingRequests
      ) {
        Object.assign(worker, {concurrency, pendingRequests})
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
    let {fromBoundRouter, token, latency, endpoint} = JSON.parse(msg[3])

    let m = _ministerById(ministerId)
    if (m) return

    if (fromBoundRouter) {
      if (!token || !_potentialPeersByConnectionToken[token]) return
      let {endpoint, latency} = _potentialPeersByConnectionToken[token]
      delete _potentialPeersByConnectionToken[token]
      m = getMinisterInstance({
        router: _connectingRouter,
        id: ministerId,
        latency,
        endpoint
      })
      debug(`Received HELLO from minister bound at ${endpoint}. Sending back HELLO message`)
      m.send(ministerHelloMessage(JSON.stringify({
        fromBoundRouter: false,
        latency,
        endpoint: _bindingRouter.publicEndpoint
      })))
    } else {
      m = getMinisterInstance({
        router: _bindingRouter,
        id: ministerId,
        latency,
        endpoint
      })
      debug(`Received HELLO from connected minister`)
    }

    debug(`Minister name: ${m.name}`)
    debug(`Minister latency: ${m.latency} milliseconds\n`)
    _ministers.push(m)
    _broadcastWorkersAvailability()
    _monitor(m)
    minister.emit('minister:connection', m.toJS())
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
  let _onMinisterRequestLostStakeholder = (msg) => {
    let uuid = msg[3].toString()
    let request = _requestByUUID(uuid)
    if (request) request.lostStakeholder()
  }
  // MinisterNotifier messages
  let _onNewMinisterConnecting = (msg) => {
    let { identity, token } = JSON.parse(msg[3])

    // Found myself resolving DNS
    if (identity === _bindingRouter.identity) {
      _bindingRouter.publicEndpoint = _potentialPeersByConnectionToken[token].endpoint
      debug(`Discovered my own endpoint: ${_bindingRouter.publicEndpoint}`)
      delete _potentialPeersByConnectionToken[token]
      return
    }

    debug(`Received notification of a new connected minister (${identity.substring(0, 11)}). Sending HELLO message\n`)
    let infos = JSON.stringify({
      fromBoundRouter: true,
      token
    })
    _bindingRouter.send([identity, ...ministerHelloMessage(infos)])
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
          debug('ZAP: Authorizing connection')
        } else {
          statusText = 'Unauthorized public key'
          debug('ZAP: Not authorizing connection')
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
      debug('ZAP with CURVE Mechanism enabled')
    }

    let bindIp = _settings.ip || '0.0.0.0'
    let bindingEndpoint = `tcp://${bindIp}:${_settings.port}`
    _bindingRouter.bindSync(bindingEndpoint)
    debug(`Binding Router bound to ${bindingEndpoint}`)
    if (isArray(_settings.ministers)) {
      _bindingRouter.publicEndpoint = _settings.advertiseEndpoint
      debug(`Binding Router public endpoint: ${_bindingRouter.publicEndpoint}`)
    } else {
      debug(`Binding Router public endpoint will be discovered through DNS resolution`)
    }
  }
  let _tearDownBindingRouter = () => {
    if (_zapRouter) {
      _zapRouter.close()
      _zapRouter = null
      debug('ZAP server destroyed')
    }
    _unsubscribeFromBindingRouter()
    _unsubscribeFromBindingRouter = null
    _bindingRouter.close()
    _bindingRouter = null
    debug('Binding Router destroyed')
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

    debug('Connecting Router created')
  }
  let _tearDownConnectingRouter = () => {
    _unsubscribeFromConnectingRouter()
    _unsubscribeFromConnectingRouter = null
    _connectingRouterConnections = null
    _connectingRouter.unmonitor()
    _connectingRouter.close()
    _connectingRouter = null
    debug('Connecting Router destroyed')
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
    let clientMessages = messages.filter(compose(isClientMessage, tail))
    let workerMessages = messages.filter(compose(isWorkerMessage, tail))
    let ministerMessages = messages.filter(compose(isMinisterMessage, tail))
    let ministerNotifierMessages = messages.filter(compose(isMinisterNotifierMessage, tail))

    subscriptions.clientHello = clientMessages
      .filter(compose(isClientHello, tail)).subscribe(_onClientHello)
    subscriptions.clientHeartbeat = clientMessages
      .filter(compose(isClientHeartbeat, tail)).subscribe(_onClientHeartbeat)
    subscriptions.clientDisconnect = clientMessages
      .filter(compose(isClientDisconnect, tail)).subscribe(_onClientDisconnect)
    subscriptions.clientRequest = clientMessages
      .filter(compose(isClientRequest, tail)).subscribe(_onClientRequest)
    subscriptions.clientDeactivateRequest = clientMessages
      .filter(compose(isClientDeactivateRequest, tail)).subscribe(_onClientDeactivateRequest)

    subscriptions.workerReady = workerMessages
      .filter(compose(isWorkerReady, tail)).subscribe(_onWorkerReady)
    subscriptions.workerHeartbeat = workerMessages
      .filter(compose(isWorkerHeartbeat, tail)).subscribe(_onWorkerHeartbeat)
    subscriptions.workerDisconnect = workerMessages
      .filter(compose(isWorkerDisconnect, tail)).subscribe(_onWorkerDisconnect)
    subscriptions.workerPartialResponse = workerMessages
      .filter(compose(isWorkerPartialResponse, tail)).subscribe(_onWorkerPartialResponse)
    subscriptions.workerFinalResponse = workerMessages
      .filter(compose(isWorkerFinalResponse, tail)).subscribe(_onWorkerFinalResponse)
    subscriptions.workerErrorResponse = workerMessages
      .filter(compose(isWorkerErrorResponse, tail)).subscribe(_onWorkerErrorResponse)

    subscriptions.ministerHello = ministerMessages
      .filter(compose(isMinisterHello, tail)).subscribe(_onMinisterHello)
    subscriptions.ministerWorkersAvailability = ministerMessages
      .filter(compose(isMinisterWorkersAvailability, tail)).subscribe(_onMinisterWorkersAvailability)
    subscriptions.ministerDisconnect = ministerMessages.filter(compose(isMinisterDisconnect, tail)).subscribe(_onMinisterDisconnect)
    subscriptions.ministerInactiveRequest = ministerMessages
      .filter(compose(isMinisterRequestLostStakeholder, tail)).subscribe(_onMinisterRequestLostStakeholder)

    subscriptions.ministerNotifierNewMinisterConnected = ministerNotifierMessages
      .filter(compose(isMinisterNotifierNewMinisterConnecting, tail)).subscribe(_onNewMinisterConnecting)

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
    debug(`${peer.liveness} remaining lives for ${peer.type} ${peer.name}`)
    peer.heartbeatCheck = setInterval(() => {
      peer.liveness--
      debug(`${peer.liveness} remaining lives for ${peer.type} ${peer.name}`)
      if (!peer.liveness) {
        switch (peer.type) {
          case 'client': return _onClientLost(peer)
          case 'worker': return _onWorkerLost(peer)
          case 'minister': return _onMinisterLost(peer)
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
    let pendingReceivedRequests = _requestsFrom(client)
    debug(`Lost connection with client ${client.name}.`)
    debug(`Discarded ${pendingReceivedRequests.length} received requests.`)

    pendingReceivedRequests.forEach(request => request.lostStakeholder())
    pull(_clients, client)
    minister.emit('client:disconnection', client.toJS())
  }
  let _onWorkerLost = (worker) => {
    _unmonitor(worker)
    let pendingAssignedRequests = _requestsAssignedTo(worker)
    debug(`Lost connection with worker ${worker.name}`)
    debug(`Discarding ${pendingAssignedRequests.length} assigned requests.`)
    pendingAssignedRequests.forEach(request => request.lostWorker())
    pull(_requests, ...pendingAssignedRequests)
    pull(_workers, worker)
    minister.emit('worker:disconnection', worker.toJS())
  }
  let _onMinisterLost = (m) => {
    _unmonitor(m)
    let pendingReceivedRequests = _requestsFrom(m)
    let pendingAssignedRequests = _requestsAssignedTo(m)
    debug(`Lost connection with minister ${m.name}`)
    debug(`Discarding ${pendingReceivedRequests.length} received requests`)
    debug(`Discarding ${pendingAssignedRequests.length} assigned requests.`)

    pendingReceivedRequests.forEach(request => request.lostStakeholder())
    pendingAssignedRequests.forEach(request => request.lostWorker())
    pull(_ministers, m)
    minister.emit('minister:disconnection', m.toJS())
  }
  let _presentToMinisters = () => {
    let getMinistersEndpoints
    if (isString(_settings.ministers)) {
      getMinistersEndpoints = discoverMinistersEndpoints({
        host: _settings.ministers,
        port: _settings.port
      })
    } else {
      getMinistersEndpoints = Promise.resolve(_settings.ministers)
    }

    getMinistersEndpoints
      .then(endpoints => {
        endpoints.forEach(endpoint => {
          debug(`Potential minister at ${endpoint}`)
          getMinisterLatency(endpoint)
            .then(latency => {
              debug(`Minister at ${endpoint} is reachable with a latency of ${latency}ms. Trying to connect...`)
              // Set the recognition token
              let token = uuid.v4()
              _potentialPeersByConnectionToken[token] = {endpoint, latency}

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

                  if (!_connectingRouter) return notifier.close()

                  debug(`Connected to minister at ${ep}`)
                  debug(`Presenting myself (${_connectingRouter.identity}) through notifier\n`)
                  // Notify the minister about myself
                  let infos = JSON.stringify({
                    identity: _connectingRouter.identity,
                    connectingTo: endpoint,
                    token
                  })
                  notifier.send(notifierNewMinisterConnectingMessage(infos))
                  notifier.close()
                }
              })
            })
            .catch(() => {
              debug(`Could not reach minister at ${endpoint}\n`)
            })
        })
      })
  }
  let _broadcastHeartbeats = () => {
    debug(`Broadcasting heartbeat to ${_clients.length} clients and ${_workers.length} workers`)
    _clients.forEach(client => client.send(_heartbeatMessage))
    _workers.forEach(worker => worker.send(_heartbeatMessage))
    // notify other ministers about own workers state
    _broadcastWorkersAvailability()
  }
  let _broadcastWorkersAvailability = () => {
    debug(`Broadcasting workers availability to ${_ministers.length} ministers`)
    let workersAvailabilityMessage = ministerWorkersAvailabilityMessage(
      JSON.stringify(_workers.map(w => w.toJS()))
    )
    _ministers.forEach(minister => minister.send(workersAvailabilityMessage))
  }
  let _broadcastDisconnectionMessage = () => {
    debug('Broadcasting disconnection message')
    let disconnectionMessage = ministerDisconnectMessage(JSON.stringify(
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
                        : minister && minister.hasSlotsForService(request.service)
                          ? minister
                          : null
        if (assignee) {
          assignee.send(request.frames)
          request.assignee = assignee
          debug(`Assigned request ${request.shortId} to ${assignee.type} ${assignee.name}`)
        }
      })
  }

  // Public API
  function start () {
    if (_connected || _togglingConnection) return minister

    debug('Starting')

    _setupBindingRouter()
    _setupConnectingRouter()
    _unsubscribeFromBindingRouter = _observeRouter(_bindingRouter, true)
    _unsubscribeFromConnectingRouter = _observeRouter(_connectingRouter, false)

    // Periodically send heartbeats
    _heartbeatMessage = ministerHeartbeatMessage(_bindingRouter.identity)
    _heartbeatsInterval = setInterval(_broadcastHeartbeats, HEARTBEAT_INTERVAL)

    // Periodically try to assign unassigned requests
    _requestAssigningInterval = setInterval(
      () => _assignRequests(),
      200
    )

    _connected = true
    process.nextTick(() => {
      minister.emit('start')
      _presentToMinisters()
    })
    return minister
  }
  function stop () {
    if (!_connected || _togglingConnection) return minister
    _togglingConnection = true
    _heartbeatMessage = []

    // Stop request assign routine
    clearInterval(_requestAssigningInterval)
    _requestAssigningInterval = null
    // Stop notifications to other ministers about services availability
    clearInterval(_heartbeatsInterval)
    _heartbeatsInterval = null

    // Declare unavaliability to connected clients, workers, and ministers
    _broadcastDisconnectionMessage()
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
      _requests.length = 0
      _togglingConnection = false
      _connected = false
      debug('Stop')
      minister.emit('stop')
    }, farthestPeerLatency * 2)
    return minister
  }

  Object.defineProperties(minister, {
    start: {value: start},
    stop: {value: stop},
    id: {
      get () {
        if (_bindingRouter) return _bindingRouter.identity
        return null
      }
    },
    endpoint: {
      get () {
        if (_bindingRouter) return _bindingRouter.publicEndpoint || null
        return null
      }
    },
    clients: {get: () => _clients.map(c => c.toJS())},
    workers: {get: () => _workers.map(w => w.toJS())},
    peers: {get: () => _ministers.map(m => m.toJS())}
  })
  return minister
}

const defaultSettings = {
  ip: null,
  port: 5555,
  advertiseEndpoint: null,
  ministers: 'localhost'
}

const eMsg = prefixString('Minister(settings): ')
function validateSettings (settings) {
  let {ip, port, ministers, advertiseEndpoint, security} = settings

  // Ip
  if (ip && !net.isIPv4(ip)) throw new Error(eMsg('settings.ip MUST be either `undefined` or a valid IPv4 address'))

  // Port
  if (!isInteger(port) || port < 1) throw new Error(eMsg('settings.port MUST be a positive integer'))

  // Ministers
  let ministersErrorMessage = eMsg('settings.ministers MUST be either a string, representing a hostname, or an array of 0 or more valid TCP endpoints, in the form of \'tcp://IP:port\'')
  if (!ministers || (!isString(ministers) && !isArray(ministers))) throw new Error(ministersErrorMessage)
  if (isString(ministers) && !isValidHostname(ministers)) throw new Error(ministersErrorMessage)
  if (isArray(ministers) && !every(ministers, isValidEndpoint)) throw new Error(ministersErrorMessage)

  // Adverstise endpoint
  if (isArray(ministers) && !isValidEndpoint(advertiseEndpoint)) throw new Error(eMsg(`if you specify your ministers'addresses as a list of endpoints you MUST also set 'advertiseEndpoint'`))
  // if (isString(ministers) && ip) throw new Error(eMsg(`if your ministers'addresses are resolvable via DNS at ${ministers} you MUST NOT set settings.ip`))

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
