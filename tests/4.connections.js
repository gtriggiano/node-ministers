'use strict'

let should = require('should/as-function')
let zmq = require('zmq')
let lodash = require('lodash')
let range = lodash.range
let random = lodash.random
let every = lodash.every

let M = require('../lib')

describe('CONNECTIONS:', function () {
  it('ministers connect to each other', (done) => {
    let minister1 = M.Minister()
    let minister2 = M.Minister({port: 5557, ministers: ['tcp://127.0.0.1:5555']})
    let c = 0
    let check = () => {
      c++
      if (c === 2) {
        minister1.stop()
      }
    }
    minister1.once('minister:connection', m => {
      should(m.id).equal(minister2.id)
      check()
    })
    minister2.once('minister:connection', m => {
      should(m.id).equal(minister1.id)
      check()
    })
    minister1.on('start', () => minister2.start())
    minister1.on('stop', () => minister2.stop())
    minister2.on('stop', () => done())
    minister1.start()
  })
  it('ministers form a mesh network', (done) => {
    let numberOfMinisters = random(3, 7)
    let connectionsByMinisterID = {}
    let expectedConnectionEvents = numberOfMinisters * (numberOfMinisters - 1)
    let connectionEvents = 0
    let onConnection = (minister) => (connectedMinister) => {
      connectionEvents++
      connectionsByMinisterID[minister.id] = connectionsByMinisterID[minister.id] || []
      connectionsByMinisterID[minister.id].push(connectedMinister.id)
      if (connectionEvents === expectedConnectionEvents) ministers[0].stop()
    }
    let ministers = range(numberOfMinisters).map(n => {
      let isLast = n === numberOfMinisters - 1
      let port = 5555 + 2 * n
      let ministersAddresses = range(n)
        .map(n => `tcp://127.0.0.1:${5555 + 2 * n}`)
      let minister = M.Minister({port, ministers: ministersAddresses})
      if (!isLast) minister.on('start', () => ministers[n + 1].start())
      if (!isLast) minister.on('stop', () => ministers[n + 1].stop())
      if (isLast) {
        minister.on('stop', () => {
          should(
            every(connectionsByMinisterID,
              cc => cc.length === (numberOfMinisters - 1))
          ).be.True()
          done()
        })
      }
      minister.on('minister:connection', onConnection(minister))
      return minister
    })
    ministers[0].start()
  })
  it('when a minister stops, its clients reconnect to another minister', (done) => {
    let minister1 = M.Minister()
    let minister2 = M.Minister({port: 5557, ministers: ['tcp://127.0.0.1:5555']})
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})

    minister1.on('start', () => minister2.start())
    minister2.on('minister:connection', () => client.start())
    client.once('connection', (m) => {
      should(m.id).equal(minister1.id)
      // First time client connects to 127.0.0.1:5555
      client.once('connection', (m) => {
        should(m.id).equal(minister2.id)
        // Now client is connected to the public endpoint of minister2
        // as suggested by minister1 upon disconnection
        should(m.endpoint).equal(minister2.endpoint)
        minister2.stop()
        client.stop()
      })
      minister1.stop()
    })
    minister2.on('stop', () => done())
    minister1.start()
  })
  it('when a minister stops, its workers reconnect to another minister', (done) => {
    let minister1 = M.Minister()
    let minister2 = M.Minister({port: 5557, ministers: ['tcp://127.0.0.1:5555']})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    minister1.on('start', () => minister2.start())
    minister2.on('minister:connection', () => worker.start())
    worker.once('connection', (m) => {
      should(m.id).equal(minister1.id)
      // First time worker connects to localhost -> 127.0.0.1:5555
      worker.once('connection', (m) => {
        should(m.id).equal(minister2.id)
        // Now worker is connected to the public endpoint of minister2
        // as suggested by minister1 upon disconnection
        should(m.endpoint).equal(minister2.endpoint)
        minister2.stop()
        worker.stop()
      })
      minister1.stop()
    })
    minister2.on('stop', () => done())
    minister1.start()
  })
  it('ZAP security is supported', (done) => {
    let ministerKeys = zmq.curveKeypair()
    let client = M.Client({
      endpoint: 'tcp://127.0.0.1:5555',
      security: {
        serverPublicKey: ministerKeys.public
      }
    })
    let worker = M.Worker({
      service: 'Test',
      endpoint: 'tcp://127.0.0.1:5555',
      security: {
        serverPublicKey: ministerKeys.public
      }
    })
    let minister = M.Minister({
      security: {
        serverPublicKey: ministerKeys.public,
        serverSecretKey: ministerKeys.secret
      }
    })

    let cc = 0
    let onPeerConnection = () => {
      cc++
      if (cc === 2) {
        client.stop()
        worker.stop()
        minister.stop()
      }
    }

    minister.on('start', () => {
      client.start()
      worker.start()
    })
    client.on('connection', onPeerConnection)
    worker.on('connection', onPeerConnection)
    minister.on('stop', () => done())
    minister.start()
  })
  it('ministers can restrict access to certain curve keys', (done) => {
    let ministerKeys = zmq.curveKeypair()
    let clientKeys = zmq.curveKeypair()

    let client = M.Client({
      endpoint: 'tcp://127.0.0.1:5555',
      security: {
        serverPublicKey: ministerKeys.public,
        publicKey: clientKeys.public,
        secretKey: clientKeys.secret
      }
    })
    // Worker has autogenerated, not allowed, public key
    let worker = M.Worker({
      service: 'Test',
      endpoint: 'tcp://127.0.0.1:5555',
      security: {
        serverPublicKey: ministerKeys.public
      }
    })
    let minister = M.Minister({
      security: {
        serverPublicKey: ministerKeys.public,
        serverSecretKey: ministerKeys.secret,
        allowedClientKeys: [
          clientKeys.public
        ]
      }
    })

    let cc = 0
    let onPeerConnection = () => cc++

    minister.on('start', () => {
      worker.start()
      client.start()
    })
    client.on('connection', onPeerConnection)
    worker.on('connection', onPeerConnection)
    minister.on('stop', () => done())
    minister.start()
    setTimeout(function () {
      should(cc).equal(1)
      minister.stop()
    }, 50)
  })
})
