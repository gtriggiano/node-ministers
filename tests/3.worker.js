'use strict'

let should = require('should/as-function')
let zmq = require('zeromq')

let M = require('../lib')

describe('WORKER:', () => {
  it('emits `start` and `stop`', (done) => {
    let worker = M.Worker({service: 'Test', endpoint: 'localhost:5555'})
    worker.on('start', () => worker.stop())
    worker.on('stop', () => done())
    worker.start()
  })
  it('emits `connection`, passing connected minister infos: {id, name, enpoint}', (done) => {
    let minister = M.Minister()
    let worker = M.Worker({service: 'Test', endpoint: 'localhost:5555'})
    worker.on('connection', m => {
      should(m.id).be.a.String()
      should(m.name).be.a.String()
      should(m.endpoint).be.a.String()
      minister.stop()
    })
    minister.on('stop', () => {
      worker.stop()
      done()
    })
    minister.start()
    worker.start()
  })
  it('emits `disconnection`, passing disconnected minister infos: {id, name, enpoint}', (done) => {
    let minister = M.Minister()
    let worker = M.Worker({service: 'Test', endpoint: 'localhost:5555'})
    worker.on('connection', () => worker.stop())
    worker.on('disconnection', m => {
      should(m.id).be.a.String()
      should(m.name).be.a.String()
      should(m.endpoint).be.a.String()
      minister.stop()
    })
    minister.on('stop', () => {
      done()
    })
    minister.start()
    worker.start()
  })

  describe('Factory method Ministers.Worker(settings) throws', () => {
    it('if settings.service is not a nonempty string', () => {
      should(() => M.Worker({service: true, endpoint: 'tcp://127.0.0.1:5555'})).throw()
      should(() => M.Worker({service: 2, endpoint: 'tcp://127.0.0.1:5555'})).throw()
      should(() => M.Worker({service: '', endpoint: 'tcp://127.0.0.1:5555'})).throw()

      should(() => M.Worker({service: 'service', endpoint: 'tcp://127.0.0.1:5555'})).not.throw()
    })
    it('if settings.concurrency is defined and is not an integer', () => {
      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:5555',
        concurrency: true
      })).throw()
      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:5555',
        concurrency: 'bad'
      })).throw()
      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:5555',
        concurrency: NaN
      })).throw()

      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:5555',
        concurrency: 3
      })).not.throw()
    })
    it('if settings.endpoint is neither a valid tcp endpoint or a string like hostname:port', () => {
      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.256:5555'
      })).throw()
      should(() => M.Worker({
        service: 'service',
        endpoint: '@github.com:5555'
      })).throw()

      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.255:5555'
      })).not.throw()
      should(() => M.Worker({
        service: 'service',
        endpoint: 'github.com:5555'
      })).not.throw()
    })
    it('if settings.security is truthy and is not a plain object', () => {
      let keys = zmq.curveKeypair()
      function Klass () {
        this.serverPublicKey = keys.public
      }

      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:8080',
        security: true
      })).throw()
      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:8080',
        security: 2
      })).throw()
      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:8080',
        security: 'bad'
      })).throw()
      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:8080',
        security: new Klass()
      })).throw()
    })
    it('if settings.security.serverPublicKey is not a 40 chars string', () => {
      let keys = zmq.curveKeypair()

      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: true
        }
      })).throw()
      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: 2
        }
      })).throw()
      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: 'bad'
        }
      })).throw()

      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: keys.public
        }
      })).not.throw()
    })
    it('if settings.security.publicKey and settings.security.secretKey are neither both falsy or a valid z85 encoded Curve25519 keypair', () => {
      let keys = zmq.curveKeypair()

      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: keys.public,
          publicKey: 'bad',
          secretKey: keys.secret
        }
      })).throw()
      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: keys.public,
          publicKey: keys.public,
          secretKey: 'bad'
        }
      })).throw()
      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: keys.public,
          publicKey: keys.public
        }
      })).throw()
      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: keys.public,
          secretKey: keys.secret
        }
      })).throw()

      should(() => M.Worker({
        service: 'service',
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: keys.public,
          publicKey: keys.public,
          secretKey: keys.secret
        }
      })).not.throw()
    })
  })
})
