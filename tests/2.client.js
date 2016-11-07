'use strict'

let should = require('should/as-function')
let zmq = require('zmq')

let M = require('../lib')

describe('CLIENT:', () => {
  it('emits `start` and `stop`', (done) => {
    let client = M.Client({endpoint: 'localhost:5555'})
    client.on('start', () => client.stop())
    client.on('stop', () => done())
    client.start()
  })
  it('emits `connection`, passing connected minister infos: {id, name, enpoint}', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'localhost:5555'})
    client.on('connection', m => {
      should(m.id).be.a.String()
      should(m.name).be.a.String()
      should(m.endpoint).be.a.String()
      minister.stop()
    })
    minister.on('stop', () => {
      client.stop()
      done()
    })
    minister.start()
    client.start()
  })
  it('emits `disconnection`, passing disconnected minister infos: {id, name, enpoint}', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'localhost:5555'})
    client.on('connection', () => client.stop())
    client.on('disconnection', m => {
      should(m.id).be.a.String()
      should(m.name).be.a.String()
      should(m.endpoint).be.a.String()
      minister.stop()
    })
    minister.on('stop', () => {
      done()
    })
    minister.start()
    client.start()
  })

  describe('Factory method Ministers.Client(settings) throws', () => {
    it('if settings.endpoint is neither a tcp endpoint or a string like hostname:port', () => {
      should(() => M.Client()).throw()
      should(() => M.Client({})).throw()
      should(() => M.Client({endpoint: true})).throw()
      should(() => M.Client({endpoint: 'bad'})).throw()
      should(() => M.Client({endpoint: '@github.com'})).throw()
      should(() => M.Client({endpoint: '@github.com:8080'})).throw()
      should(() => M.Client({endpoint: 'tcp://127.0.0.256:8080'})).throw()

      should(() => M.Client({endpoint: 'github.com:8080'})).not.throw()
      should(() => M.Client({endpoint: 'tcp://127.0.0.1:8080'})).not.throw()
    })
    it('if settings.security is truthy and is not a plain object', () => {
      let keys = zmq.curveKeypair()
      function Klass () {
        this.serverPublicKey = keys.public
      }

      should(() => M.Client({
        endpoint: 'tcp://127.0.0.1:8080',
        security: true
      })).throw()
      should(() => M.Client({
        endpoint: 'tcp://127.0.0.1:8080',
        security: 2
      })).throw()
      should(() => M.Client({
        endpoint: 'tcp://127.0.0.1:8080',
        security: 'bad'
      })).throw()
      should(() => M.Client({
        endpoint: 'tcp://127.0.0.1:8080',
        security: new Klass()
      })).throw()
    })
    it('if settings.security.serverPublicKey is not a 40 chars string', () => {
      let keys = zmq.curveKeypair()

      should(() => M.Client({
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: true
        }
      })).throw()
      should(() => M.Client({
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: 2
        }
      })).throw()
      should(() => M.Client({
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: 'bad'
        }
      })).throw()

      should(() => M.Client({
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: keys.public
        }
      })).not.throw()
    })
    it('if settings.security.publicKey and settings.security.secretKey are neither both falsy or a valid z85 encoded Curve25519 keypair', () => {
      let keys = zmq.curveKeypair()

      should(() => M.Client({
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: keys.public,
          publicKey: 'bad',
          secretKey: keys.secret
        }
      })).throw()
      should(() => M.Client({
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: keys.public,
          publicKey: keys.public,
          secretKey: 'bad'
        }
      })).throw()
      should(() => M.Client({
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: keys.public,
          publicKey: keys.public
        }
      })).throw()
      should(() => M.Client({
        endpoint: 'tcp://127.0.0.1:8080',
        security: {
          serverPublicKey: keys.public,
          secretKey: keys.secret
        }
      })).throw()

      should(() => M.Client({
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
