'use strict'

let should = require('should/as-function')
let zmq = require('zmq')

let M = require('../lib')

describe('MINISTER:', () => {
  it('emits `start` and `stop`', (done) => {
    let minister = M.Minister()
    minister.on('start', () => minister.stop())
    minister.on('stop', () => done())
    minister.start()
  })
  it('emits `client:connection`, passing connected client infos: {id, name}', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'localhost:5555'})
    minister.on('client:connection', c => {
      should(c.id).be.a.String()
      should(c.name).be.a.String()
      minister.stop()
      client.stop()
    })
    minister.on('stop', () => done())
    minister.start()
    client.start()
  })
  it('emits `client:disconnection`, passing disconnected client infos: {id, name}', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'localhost:5555'})
    minister.on('client:disconnection', c => {
      should(c.id).be.a.String()
      should(c.name).be.a.String()
      minister.stop()
    })
    minister.on('stop', () => done())
    minister.start()
    client.on('connection', () => client.stop())
    client.start()
  })
  it('emits `worker:connection`, passing connected worker infos: {id, name, service, latency}', (done) => {
    let minister = M.Minister()
    let worker = M.Worker({service: 'Test', endpoint: 'localhost:5555'})
    minister.on('worker:connection', w => {
      should(w.id).be.a.String()
      should(w.name).be.a.String()
      should(w.service).equal('Test')
      should(w.latency).be.an.Number()
      minister.stop()
      worker.stop()
    })
    minister.on('stop', () => done())
    minister.start()
    worker.start()
  })
  it('emits `worker:disconnection`, passing disconnected worker infos: {id, name, service, latency}', (done) => {
    let minister = M.Minister()
    let worker = M.Worker({service: 'Test', endpoint: 'localhost:5555'})
    minister.on('worker:disconnection', w => {
      should(w.id).be.a.String()
      should(w.name).be.a.String()
      should(w.service).equal('Test')
      should(w.latency).be.an.Number()
      minister.stop()
    })
    minister.on('stop', () => done())
    minister.start()
    worker.on('connection', () => worker.stop())
    worker.start()
  })
  it('emits `minister:connection`, passing connected minister infos: {id, name, latency, endpoint}', (done) => {
    let minister = M.Minister()
    let minister2 = M.Minister({port: 5557, ministers: ['tcp://127.0.0.1:5555']})
    minister.on('minister:connection', m => {
      should(m.id).be.a.String()
      should(m.name).be.a.String()
      should(m.latency).be.an.Number()
      should(m.endpoint).be.an.String()
      minister.stop()
    })
    minister.on('stop', () => minister2.stop())
    minister2.on('stop', () => done())
    minister.start()
    minister2.start()
  })
  it('emits `minister:disconnection`, passing disconnected minister infos: {id, name, latency, endpoint}', (done) => {
    let minister = M.Minister()
    let minister2 = M.Minister({port: 5557, ministers: ['tcp://127.0.0.1:5555']})
    minister.on('minister:connection', m => {
      minister2.stop()
    })
    minister.on('minister:disconnection', m => {
      should(m.id).be.a.String()
      should(m.name).be.a.String()
      should(m.latency).be.an.Number()
      should(m.endpoint).be.an.String()
      minister.stop()
    })
    minister.on('stop', () => done())
    minister.start()
    minister2.start()
  })

  describe('Factory method Ministers.Minister([settings]) throws', () => {
    it('if settings.ip is truthy and is not a valid IPv4', () => {
      should(() => M.Minister({ip: true})).throw()
      should(() => M.Minister({ip: 2})).throw()
      should(() => M.Minister({ip: '151.101.16.256'})).throw()

      should(() => M.Minister({ip: '151.101.16.133'})).not.throw()
    })
    it('if settings.port is defined and is not a poistive integer', () => {
      should(() => M.Minister({port: true})).throw()
      should(() => M.Minister({port: -2})).throw()
      should(() => M.Minister({port: '8000'})).throw()

      should(() => M.Minister({port: 8000})).not.throw()
    })
    it('if settings.ministers is definded and is neither a string representing a hostname or an array of tcp endpoints', () => {
      should(() => M.Minister({ministers: true})).throw()
      should(() => M.Minister({ministers: -2})).throw()
      should(() => M.Minister({ministers: '@bad'})).throw()
      should(() => M.Minister({ministers: '!bad'})).throw()
      should(() => M.Minister({ministers: 'bad..com'})).throw()
      should(() => M.Minister({ministers: ['8000']})).throw()
      should(() => M.Minister({ministers: ['8000', 'tcp://151.101.16.133:8000']})).throw()

      should(() => M.Minister({ministers: 'github.com'})).not.throw()
      should(() => M.Minister({ministers: ['tcp://151.101.16.133:8000']})).not.throw()
    })
    it('if settings.ministers is a hostname while settings.ip has been defined', () => {
      should(() => M.Minister({ip: '127.0.0.1', ministers: 'github.com'})).throw()
    })
    it('if settings.security is truthy and is not a plain object', () => {
      let keys = zmq.curveKeypair()
      function Klass () {
        this.serverPublicKey = keys.public
        this.serverSecretKey = keys.secret
      }
      should(() => M.Minister({security: true})).throw()
      should(() => M.Minister({security: 2})).throw()
      should(() => M.Minister({security: new Klass()})).throw()
    })
    it('if settings.security.serverPublicKey and settings.security.serverSecretKey are not a valid z85 encoded Curve25519 keypair', () => {
      let keys = zmq.curveKeypair()
      let keys2 = zmq.curveKeypair()

      should(() => M.Minister({security: {}})).throw()
      should(() => M.Minister({security: {
        serverPublicKey: keys.public,
        serverSecretKey: 'bad'
      }})).throw()
      should(() => M.Minister({security: {
        serverPublicKey: 'bad',
        serverSecretKey: keys.secret
      }})).throw()
      should(() => M.Minister({security: {
        serverPublicKey: keys2.public,
        serverSecretKey: keys.secret
      }})).throw()

      should(() => M.Minister({security: {
        serverPublicKey: keys.public,
        serverSecretKey: keys.secret
      }})).not.throw()
    })
    it('if settings.security.allowedClientKeys is truthy and is not an array of 40 chars strings', () => {
      let keys = zmq.curveKeypair()

      should(() => M.Minister({
        security: {
          serverPublicKey: keys.public,
          serverSecretKey: keys.secret,
          allowedClientKeys: true
        }
      })).throw()
      should(() => M.Minister({
        security: {
          serverPublicKey: keys.public,
          serverSecretKey: keys.secret,
          allowedClientKeys: 2
        }
      })).throw()
      should(() => M.Minister({
        security: {
          serverPublicKey: keys.public,
          serverSecretKey: keys.secret,
          allowedClientKeys: ['bad', keys.public]
        }
      })).throw()

      should(() => M.Minister({
        security: {
          serverPublicKey: keys.public,
          serverSecretKey: keys.secret,
          allowedClientKeys: [keys.public]
        }
      })).not.throw()
    })
    it('if settings.security.publicKey and settings.security.secretKey are neither both falsy or a valid z85 encoded Curve25519 keypair', () => {
      let keys = zmq.curveKeypair()
      let keys2 = zmq.curveKeypair()

      should(() => M.Minister({
        security: {
          serverPublicKey: keys.public,
          serverSecretKey: keys.secret,
          publicKey: keys.public
        }
      })).throw()
      should(() => M.Minister({
        security: {
          serverPublicKey: keys.public,
          serverSecretKey: keys.secret,
          secretKey: keys.secret
        }
      })).throw()
      should(() => M.Minister({
        security: {
          serverPublicKey: keys.public,
          serverSecretKey: keys.secret,
          publicKey: keys.public,
          secretKey: keys2.secret
        }
      })).throw()

      should(() => M.Minister({
        security: {
          serverPublicKey: keys.public,
          serverSecretKey: keys.secret,
          publicKey: keys2.public,
          secretKey: keys2.secret
        }
      })).not.throw()
    })
  })
})
