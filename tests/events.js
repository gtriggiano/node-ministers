import should from 'should/as-function'

import M from '../src'

describe('MINISTER EVENTS', function () {
  it('emits `start` and `stop`', (done) => {
    let minister = M.Minister()
    minister.on('start', () => minister.stop())
    minister.on('stop', () => done())
    minister.start()
  })
  it('emits `client:connection`, passing connected client infos: {id}', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'localhost:5555'})
    minister.on('client:connection', c => {
      should(c.id).be.a.String()
      minister.stop()
      client.stop()
    })
    minister.on('stop', () => done())
    minister.start()
    client.start()
  })
  it('emits `client:disconnection`, passing disconnected client infos: {id}', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'localhost:5555'})
    minister.on('client:disconnection', c => {
      should(c.id).be.a.String()
      minister.stop()
    })
    minister.on('stop', () => done())
    minister.start()
    client.on('connection', () => client.stop())
    client.start()
  })
  it('emits `worker:connection`, passing connected worker infos: {id, service, latency}', (done) => {
    let minister = M.Minister()
    let worker = M.Worker({service: 'Test', endpoint: 'localhost:5555'})
    minister.on('worker:connection', w => {
      should(w.id).be.a.String()
      should(w.service).equal('Test')
      should(w.latency).be.an.Number()
      minister.stop()
      worker.stop()
    })
    minister.on('stop', () => done())
    minister.start()
    worker.start()
  })
  it('emits `worker:disconnection`, passing disconnected worker infos: {id, service, latency}', (done) => {
    let minister = M.Minister()
    let worker = M.Worker({service: 'Test', endpoint: 'localhost:5555'})
    minister.on('worker:disconnection', w => {
      should(w.id).be.a.String()
      should(w.service).equal('Test')
      should(w.latency).be.an.Number()
      minister.stop()
    })
    minister.on('stop', () => done())
    minister.start()
    worker.on('connection', () => worker.stop())
    worker.start()
  })
  it('emits `minister:connection`, passing connected minister infos: {id, latency, endpoint}', (done) => {
    let minister = M.Minister()
    let minister2 = M.Minister({port: 5557, ministers: ['tcp://127.0.0.1:5555']})
    minister.on('minister:connection', m => {
      should(m.id).be.a.String()
      should(m.latency).be.an.Number()
      should(m.endpoint).be.an.String()
      minister.stop()
    })
    minister.on('stop', () => minister2.stop())
    minister2.on('stop', () => done())
    minister.start()
    minister2.start()
  })
  it('emits `minister:disconnection`, passing disconnected minister infos: {id, latency, endpoint}', (done) => {
    let minister = M.Minister()
    let minister2 = M.Minister({port: 5557, ministers: ['tcp://127.0.0.1:5555']})
    minister.on('minister:connection', m => {
      minister2.stop()
    })
    minister.on('minister:disconnection', m => {
      should(m.id).be.a.String()
      should(m.latency).be.an.Number()
      should(m.endpoint).be.an.String()
      minister.stop()
    })
    minister.on('stop', () => done())
    minister.start()
    minister2.start()
  })
})

describe('CLIENT EVENTS', () => {
  it('emits `start` and `stop`', (done) => {
    let client = M.Client({endpoint: 'localhost:5555'})
    client.on('start', () => client.stop())
    client.on('stop', () => done())
    client.start()
  })
  it('emits `connection`, passing connected minister infos: {id, enpoint}', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'localhost:5555'})
    client.on('connection', m => {
      should(m.id).be.a.String()
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
  it('emits `disconnection`, passing disconnected minister infos: {id, enpoint}', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'localhost:5555'})
    client.on('connection', () => client.stop())
    client.on('disconnection', m => {
      should(m.id).be.a.String()
      should(m.endpoint).be.a.String()
      minister.stop()
    })
    minister.on('stop', () => {
      done()
    })
    minister.start()
    client.start()
  })
})

describe('WORKER EVENTS', () => {
  it('emits `start` and `stop`', (done) => {
    let worker = M.Worker({service: 'Test', endpoint: 'localhost:5555'})
    worker.on('start', () => worker.stop())
    worker.on('stop', () => done())
    worker.start()
  })
  it('emits `connection`, passing connected minister infos: {id, enpoint}', (done) => {
    let minister = M.Minister()
    let worker = M.Worker({service: 'Test', endpoint: 'localhost:5555'})
    worker.on('connection', m => {
      should(m.id).be.a.String()
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
  it('emits `disconnection`, passing disconnected minister infos: {id, enpoint}', (done) => {
    let minister = M.Minister()
    let worker = M.Worker({service: 'Test', endpoint: 'localhost:5555'})
    worker.on('connection', () => worker.stop())
    worker.on('disconnection', m => {
      should(m.id).be.a.String()
      should(m.endpoint).be.a.String()
      minister.stop()
    })
    minister.on('stop', () => {
      done()
    })
    minister.start()
    worker.start()
  })
})
