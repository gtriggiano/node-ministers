'use strict'

let should = require('should/as-function')

let M = require('../lib')

describe('CLIENT REQUEST PROPERTIES (request = client.request(service[, body][, options])):', () => {
  it('request.isClean (boolean) is true if the request has not already received any data', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => {
      setTimeout(() => res.write('data'), 5)
      setTimeout(() => res.end(), 10)
    })
    worker.on('connection', () => {
      let clientRequest = client.request('Test')
      clientRequest
        .on('data', () => should(clientRequest.isClean).be.False())
        .promise().then(() => {
          minister.stop()
        })
      should(clientRequest.isClean).be.True()
    })
    client.on('connection', () => worker.start())
    minister.on('start', () => client.start())
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('request.isFinished (boolean)', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => res.send('data'))
    worker.on('connection', () => {
      let clientRequest = client.request('Test')
      clientRequest.promise().then(() => {
        should(clientRequest.isFinished).be.True()
        minister.stop()
      })
      should(clientRequest.isFinished).be.False()
    })
    client.on('connection', () => worker.start())
    minister.on('start', () => client.start())
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('request.isAccomplished (boolean) is true if the request completed without errors', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    let c = 0
    worker.on('request', (req, res) => {
      c++
      if (c === 1) return res.send('data')
      res.error('ops...')
    })
    worker.on('connection', () => {
      let firstRequest = client.request('Test')
      firstRequest.promise().then(() => {
        should(firstRequest.isAccomplished).be.True()
        let secondRequest = client.request('Test')
        secondRequest.on('data', () => {})
        secondRequest.on('end', () => {
          should(secondRequest.isFinished).be.True()
          should(secondRequest.isAccomplished).be.False()
          minister.stop()
        })
      })
      should(firstRequest.isAccomplished).be.False()
    })
    client.on('connection', () => worker.start())
    minister.on('start', () => client.start())
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('request.isFailed (boolean) is true if the request completed with an error', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => res.error('ops...'))
    worker.on('connection', () => {
      let clientRequest = client.request('Test')
      clientRequest.on('data', () => {})
      clientRequest.on('end', () => {
        should(clientRequest.isFailed).be.True()
        minister.stop()
      })
      should(clientRequest.isFailed).be.False()
    })
    client.on('connection', () => worker.start())
    minister.on('start', () => client.start())
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('request.isTimedout (boolean) is true if the request completed with error because of timeout', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => setTimeout(() => res.send('data'), 100))
    worker.on('connection', () => {
      let clientRequest = client.request('Test', {}, {timeout: 50})
      clientRequest.on('data', () => {})
      clientRequest.on('end', () => {
        should(clientRequest.isFailed).be.True()
        should(clientRequest.isTimedout).be.True()
        minister.stop()
      })
      should(clientRequest.isTimedout).be.False()
    })
    client.on('connection', () => worker.start())
    minister.on('start', () => client.start())
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('request.receivedBytes (integer) is a getter which provides the total received bytes so far', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => {
      setTimeout(() => res.write(new Buffer(10)), 10)
      setTimeout(() => res.write(new Buffer(20)), 15)
      setTimeout(() => res.write(new Buffer(30)), 20)
      setTimeout(() => res.write(new Buffer(40)), 25)
      setTimeout(() => res.end(), 30)
    })
    worker.on('connection', () => {
      let clientRequest = client.request('Test')
      clientRequest.on('data', () => {})
      clientRequest.on('end', () => {
        should(clientRequest.receivedBytes).equal(100)
        minister.stop()
      })
      should(clientRequest.receivedBytes).equal(0)
    })
    client.on('connection', () => worker.start())
    minister.on('start', () => client.start())
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('request.isIdempotent (boolean)', () => {
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let req1 = client.request('Test')
    should(req1.isIdempotent).be.False()
    let req2 = client.request('Test', {}, {idempotent: true})
    should(req2.isIdempotent).be.True()
  })
  it('request.canReconnectStream (boolean)', () => {
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let req1 = client.request('Test', {})
    should(req1.canReconnectStream).be.False()
    let req2 = client.request('Test', {}, {idempotent: true})
    should(req2.canReconnectStream).be.False()
    let req3 = client.request('Test', {}, {idempotent: true, reconnectStream: true})
    should(req3.canReconnectStream).be.True()
    let req4 = client.request('Test', {}, {idempotent: false, reconnectStream: true})
    should(req4.canReconnectStream).be.False()
  })
})
