'use strict'

let fs = require('fs')
let should = require('should/as-function')
let sinon = require('sinon')
let uuid = require('uuid')

let M = require('../lib')

describe('REQUEST >> RESPONSE FLOW:', () => {
  it('a minister act as an async broker between a client and a worker', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => res.send('hello'))
    client.on('connection', () => {
      client.request('Test', {param: 'param', bool: true}, {someOption: 'option'})
        .on('data', buffer => should(buffer.toString()).equal('hello'))
        .on('end', () => minister.stop())

      setTimeout(() => worker.start(), 100)
    })
    minister.on('start', () => client.start())
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('a minister can delegate requests to other ministers', (done) => {
    let minister1 = M.Minister()
    let minister2 = M.Minister({port: 5557, ministers: ['tcp://127.0.0.1:5555']})
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5557'})

    worker.on('request', (req, res) => res.send('hello'))
    client.on('connection', () => {
      client.request('Test', {param: 'param', bool: true}, {someOption: 'option'})
        .on('data', buffer => should(buffer.toString()).equal('hello'))
        .on('end', () => minister1.stop())
    })
    minister1.on('start', () => minister2.start())
    minister2.on('start', () => { client.start(); worker.start() })
    minister1.on('stop', () => { minister2.stop(); client.stop(); worker.stop() })
    minister2.on('stop', () => done())
    minister1.start()
  })
  it('a worker\'s request handler has access to request body and options', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => {
      should(req.body.param).equal('hello')
      should(req.options.timeout).equal(60000)
      should(req.options.idempotent).be.False()
      should(req.options.reconnectStream).be.False()
      should(req.options.val).equal('world')
      minister.stop()
    })
    client.on('connection', () => {
      client.request('Test', {param: 'hello'}, {val: 'world'})
    })
    minister.on('start', () => { client.start(); worker.start() })
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('a client request behave like readable streams', (done) => {
    let newFile = `${__dirname}/${uuid.v4()}`
    let fileContent = uuid.v4()
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => res.send(fileContent))
    client.on('connection', () => {
      let fileStream = fs.createWriteStream(newFile)
      let request = client.request('Test')
      request.pipe(fileStream)
      request.on('end', () => {
        fileStream.end()
        should(fs.readFileSync(newFile, {encoding: 'utf8'})).equal(fileContent)
        fs.unlinkSync(newFile)
        minister.stop()
      })
    })
    minister.on('start', () => { client.start(); worker.start() })
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('a worker\'s request handler response object behave like writable streams', (done) => {
    let thisFile = `${__filename}`
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => fs.createReadStream(thisFile).pipe(res))
    client.on('connection', () => {
      let receivedData = ''
      let request = client.request('Test')
      request.on('data', buffer => receivedData += buffer.toString())
      request.on('end', () => {
        should(fs.readFileSync(thisFile, {encoding: 'utf8'})).equal(receivedData)
        minister.stop()
      })
    })
    minister.on('start', () => { client.start(); worker.start() })
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('a client request can use a partialCallback and finalCallback if passed through request\'s options', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => { res.write('hello'); res.send('world') })
    client.on('connection', () => {
      client.request('Test', {}, {
        partialCallback: buffer => should(buffer.toString()).equal('hello'),
        finalCallback: (e, buffer) => {
          should(e).equal(null)
          should(buffer.toString()).equal('world')
          minister.stop()
        }
      })
    })
    minister.on('start', () => { client.start(); worker.start() })
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('a client request can be turned to a promise through the .promise() method', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => res.send('hello world'))
    client.on('connection', () => {
      let request = client.request('Test')
      let promise = request.promise()
      let re_promise = request.promise()
      should(promise).be.a.Promise()
      should(promise === re_promise).be.True()
      promise.then(buffer => {
        should(buffer.toString()).equal('hello world')
        minister.stop()
      })
    })
    minister.on('start', () => { client.start(); worker.start() })
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('a client request can have a user defined timeout', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req) => {
      should(req.options.timeout).equal(100)
    })
    client.on('connection', () => {
      let now = new Date()
      client
        .request('Test', {}, {timeout: 100})
        .promise()
        .catch((e) => {
          let diff = (new Date()) - now
          should(diff).be.below(110)
          should(e).be.an.instanceof(Error)
          should(e.message).equal(M.REQUEST_TIMEOUT)
          minister.stop()
        })
    })
    minister.on('start', () => { client.start(); worker.start() })
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('a client request has a default timeout of 60 sec', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => {
      should(req.options.timeout).equal(60000)
      res.send('hello')
    })
    client.on('connection', () => {
      client
        .request('Test')
        .promise()
        .then(() => {
          minister.stop()
        })
    })
    minister.on('start', () => { client.start(); worker.start() })
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('a client request\'s timeout gets canceled as soon as first data is received', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => {
      setTimeout(() => res.write('hello'), 10)
      setTimeout(() => res.end(), 50)
    })
    worker.on('connection', () => {
      client
        .request('Test', {}, {timeout: 30})
        .promise()
        .then(() => {
          minister.stop()
        })
        .catch((e) => done(e))
    })
    client.on('connection', () => worker.start())
    minister.on('start', () => { client.start(); worker.start() })
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('a client request\'s timeout is restored if the request is rescheduled', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    let c = 0
    worker.on('request', (req, res) => {
      c++
      if (c === 1) {
        res.write('hello')
        setTimeout(() => worker.stop(), 10)
      }
    })
    worker.once('connection', () => {
      client
        .request('Test', {}, {timeout: 30, idempotent: true, reconnectStream: true})
        .promise()
        .then(() => done(new Error('request promise should not resolve')))
        .catch((e) => {
          should(e).be.an.instanceof(Error)
          should(e.message).equal(M.REQUEST_TIMEOUT)
          minister.stop()
        })
    })
    worker.once('stop', () => setTimeout(() => worker.start(), 10))
    client.on('connection', () => worker.start())
    minister.on('start', () => { client.start(); worker.start() })
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
})

describe('REQUEST >> RESPONSE FLOW WITH INTERRUPTIONS:', () => {
  describe('If a minister gets disconnected', () => {
    it('the worker\'s request handler can know that request is not active anymore', (done) => {
      let minister = M.Minister()
      let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
      let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

      worker.on('request', (req, res) => {
        should(req.isActive).be.True()
        setTimeout(() => {
          should(req.isActive).be.False()
          client.stop()
          worker.stop()
          setTimeout(() => done(), 20)
        }, 50)
      })
      client.on('connection', () => {
        worker.start()
        worker.on('connection', () => {
          client.request('Test')
          setTimeout(() => minister.stop(), 30)
        })
      })
      minister.on('start', () => client.start())
      minister.start()
    })
    describe('and the request is not idempotent', () => {
      it('the client request emits `error`', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        worker.on('request', (req, res) => {
          minister.stop()
          res.send('hello')
        })
        worker.on('connection', () => {
          client.request('Test')
            .on('data', () => done(new Error('request should not receive data')))
            .on('error', e => {
              should(e).be.an.instanceof(Error)
              should(e.message).equal(M.REQUEST_LOST_WORKER)
            })
        })
        client.on('connection', () => worker.start())
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.on('start', () => client.start())
        minister.start()
      })
      it('the client request\'s promise is rejected with an error', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        worker.on('request', (req, res) => {
          minister.stop()
          res.send('hello')
        })
        worker.on('connection', () => {
          client.request('Test')
            .promise()
            .then(() => done(new Error('request should not receive data')))
            .catch(e => {
              should(e).be.an.instanceof(Error)
              should(e.message).equal(M.REQUEST_LOST_WORKER)
            })
        })
        client.on('connection', () => worker.start())
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.on('start', () => client.start())
        minister.start()
      })
      it('the client request\'s finalCallback is called with an error', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        worker.on('request', (req, res) => {
          minister.stop()
          res.send('hello')
        })
        worker.on('connection', () => {
          client.request('Test', {}, {
            finalCallback: (e, response) => {
              should(e).be.an.instanceof(Error)
              should(e.message).equal(M.REQUEST_LOST_WORKER)
              should(response).be.undefined()
            }
          })
        })
        client.on('connection', () => worker.start())
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.on('start', () => client.start())
        minister.start()
      })
    })
    describe('and the request is idempotent and "clean" (has not already received any response)', () => {
      it('the client reschedules the request', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        worker.on('request', (req, res) => res.send('hello'))
        client.once('connection', () => {
          client
            .request('Test', {}, {idempotent: true})
            .promise()
            .then(buffer => {
              should(buffer.toString()).equal('hello')
              minister.stop()
            })
            .catch(e => done(e))

          setTimeout(() => minister.stop(), 20)
        })
        minister.once('stop', () => {
          minister.on('start', () => worker.start())
          minister.on('stop', () => { client.stop(); worker.stop(); done() })
          setTimeout(() => minister.start(), 20)
        })
        minister.on('start', () => client.start())
        minister.start()
      })
    })
    describe('and the request is idempotent and "dirty"', () => {
      it('the client request emits `error`', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        worker.on('request', (req, res) => {
          res.write(1)
          setTimeout(() => minister.stop(), 10)
        })
        worker.on('connection', () => {
          let chunks = []
          client.request('Test', {}, {idempotent: true})
            .on('data', (buffer) => chunks.push(JSON.parse(buffer)))
            .on('error', (e) => {
              should(chunks).eql([1])
              should(e).be.an.instanceof(Error)
              should(e.message).equal(M.REQUEST_LOST_WORKER)
            })
        })
        client.on('connection', () => worker.start())
        minister.on('start', () => { client.start() })
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.start()
      })
      it('the client request\'s promise is rejected with an error', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        worker.on('request', (req, res) => {
          res.write(1)
          setTimeout(() => minister.stop(), 10)
        })
        worker.on('connection', () => {
          let chunks = []
          client.request('Test', {}, {idempotent: true})
            .on('data', (buffer) => chunks.push(JSON.parse(buffer)))
            .promise()
            .then(() => done(new Error('promise should not be resolved')))
            .catch((e) => {
              should(chunks).eql([1])
              should(e).be.an.instanceof(Error)
              should(e.message).equal(M.REQUEST_LOST_WORKER)
            })
        })
        client.on('connection', () => worker.start())
        minister.on('start', () => { client.start() })
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.start()
      })
      it('the client request\'s finalCallback is called with an error', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        worker.on('request', (req, res) => {
          res.write(1)
          setTimeout(() => minister.stop(), 10)
        })
        worker.on('connection', () => {
          let chunks = []
          client.request('Test', {}, {
            idempotent: true,
            partialCallback: (buffer) => chunks.push(JSON.parse(buffer)),
            finalCallback: (e, response) => {
              should(chunks).eql([1])
              should(e).be.an.instanceof(Error)
              should(e.message).equal(M.REQUEST_LOST_WORKER)
              should(response).be.undefined()
            }
          })
        })
        client.on('connection', () => worker.start())
        minister.on('start', () => { client.start() })
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.start()
      })
      it('the client still reschedules the request if the request\'s options.reconnectStream = true', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        let chunk = 0
        worker.on('request', (req, res) => {
          chunk++
          res.write(chunk)
          if (chunk === 2) res.end()
        })
        worker.on('connection', () => {
          let chunks = []
          client.request('Test', {}, {idempotent: true, reconnectStream: true})
            .on('data', (buffer) => chunks.push(JSON.parse(buffer)))
            .on('end', () => {
              should(chunks).eql([1, 2])
              minister.stop()
            })
          setTimeout(() => minister.stop(), 20)
        })
        client.on('connection', () => worker.start())
        minister.once('stop', () => {
          minister.on('stop', () => { client.stop(); worker.stop(); done() })
          setTimeout(() => minister.start(), 20)
        })
        minister.on('start', () => client.start())
        minister.start()
      })
    })
  })
  describe('If the worker gets disconnected', () => {
    it('the worker\'s request handler can know that the request is not active anymore', (done) => {
      let minister = M.Minister()
      let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
      let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

      worker.on('request', (req) => {
        should(req.isActive).be.True()
        worker.on('stop', () => should(req.isActive).be.False())
        worker.stop()
      })
      worker.on('connection', () => client.request('Test').on('error', () => minister.stop()))
      client.on('connection', () => worker.start())
      minister.on('start', () => { client.start() })
      minister.on('stop', () => { client.stop(); done() })
      minister.start()
    })
    describe('and the request is not idempotent', () => {
      it('the client request emits `error`', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        worker.on('request', () => worker.stop())
        worker.on('connection', () => {
          client.request('Test')
            .on('error', (e) => {
              should(e).be.an.instanceof(Error)
              should(e.message).equal(M.REQUEST_LOST_WORKER)
              minister.stop()
            })
        })
        client.on('connection', () => worker.start())
        minister.on('start', () => { client.start() })
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.start()
      })
      it('the client request\'s promise is rejected with an error', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        worker.on('request', () => worker.stop())
        worker.on('connection', () => {
          client.request('Test')
            .promise()
            .then(() => done(new Error('promise should not be resolved')))
            .catch((e) => {
              should(e).be.an.instanceof(Error)
              should(e.message).equal(M.REQUEST_LOST_WORKER)
              minister.stop()
            })
        })
        client.on('connection', () => worker.start())
        minister.on('start', () => { client.start() })
        minister.on('stop', () => { client.stop(); done() })
        minister.start()
      })
      it('the client request\'s finalCallback is called with an error', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        worker.on('request', () => worker.stop())
        worker.on('connection', () => {
          client.request('Test', {}, {
            finalCallback: (e, response) => {
              should(e).be.an.instanceof(Error)
              should(e.message).equal(M.REQUEST_LOST_WORKER)
              should(response).be.undefined()
              minister.stop()
            }
          })
        })
        client.on('connection', () => worker.start())
        minister.on('start', () => { client.start() })
        minister.on('stop', () => { client.stop(); done() })
        minister.start()
      })
    })
    describe('and the request is idempotent and "clean" (has not already received any response)', () => {
      it('the client reschedules request', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        let attempt = 0
        worker.on('request', (req, res) => {
          attempt++
          if (attempt === 1) {
            setTimeout(() => worker.start(), 20)
            return worker.stop()
          }
          res.send('hello')
        })
        worker.once('connection', () => {
          client.request('Test', {}, {idempotent: true})
            .promise()
            .then(buffer => {
              should(buffer.toString()).equal('hello')
              minister.stop()
            })
            .catch(e => done(e))
        })
        client.once('connection', () => worker.start())
        minister.on('start', () => client.start())
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.start()
      })
    })
    describe('and the request is idempotent and "dirty"', () => {
      it('the client request emits `error`', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        worker.on('request', (req, res) => {
          res.write(1)
          setTimeout(() => worker.stop(), 10)
        })
        worker.once('connection', () => {
          let chunks = []
          client.request('Test', {}, {idempotent: true})
            .on('data', (buffer) => chunks.push(JSON.parse(buffer)))
            .on('error', (e) => {
              should(chunks).eql([1])
              should(e).be.an.instanceof(Error)
              should(e.message).equal(M.REQUEST_LOST_WORKER)
              minister.stop()
            })
        })
        client.on('connection', () => worker.start())
        minister.on('start', () => { client.start() })
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.start()
      })
      it('the client request\'s promise is rejected with an error', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        worker.on('request', (req, res) => {
          res.write(1)
          setTimeout(() => worker.stop(), 10)
        })
        worker.once('connection', () => {
          let chunks = []
          client.request('Test', {}, {idempotent: true})
            .on('data', (buffer) => chunks.push(JSON.parse(buffer)))
            .promise()
            .then(() => done(new Error('promise should not be resolved')))
            .catch((e) => {
              should(chunks).eql([1])
              should(e).be.an.instanceof(Error)
              should(e.message).equal(M.REQUEST_LOST_WORKER)
              minister.stop()
            })
        })
        client.on('connection', () => worker.start())
        minister.on('start', () => { client.start() })
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.start()
      })
      it('the client request\'s finalCallback is called with an error', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        worker.on('request', (req, res) => {
          res.write(1)
          setTimeout(() => worker.stop(), 10)
        })
        worker.once('connection', () => {
          let chunks = []
          client.request('Test', {}, {
            idempotent: true,
            partialCallback: (buffer) => chunks.push(JSON.parse(buffer)),
            finalCallback: (e, response) => {
              should(chunks).eql([1])
              should(e).be.an.instanceof(Error)
              should(e.message).equal(M.REQUEST_LOST_WORKER)
              should(response).be.undefined()
              minister.stop()
            }
          })
        })
        client.on('connection', () => worker.start())
        minister.on('start', () => { client.start() })
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.start()
      })
      it('the client still reschedules the request if the request\'s options.reconnectStream = true', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        let chunk = 0
        worker.on('request', (req, res) => {
          chunk++
          res.write(chunk)
          if (chunk === 2) res.end()
        })
        worker.once('connection', () => {
          let chunks = []
          client.request('Test', {}, {idempotent: true, reconnectStream: true})
            .on('data', (buffer) => chunks.push(JSON.parse(buffer)))
            .on('end', () => {
              should(chunks).eql([1, 2])
              minister.stop()
            })
          setTimeout(() => worker.stop(), 20)
        })
        worker.once('stop', () => setTimeout(() => worker.start(), 20))
        client.on('connection', () => worker.start())
        minister.on('start', () => { client.start() })
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.start()
      })
    })
  })
  describe('If the request is deactivated by client', () => {
    it('the client request emits `end`', (done) => {
      let minister = M.Minister()
      let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
      let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

      let request
      let chunk = 0
      worker.on('request', (req, res) => {
        let interval = setInterval(() => {
          if (chunk === 2) {
            request.deactivate()
            return clearInterval(interval)
          }
          chunk++
          res.write(chunk)
        }, 10)
      })
      client.on('connection', () => {
        let chunks = []
        request = client.request('Test')
          .on('data', (buffer) => chunks.push(JSON.parse(buffer)))
          .on('end', () => {
            should(chunks).eql([1, 2])
            minister.stop()
          })
      })
      worker.on('connection', () => client.start())
      minister.on('start', () => worker.start())
      minister.on('stop', () => { client.stop(); worker.stop(); done() })
      minister.start()
    })
    it('the client request\'s promise is resolved with no value', (done) => {
      let minister = M.Minister()
      let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
      let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

      let request
      let chunk = 0
      worker.on('request', (req, res) => {
        let interval = setInterval(() => {
          if (chunk === 2) {
            request.deactivate()
            return clearInterval(interval)
          }
          chunk++
          res.write(chunk)
        }, 10)
      })
      client.on('connection', () => {
        request = client.request('Test')

        request.promise()
          .then(buffer => {
            should(buffer).be.undefined()
            minister.stop()
          })
          .catch(e => done(e))
      })
      worker.on('connection', () => client.start())
      minister.on('start', () => worker.start())
      minister.on('stop', () => { client.stop(); worker.stop(); done() })
      minister.start()
    })
    it('the client request\'s finalCallback is called without arguments', (done) => {
      let minister = M.Minister()
      let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
      let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

      let request
      let chunk = 0
      worker.on('request', (req, res) => {
        let interval = setInterval(() => {
          if (chunk === 2) {
            request.deactivate()
            return clearInterval(interval)
          }
          chunk++
          res.write(chunk)
        }, 10)
      })
      client.on('connection', () => {
        let chunks = []
        request = client.request('Test', {}, {
          partialCallback: (buffer) => chunks.push(JSON.parse(buffer)),
          finalCallback: (e, response) => {
            should(chunks).eql([1, 2])
            should(e).be.undefined()
            should(response).be.undefined()
            minister.stop()
          }
        })
      })
      worker.on('connection', () => client.start())
      minister.on('start', () => worker.start())
      minister.on('stop', () => { client.stop(); worker.stop(); done() })
      minister.start()
    })
    it('the worker\'s request handler can know that the request is not active anymore', (done) => {
      let minister = M.Minister()
      let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
      let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

      let request
      let chunk = 0
      worker.on('request', (req, res) => {
        should(req.isActive).be.True()

        let interval = setInterval(() => {
          if (chunk === 2) {
            request.deactivate()
            return clearInterval(interval)
          }
          chunk++
          res.write(chunk)
        }, 10)

        request
          .promise()
          .then(() => setTimeout(() => {
            should(req.isActive).be.False()
            minister.stop()
          }, 10))
      })
      client.on('connection', () => {
        request = client.request('Test')
      })
      worker.on('connection', () => client.start())
      minister.on('start', () => worker.start())
      minister.on('stop', () => { client.stop(); worker.stop(); done() })
      minister.start()
    })
  })
})

describe('REQUEST >> RESPONSE FLOW WITH ERROR (when response.error(err) is called):', () => {
  it('the client request emits `error`', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => res.error())
    client.on('connection', () => {
      client.request('Test')
        .on('error', (e) => {
          should(e).be.an.instanceof(Error)
          minister.stop()
        })
    })
    minister.on('start', () => { client.start(); worker.start() })
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('the client request\'s promise is rejected with an error', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => res.error())
    client.on('connection', () => {
      client.request('Test')
        .promise()
        .then(() => done(new Error('promise should not be resolved')))
        .catch((e) => {
          should(e).be.an.instanceof(Error)
          minister.stop()
        })
    })
    minister.on('start', () => { client.start(); worker.start() })
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })
  it('the client request\'s finalCallback is called with an error', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => res.error())
    client.on('connection', () => {
      client.request('Test', {}, {
        finalCallback: (e, response) => {
          should(e).be.an.instanceof(Error)
          should(response).be.undefined()
          minister.stop()
        }
      })
    })
    minister.on('start', () => { client.start(); worker.start() })
    minister.on('stop', () => { client.stop(); worker.stop(); done() })
    minister.start()
  })

  describe('If `err` is a nonempty string', () => {
    it('the client error.message === err', (done) => {
      let minister = M.Minister()
      let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
      let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

      worker.on('request', (req, res) => res.error('error world!'))
      client.on('connection', () => {
        client.request('Test')
          .promise()
          .then(() => done(new Error('promise should not be resolved')))
          .catch((e) => {
            should(e).be.an.instanceof(Error)
            should(e.message).equal('error world!')
            minister.stop()
          })
      })
      minister.on('start', () => { client.start(); worker.start() })
      minister.on('stop', () => { client.stop(); worker.stop(); done() })
      minister.start()
    })
  })
  describe('If `err` is falsy', () => {
    it('the client error.message === `request failed`', (done) => {
      let minister = M.Minister()
      let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
      let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

      worker.on('request', (req, res) => res.error(''))
      client.on('connection', () => {
        client.request('Test')
          .promise()
          .then(() => done(new Error('promise should not be resolved')))
          .catch((e) => {
            should(e).be.an.instanceof(Error)
            should(e.message).equal('request failed')
            minister.stop()
          })
      })
      minister.on('start', () => { client.start(); worker.start() })
      minister.on('stop', () => { client.stop(); worker.stop(); done() })
      minister.start()
    })
  })
  describe('If `err` is an object', () => {
    it('the client error.message === (err.message || `request failed`)', (done) => {
      let minister = M.Minister()
      let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
      let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

      let c = 0
      worker.on('request', (req, res) => {
        c++
        let e = c === 1 ? {message: 'error world!'} : {}
        res.error(e)
      })
      client.on('connection', () => {
        // First request
        client.request('Test')
          .promise()
          .then(() => done(new Error('promise should not be resolved')))
          .catch((e) => {
            should(e).be.an.instanceof(Error)
            should(e.message).equal('error world!')

            client.request('Test')
              .promise()
              .then(() => done(new Error('promise should not be resolved')))
              .catch((e) => {
                should(e).be.an.instanceof(Error)
                should(e.message).equal('request failed')
                minister.stop()
              })
          })
      })
      minister.on('start', () => { client.start(); worker.start() })
      minister.on('stop', () => { client.stop(); worker.stop(); done() })
      minister.start()
    })
    it('the `err` properties survived to JSON.stringify() are copied to the client error object', (done) => {
      let minister = M.Minister()
      let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
      let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

      worker.on('request', (req, res) => res.error({
        a: true,
        b: 2,
        c: function () {},
        d: 'hello',
        e: {
          ea: true
        }
      }))
      client.on('connection', () => {
        client.request('Test')
          .promise()
          .then(() => done(new Error('promise should not be resolved')))
          .catch((e) => {
            should(e).be.an.instanceof(Error)
            should(e.message).equal('request failed')
            should(e).containEql({
              a: true,
              b: 2,
              d: 'hello',
              e: {
                ea: true
              }
            })
            minister.stop()
          })
      })
      minister.on('start', () => { client.start(); worker.start() })
      minister.on('stop', () => { client.stop(); worker.stop(); done() })
      minister.start()
    })

    describe('which cannot be JSON.stringify(ed)', () => {
      it('the client error.message === `request failed`', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        sinon.stub(console, 'error')

        // Circular !!
        let a = {}
        let b = {}
        a.b = b
        b.a = a

        worker.on('request', (req, res) => res.error(a))
        client.on('connection', () => {
          client.request('Test')
            .promise()
            .then(() => done(new Error('promise should not be resolved')))
            .catch((e) => {
              should(e).be.an.instanceof(Error)
              should(e.message).equal('request failed')
              console.error.restore()
              minister.stop()
            })
        })
        minister.on('start', () => { client.start(); worker.start() })
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.start()
      })
      it('the JSON.stringify(err) error is printed to console', (done) => {
        let minister = M.Minister()
        let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
        let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

        sinon.stub(console, 'error')

        // Circular !!
        let a = {}
        let b = {}
        a.b = b
        b.a = a

        worker.on('request', (req, res) => res.error(a))
        client.on('connection', () => {
          client.request('Test')
            .promise()
            .then(() => done(new Error('promise should not be resolved')))
            .catch((e) => {
              should(e).be.an.instanceof(Error)
              should(e.message).equal('request failed')
              should(console.error.calledWithMatch((e) => e instanceof TypeError)).be.True()
              console.error.restore()
              minister.stop()
            })
        })
        minister.on('start', () => { client.start(); worker.start() })
        minister.on('stop', () => { client.stop(); worker.stop(); done() })
        minister.start()
      })
    })
  })
})
