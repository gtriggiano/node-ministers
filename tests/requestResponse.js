import fs from 'fs'
import should from 'should/as-function'
import uuid from 'uuid'

import M from '../src'

describe('REQUEST RESPONSE FLOW', () => {
  it('minister act as an async broker between a client and a worker', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => {
      should(req.body.param).equal('param')
      should(req.body.bool).be.True()
      should(req.options.someOption).equal('option')
      res.send('hello')
    })
    client.on('connection', () => {
      client.request('Test', {param: 'param', bool: true}, {someOption: 'option'})
        .on('data', buffer => should(buffer.toString()).equal('hello'))
        .on('end', () => minister.stop())
    })
    minister.on('start', () => {
      client.start()
      setTimeout(() => worker.start(), 100)
    })
    minister.on('stop', () => {
      client.stop()
      worker.stop()
      done()
    })
    minister.start()
  })
  it('minister can delegate requests to other ministers', (done) => {
    let minister1 = M.Minister()
    let minister2 = M.Minister({port: 5557, ministers: ['tcp://127.0.0.1:5555']})
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5557'})

    worker.on('request', (req, res) => {
      should(req.body.param).equal('param')
      should(req.body.bool).be.True()
      should(req.options.someOption).equal('option')
      res.send('hello')
    })
    client.on('connection', () => {
      client.request('Test', {param: 'param', bool: true}, {someOption: 'option'})
        .on('data', buffer => should(buffer.toString()).equal('hello'))
        .on('end', () => minister1.stop())
    })
    minister1.on('start', () => minister2.start())
    minister2.on('start', () => {
      client.start()
      worker.start()
    })
    minister1.on('stop', () => {
      minister2.stop()
      client.stop()
      worker.stop()
    })
    minister2.on('stop', () => done())
    minister1.start()
  })
  it('client requests behave like readable streams', (done) => {
    let filePath = `${__dirname}/${uuid.v4()}`
    let content = uuid.v4()
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => res.send(content))
    client.on('connection', () => {
      let fileStream = fs.createWriteStream(filePath)
      let request = client.request('Test')
      request.pipe(fileStream)
      request.on('end', () => {
        fileStream.end()
        should(fs.readFileSync(filePath, {encoding: 'utf8'})).equal(content)
        fs.unlinkSync(filePath)
        minister.stop()
      })
    })
    minister.on('start', () => {
      client.start()
      worker.start()
    })
    minister.on('stop', () => {
      client.stop()
      worker.stop()
      done()
    })
    minister.start()
  })
  it('worker responses behave like writable streams', (done) => {
    let filePath = `${__filename}`
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => fs.createReadStream(filePath).pipe(res))
    client.on('connection', () => {
      let data = ''
      let request = client.request('Test')
      request.on('data', buffer => data += buffer.toString())
      request.on('end', () => {
        should(fs.readFileSync(filePath, {encoding: 'utf8'})).equal(data)
        minister.stop()
      })
    })
    minister.on('start', () => {
      client.start()
      worker.start()
    })
    minister.on('stop', () => {
      client.stop()
      worker.stop()
      done()
    })
    minister.start()
  })
  it('requests can use a partialCallback and finalCallback if passed through request options', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => {
      res.write('hello')
      res.send('world')
    })
    client.on('connection', () => {
      client.request('Test', {}, {
        partialCallback: buffer => should(buffer.toString()).equal('hello'),
        finalCallback: (e, buffer) => {
          should(buffer.toString()).equal('world')
          minister.stop()
        }
      })
    })
    minister.on('start', () => {
      client.start()
      worker.start()
    })
    minister.on('stop', () => {
      client.stop()
      worker.stop()
      done()
    })
    minister.start()
  })
  it('requests can be used as promises calling request.promise()', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    worker.on('request', (req, res) => res.send('hello world'))
    client.on('connection', () => {
      let request = client.request('Test')
      let promise = request.promise()
      let re_promise = request.promise()
      should(promise === re_promise).be.True()
      promise.then(buffer => {
        should(buffer.toString()).equal('hello world')
        minister.stop()
      })
    })
    minister.on('start', () => {
      client.start()
      worker.start()
    })
    minister.on('stop', () => {
      client.stop()
      worker.stop()
      done()
    })
    minister.start()
  })
  it('requests can have a user defined timeout', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    client.on('connection', () => {
      client
        .request('Test', {}, {timeout: 500})
        .promise()
        .catch((e) => {
          should(e.message).equal(M.RESPONSE_TIMEOUT)
          minister.stop()
        })
    })
    minister.on('start', () => {
      client.start()
      worker.start()
    })
    minister.on('stop', () => {
      client.stop()
      worker.stop()
      done()
    })
    minister.start()
  })
  it('clean and idempotent requests will be retried if minister disconnects', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    let response
    worker.on('request', (req, res) => res.send('hello'))
    client.once('connection', () => {
      client
        .request('Test', {}, {idempotent: true})
        .promise()
        .then(buffer => {
          response = buffer.toString()
          minister.stop()
        })
        .catch(e => done(e))

      setTimeout(() => minister.stop(), 100)
    })
    minister.on('start', () => {
      client.start()
    })
    minister.once('stop', () => {
      minister.on('start', () => worker.start())
      minister.on('stop', () => {
        should(response).equal('hello')
        client.stop()
        worker.stop()
        done()
      })
      setTimeout(() => minister.start(), 10)
    })
    minister.start()
  })
  it('clean and idempotent requests will be retried if worker disconnects', (done) => {
    let minister = M.Minister()
    let client = M.Client({endpoint: 'tcp://127.0.0.1:5555'})
    let worker = M.Worker({service: 'Test', endpoint: 'tcp://127.0.0.1:5555'})

    let c = 0
    let response
    worker.on('request', (req, res) => {
      c++
      setTimeout(() => res.send(`hello ${c}`), 100)
    })
    minister.on('start', () => client.start())
    client.once('connection', () => worker.start())
    worker.once('connection', () => {
      client
        .request('Test', {}, {idempotent: true})
        .promise()
        .then(buffer => {
          response = buffer.toString()
          minister.stop()
        })
        .catch(e => done(e))

      setTimeout(() => worker.stop(), 50)
    })
    worker.once('stop', () => setTimeout(() => worker.start(), 50))
    minister.on('stop', () => {
      should(response).equal('hello 2')
      client.stop()
      worker.stop()
      done()
    })
    minister.start()
  })
  it('dirty and idempotent requests will be retried if options.reconnectStream = true')
})
