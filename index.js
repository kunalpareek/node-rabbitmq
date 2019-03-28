'use strict'

const amqplib = require('amqplib')

module.exports = class RabbitMQService {
  constructor () {
    this.__conn = {}
    this.__channel = {}
  }

  /**
   * Set a common configuration for the rabbitmq wrapper.
   * @param {*} config
   */
  init (config) {
    let connectionString = 'amqp://' + config.username + ':' + config.password + '@' + config.host
    return amqplib.connect(connectionString)
      .then((connection) => {
        this.__conn = connection
        return this.__conn.createChannel()
      })
      .then((ch) => { this.__channel = ch })
      .catch(err => { throw err })
  }

  /**
   * Send data to queue
   * @param {String} q
   * @param {*} data
   */
  sendToQueue (q, data) {
    return this.__channel.assertQueue(q)
      .then((_) => {
        log(q, null, JSON.stringify(data))
        return this.__channel.sendToQueue(q, Buffer.from(JSON.stringify(data)))
      })
      .catch(err => {
        log(q, null, JSON.stringify(data), null, err)
        throw err
      })
  }

  /**
   * Send data to delayed queue
   * @param {String} q
   * @param {String} exchange
   * @param {*} data
   * @param {Number} offset
   */
  sendToDelayedQueue (q, exchange, data, offset) {
    let params = { persistent: true, headers: { 'x-delay': offset } }
    return this.__channel.assertQueue(q)
      .then(() => this.__channel.assertExchange(exchange, 'x-delayed-message', { arguments: { 'x-delayed-type': 'direct' } }))
      .then(() => this.__channel.bindQueue(q, exchange))
      .then(() => {
        log(q, exchange, JSON.stringify(data), offset)
        return this.__channel.publish(exchange, '', Buffer.from(JSON.stringify(data)), params)
      })
      .catch(err => {
        log(q, exchange, JSON.stringify(data), offset, err)
        throw err
      })
  }

  /**
   * Sends a message to fanout exchange
   * @param {String} q
   * @param {String} exchange
   * @param {*} data
   */
  sendToFanoutExchange (q, exchange, data) {
    return this.__channel.assertQueue(q)
      .then((_) => this.__channel.assertExchange(exchange, 'fanout', { durable: false }))
      .then((_) => this.__channel.bindQueue(q, exchange))
      .then((_) => {
        log(q, exchange, JSON.stringify(data))
        return this.__channel.publish(exchange, '', Buffer.from(JSON.stringify(data)))
      })
      .catch(err => {
        log(q, exchange, JSON.stringify(data), null, err)
        throw err
      })
  }

  /**
   * Creates a fanout exchange consumer
   * @param {String} q
   * @param {String} exchange
   * @param {Boolean} acknowledgement
   * @param {function} fn
   */
  createFanoutConsumer (exchange, acknowledgement, fn) {
    return this.__channel.assertExchange(exchange, 'fanout', { durable: false })
      .then(() => this.__channel.assertQueue('', { exclusive: true }))
      .then((queue) => {
        return this.__channel.bindQueue(queue.queue, exchange, '')
          .then(() => {
            this.__channel.consume(queue.queue, (msg) => {
              if (msg.content) {
                log(queue.queue, exchange, msg.content.toString())
                fn(msg.content.toString())
              }
            }, { noAck: acknowledgement })
          })
      })
      .catch(err => {
        log(null, exchange, null, null, err)
        throw err
      })
  }

  /**
   * Create a queue consumer
   * @param {String} q
   * @param {String} acknowledgement
   * @param {function} fn
   */
  createConsumer (q, acknowledgement, fn) {
    return this.__channel.assertQueue(q)
      .then((_) => {
        this.__channel.consume(q, (msg) => {
          if (msg.content) {
            log(q, null, msg.content.toString())
            fn(msg.content.toString())
          }
        }, { noAck: acknowledgement })
      })
      .catch(err => {
        log(q, null, null, null, err)
        throw err
      })
  }
}

function log (q, exchange, data, delay, err) {
  let level = (err === undefined) ? 'info' : 'error'
  let payload = {
    timestamp: new Date(),
    queue: q,
    exchange: exchange,
    body: data
  }
  if (delay) {
    payload.delay = delay
  }
  if (err) {
    payload.error = err
  }
  console.log(JSON.parse(JSON.stringify(payload)))
}
