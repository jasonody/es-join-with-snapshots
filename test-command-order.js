'use strict'

process.env.STREAM_NAME = 'dev-cndp-cqrs-es-join-with-snapshots-stream'

const aws = require('aws-sdk')

const handler = require('./handler')

aws.config.update({
  region: 'us-east-1'
})

const callback = (err, data) => err ? console.log('Returned Error: %j', err) : console.log('Returned Data: %j', data)

handler.commandOrder(null, {}, callback)