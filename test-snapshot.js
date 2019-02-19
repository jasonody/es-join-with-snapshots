'use strict'

process.env.EVENTS_TABLE_NAME = 'dev-cndp-cqrs-es-join-with-snapshots-events'
process.env.VIEW_TABLE_NAME = 'dev-cndp-cqrs-es-join-with-snapshots-view'

const aws = require('aws-sdk')

const handler = require('./handler')

aws.config.update({
  region: 'us-east-1'
})

const callback = (err, data) => err ? console.log('Returned Error: %j', err) : console.log('DONE')//console.log('Returned Data: %j', data)

const event = {
  "Records": [
      {
          "eventID": "2ca2efcaf4637e13197d854fcae75df6",
          "eventName": "MODIFY",
          "eventVersion": "1.1",
          "eventSource": "aws:dynamodb",
          "awsRegion": "us-east-1",
          "dynamodb": {
              "ApproximateCreationDateTime": 1546741048,
              "Keys": {
                  "id": {
                      "S": "0d2e140f-08b4-4064-bfc1-91feba015f4c"
                  }
              },
              "SequenceNumber": "67000000000049711534276",
              "SizeBytes": 38,
              "StreamViewType": "KEYS_ONLY"
          },
          "eventSourceARN": "arn:aws:dynamodb:us-east-1:780510034593:table/dev-cndp-cqrs-es-join-view/stream/2019-01-06T02:04:36.915"
      }
  ]
}

handler.snapshot(event, {}, callback)