'use strict';

const aws = require('aws-sdk');
const _ = require('highland');
const uuid = require('uuid');

aws.config.update({
  logger: { log: msg => console.log(msg) },
});

module.exports.command = (evt, context, callback) => {
  const userId = '0d2e140f-08b4-4064-bfc1-91feba015f4c'//uuid.v4();
  const userId2 = '13989ddd-770c-4051-9ba6-4b2a83325ded'//uuid.v4();

  const params = {
    StreamName: process.env.STREAM_NAME,
    Records: [
      {
        PartitionKey: userId,
        Data: Buffer.from(JSON.stringify({
          id: uuid.v1(),
          type: 'user-created',
          timestamp: Date.now(),
          user: {
            id: userId,
            name: 'Fred User'
          }
        })),
      },
      {
        PartitionKey: userId,
        Data: Buffer.from(JSON.stringify({
          id: uuid.v1(),
          type: 'user-loggedIn',
          timestamp: Date.now(),
          user: {
            id: userId
          }
        })),
      },
      {
        PartitionKey: userId,
        Data: Buffer.from(JSON.stringify({
          id: uuid.v1(),
          type: 'order-submitted',
          timestamp: Date.now(),
          order: {
            userId: userId
          }
        })),
      },
      {
        PartitionKey: userId2,
        Data: Buffer.from(JSON.stringify({
          id: uuid.v1(),
          type: 'user-loggedIn',
          timestamp: Date.now(),
          user: {
            id: userId2
          }
        })),
      },
    ]
  };

  console.log('params: %j', params);

  const kinesis = new aws.Kinesis();

  kinesis.putRecords(params).promise()
    .then(resp => callback(null, resp))
    .catch(err => callback(err));
};

module.exports.commandLogin = (evt, context, callback) => {
  const userId = '0d2e140f-08b4-4064-bfc1-91feba015f4c'

  const params = {
    StreamName: process.env.STREAM_NAME,
    Records: [
      {
        PartitionKey: userId,
        Data: Buffer.from(JSON.stringify({
          id: uuid.v1(),
          type: 'user-loggedIn',
          timestamp: Date.now(),
          user: {
            id: userId
          }
        })),
      },
    ]
  };

  console.log('login params: %j', params);

  const kinesis = new aws.Kinesis();

  kinesis.putRecords(params).promise()
    .then(resp => callback(null, resp))
    .catch(err => callback(err));
};

module.exports.commandOrder = (evt, context, callback) => {
  const userId = '0d2e140f-08b4-4064-bfc1-91feba015f4c'

  const params = {
    StreamName: process.env.STREAM_NAME,
    Records: [
      {
        PartitionKey: userId,
        Data: Buffer.from(JSON.stringify({
          id: uuid.v1(),
          type: 'order-submitted',
          timestamp: Date.now(),
          order: {
            userId: userId
          }
        })),
      },
    ]
  };

  console.log('login params: %j', params);

  const kinesis = new aws.Kinesis();

  kinesis.putRecords(params).promise()
    .then(resp => callback(null, resp))
    .catch(err => callback(err));
};

module.exports.consumer = (event, context, cb) => {
  console.log('event: %j', event);

  _(event.Records)
    .map(recordToUow)
    .tap(print)
    .filter(byType)
    .flatMap(saveEvent)
    .tap(print)
    .collect().toCallback(cb);
};

const print = uow => console.log('uow: %j', uow);

const recordToUow = r => ({
  record: r,
  event: JSON.parse(new Buffer(r.kinesis.data, 'base64'))
});

const byType = uow =>
  uow.event.type === 'user-created' ||
  uow.event.type === 'user-loggedIn' ||
  uow.event.type === 'order-submitted';

const saveEvent = uow => {
  const params = {
    TableName: process.env.EVENTS_TABLE_NAME,
    Item: {
      id: uow.event.user ? uow.event.user.id : uow.event.order.userId,
      sequence: uow.record.kinesis.sequenceNumber,
      event: uow.event,
    }
  };

  console.log('params: %j', params);

  const db = new aws.DynamoDB.DocumentClient();

  return _(db.put(params).promise()
    .then(() => uow)
  );
}

module.exports.trigger = (event, context, cb) => {
  console.log('event: %j', event);

  _(event.Records)
    .tap(r => console.log('record: %j', r))
    .flatMap(getRelatedEvents)
    .map(view)
    .tap(uow => console.log('%j', uow))
    .flatMap(saveView)
    .collect().toCallback(cb);
};

const getRelatedEvents = (record) => {
  const params = {
    TableName: process.env.EVENTS_TABLE_NAME,
    KeyConditionExpression: '#id = :id',
    ExpressionAttributeNames: {
      '#id': 'id'
    },
    ExpressionAttributeValues: {
      ':id': record.dynamodb.Keys.id.S
    }
  };

  const db = new aws.DynamoDB.DocumentClient();

  return _(db.query(params).promise()
    .then(data => ({
      record: record,
      data: data,
    }))
  );
}

const view = (uow) => {
  // create a dictionary by event type
  uow.dictionary = uow.data.Items.reduce((dictionary, item) => {
    // events are sorted by range key
    item.event.type === 'order-submitted' ?
      dictionary[item.event.type].push(item.event) :
      dictionary[item.event.type] = item.event;

    return dictionary;
  }, { // default values
      'user-created': { user: { name: undefined } },
      'user-loggedIn': { timestamp: undefined },
      'order-submitted': []
    });

  // map the fields
  uow.item = {
    id: uow.record.dynamodb.Keys.id.S,
    name: uow.dictionary['user-created'].user.name,
    lastLogin: uow.dictionary['user-loggedIn'].timestamp,
    recentOrderCount: uow.dictionary['order-submitted'].length,
  };

  return uow;
}

const saveView = (uow) => {
  const params = {
    TableName: process.env.VIEW_TABLE_NAME,
    Item: uow.item,
  };

  console.log('params: %j', params);

  const db = new aws.DynamoDB.DocumentClient();
  return _(db.put(params).promise());
};

module.exports.snapshot = (event, context, cb) => {
  //id: event.[Records].Keys.id.S
  _(event.Records)
    .flatMap(getRelatedEvents)
    .map(toTrimmedEvents)
    //get snapshot
    .map(view)
    //.flatMap(lockSnapshot)
    //delete older events
    //update and unlock snapshot
    .tap(uow => console.log('snapshot $LATEST$: %j', uow))
    .collect()
    .toCallback(cb)

  //get all events for updated materialized view
  //filter--are there any that are older than the live period?
  //get previous snapshot
  //build new snapshot
  //lock snapshot
  //update snapshot
  //delete older events from events store
  //unlock snapshot
}

const toTrimmedEvents = (uow) => {
  const horizon = Date.now() - (1000 * 60) //one minute ago
  const oldEvents = uow.data.Items.filter(item => item.event.timestamp < cutoff)
  
  uow.data.Items = oldEvents

  return uow
}

const lockSnapshot = (uow) => {
  if (uow.data.Items.length > 0) {

  }
  else
    return _(Promise.resolve(uow))
}