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
    .flatMap(getView)
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

  const defaultValues = {
    'user-created': { user: { name: undefined } },
    'user-loggedIn': { timestamp: undefined },
    'order-submitted': []
  }
  const snapshot = uow.view && uow.view.snapshot || defaultValues

  uow.dictionary = uow.data.Items.reduce((dictionary, item) => {
    // events are sorted by range key
    item.event.type === 'order-submitted' ?
      dictionary[item.event.type].push(item.event) :
      dictionary[item.event.type] = item.event;

    return dictionary;
  }, snapshot);

  // map the fields
  uow.item = {
    id: uow.record.dynamodb.Keys.id.S,
    name: uow.dictionary['user-created'].user.name,
    lastLogin: uow.dictionary['user-loggedIn'].timestamp,
    recentOrderCount: uow.dictionary['order-submitted'].length,
    snapshot: uow.view && JSON.stringify(uow.view.snapshot) || undefined,
    lock: uow.view && uow.view.lock || undefined
  };

  return uow;
}

const saveView = (uow) => {
  const params = {
    TableName: process.env.VIEW_TABLE_NAME,
    Item: uow.item,
  };

  console.log('save view params: %j', params);

  const db = new aws.DynamoDB.DocumentClient();
  return _(db.put(params).promise());
};

module.exports.snapshot = (event, context, cb) => {
  _(event.Records)
    .flatMap(getRelatedEvents)
    .map(toTrimmedEvents)
    .tap(isSnapshotUpToDate)
    .flatMap(getView)
    //lock view
    .map(viewSnapshot)
    .flatMap(deleteTrimmedEvents)
    .flatMap(saveView) //and unlock view
    .tap(uow => console.log('snapshot $LATEST$: %j', uow))
    .errors(handleErrors)
    .collect()
    .toCallback(cb)
}

const toTrimmedEvents = (uow) => {
  const horizon = Date.now() - (1000 * 60) //one minute ago
  const oldEvents = uow.data.Items.filter(item => item.event.timestamp < horizon)

  uow.data = { Items: oldEvents }

  return uow
}

const isSnapshotUpToDate = (uow) => {
  if (!uow.data.Items.length) {
    const noWork = new Error('Current snapshot is up to date.')
    noWork.code = 'NoFurtherWork'
    noWork.uow = uow

    throw noWork
  }
}

const getView = (uow) => {
  const params = {
    Key: { 'id': uow.record.dynamodb.Keys.id.S },
    TableName: process.env.VIEW_TABLE_NAME
  }

  const db = new aws.DynamoDB.DocumentClient();

  return _(db.get(params).promise()
    .then(view => {
      //TODO: if view is locked, throw transient error
      if (view.Item && view.Item.snapshot) {
        view.Item.snapshot = JSON.parse(view.Item.snapshot)
      }
      
      uow.view = view.Item

      return uow
    })
  );
}

const viewSnapshot = (uow) => {
  // create a dictionary by event type

  const defaultValues = {
    'user-created': { user: { name: undefined } },
    'user-loggedIn': { timestamp: undefined },
    'order-submitted': []
  }
  const snapshot = uow.view.snapshot || defaultValues

  uow.dictionary = uow.data.Items.reduce((dictionary, item) => {
    // events are sorted by range key
    item.event.type === 'order-submitted' ?
      dictionary[item.event.type].push(item.event) :
      dictionary[item.event.type] = item.event;

    return dictionary;
  }, snapshot);

  // map the fields
  uow.item = Object.assign(uow.view, {
    snapshot: JSON.stringify(uow.dictionary),
    lock: false
  })

  return uow;
}

const lockSnapshot = (uow) => {
  
}

const deleteTrimmedEvents = (uow) => {
  const deleteOps = uow.data.Items.map(event => {
    return {
      DeleteRequest: {
        Key: {
          id: event.id,
          sequence: event.sequence
        },
        Item: {}
      }
    }
  })
  
  const params = {
    RequestItems: {
      [process.env.EVENTS_TABLE_NAME]: deleteOps,
      //[process.env.VIEW_TABLE_NAME] : [{ PutRequest: { Item: uow.item } }]
    }
  }
  console.log('delete params: %j', params)
  var db = new aws.DynamoDB.DocumentClient();

  return _(db.batchWrite(params).promise()
    .then(data => {
      uow.deleteResult = data

      return uow
    }))
}

const handleErrors = (err, push) => {
  if (err.code === 'NoFurtherWork') {
    console.log('NO_WORK!')
    push(null, err)
  } else {
    console.log(err)
    push(err)
  }
}