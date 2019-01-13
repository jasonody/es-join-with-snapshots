# Extending the Event Source Join pattern with Snapshots

This example includes:
- a Lambda function to write events to a Kinesis stream to seed the example: `command`
- a Lambda function to consume events from the stream and write to a DynamoDB table (events): `consumer`
- a Lambda function that's triggered by the dynamodb CDC stream and writes to another dynamodb table (view), creating a materialized view: `trigger`
- a Lambda function that creates a snapshot of the events store, removing the need to iterate over all the events everytime a change occurs in order to build a most recent version of the materialzed view: `snapshot`

Test scripts:
- write a login event to the Kinesis stream: `test-command-login.js`
- write an order event to the Kinesis stream: `test-command-order.js`
- update the snapshot: `test-snapshot.js`

To run a test script, execute: `node {test script filename}`

The safest way to determine what events to include in a snapshot would be those that are older than the retention period of the stream that the event store is being populated from. This would help to ensure that no events will arrive that occurred before the snapshot.

> This example builds on the CQRS Event Sources Join pattern presented in [Cloud Native Development Patterns and Best Practices](https://github.com/PacktPublishing/Cloud-Native-Development-Patterns-and-Best-Practices/tree/master/Chapter04/cqrs-es-join)

## Steps
1. Execute: `npm install`
2. Execute: `npm run dp:dev:e`
3. In the AWS console review the various tabs for the following:
   * Cloudformation Stack: `cndp-cqrs-es-join-dev`
   * DynamoDB Tables: `dev-cndp-cqrs-es-join-events` and `dev-cndp-cqrs-es-join-view`
   * Kinesis Stream: `dev-us-east-1-cndp-cqrs-es-join-stream`
   * Lambda functions: `cndp-cqrs-es-join-dev-command`, `cndp-cqrs-es-join-dev-consumer`, `cndp-cqrs-es-join-dev-trigger`, and `cndp-cqrs-es-join-dev-snapshot`
4. Invoke Lambda function `cndp-cqrs-es-join-dev-command` from the AWS console by pressing the Test button (or execute: `sls invoke -f command`)
   * Accept the defaults if asked.
5. Inspect the DynamoDB tables for the new contents
6. Inspect the Lambda Monitoring tab and logs for each function
7. Execute: `npm run rm:dev:e`