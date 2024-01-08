import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { inspect } from "util";
import { AccountSelectionStrategy } from "../lib/generators.js";
import { LoadTestDriver } from "../lib/load-test-runner.js";
import { CreateTransfersLoadTest, ReadAccountBalancesLoadTest } from "../lib/load-tests.js";

// Load test parameters

const testDurationSeconds = 10;
const numAccounts = 1_000;
const hotAccounts = 1_000;

const readRate = 500; // Set to 0 to disable
const readConcurrency = 5;
const readBatchSize = 5;

const writeRate = 500; // Set to 0 to disable
const writeConcurrency = 5;
const writeBatchSize = 5;
const writeAccountSelectionStrategy = AccountSelectionStrategy.RANDOM_PEER_TO_PEER;

const requestTimeoutMs = 100;
const dynamoDbClientTimeoutMs = 500;

// No more test configuration below this line

inspect.defaultOptions.depth = 5;

export const TABLE_NAME = process.env["TABLE_NAME"] ?? "transactions";

const dynamoDbClient = new dynamodb.DynamoDBClient({
  region: "localhost",
  endpoint: "http://localhost:8000",
  requestHandler: new NodeHttpHandler({
    connectionTimeout: dynamoDbClientTimeoutMs,
    requestTimeout: dynamoDbClientTimeoutMs,
  }),
  maxAttempts: 2,
  credentials: {
    accessKeyId: "a",
    secretAccessKey: "k",
  },
});
export const documentClient = ddc.DynamoDBDocumentClient.from(dynamoDbClient, {
  marshallOptions: { removeUndefinedValues: true },
});

async function createDatabaseTable(opts: { recreateIfExists: boolean }) {
  if (!opts.recreateIfExists) {
    try {
      await dynamoDbClient.send(new dynamodb.DescribeTableCommand({ TableName: TABLE_NAME }));
      console.log("Table already exists, not re-creating.");
      return;
    } catch (err) {
      if (!(err instanceof dynamodb.ResourceNotFoundException)) {
        throw err;
      }
      // ignore and continue
    }
  } else {
    try {
      await dynamoDbClient.send(new dynamodb.DeleteTableCommand({ TableName: TABLE_NAME }));
      console.log("Deleted existing table.");
    } catch (err) {
      if (!(err instanceof dynamodb.ResourceNotFoundException)) {
        throw err;
      }
    }
  }

  await dynamoDbClient.send(
    new dynamodb.CreateTableCommand({
      TableName: TABLE_NAME,
      KeySchema: [
        { AttributeName: "pk", KeyType: "HASH" },
        { AttributeName: "sk", KeyType: "RANGE" },
      ],
      AttributeDefinitions: [
        { AttributeName: "pk", AttributeType: "S" },
        { AttributeName: "sk", AttributeType: "S" },
      ],
      BillingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
    }),
  );
  console.log("Created empty table.");
}

async function main() {
  await createDatabaseTable({ recreateIfExists: false });

  const writeDriver = new LoadTestDriver(
    new CreateTransfersLoadTest({
      documentClient,
      tableName: TABLE_NAME,
      numAccounts,
      hotAccounts,
      batchSize: writeBatchSize,
      accountSelectionStrategy: writeAccountSelectionStrategy,
      progressMarker: writeRate,
    }),
    {
      targetRequestRatePerSecond: writeRate,
      concurrency: writeConcurrency,
      durationSeconds: testDurationSeconds,
      timeoutValueMs: requestTimeoutMs,
      skipWarmup: false,
    },
  );

  const readDriver = new LoadTestDriver(
    new ReadAccountBalancesLoadTest({
      documentClient,
      tableName: TABLE_NAME,
      numAccounts,
      batchSize: readBatchSize,
      progressMarker: readRate,
    }),
    {
      targetRequestRatePerSecond: readRate,
      concurrency: readConcurrency,
      durationSeconds: testDurationSeconds,
      timeoutValueMs: requestTimeoutMs,
      skipWarmup: false,
    },
  );

  const startTime = Date.now();

  const [write, read] = await Promise.allSettled([writeDriver.run(), readDriver.run()]);
  process.stdout.write("\n");

  console.log({
    startTime: new Date(startTime).toISOString(),
    write: write.status === "fulfilled" ? write.value : write,
    read: read.status === "fulfilled" ? read.value : read,
  });
}

main();