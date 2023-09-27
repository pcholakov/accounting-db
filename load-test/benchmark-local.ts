import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import PQueue from "p-queue";
import { AccountSelectionStrategy, buildRandomTransactions, setupAccounts } from "../lib/benchmarks.js";
import { createTransfersBatch } from "../lib/transactions.js";
import { LoadTestDriver, Test } from "./driver.js";

const TABLE_NAME = process.env["TABLE_NAME"] ?? "transactions";
const ACCOUNT_COUNT = 10_000;
const TRANSFERS_PER_BATCH = 10;

const dynamoDbClient = new dynamodb.DynamoDBClient({
  region: "localhost",
  endpoint: "http://localhost:8000",
  credentials: {
    accessKeyId: "a",
    secretAccessKey: "k",
  },
});
const documentClient = ddc.DynamoDBDocumentClient.from(dynamoDbClient, {
  marshallOptions: { removeUndefinedValues: true },
});

let count = 0;

const test: Test = {
  async setup() {
    try {
      await dynamoDbClient.send(new dynamodb.DeleteTableCommand({ TableName: TABLE_NAME }));
    } catch (err) {
      if (!(err instanceof dynamodb.ResourceNotFoundException)) {
        throw err;
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

    await setupAccounts(documentClient, TABLE_NAME, new PQueue({ concurrency: 10 }), ACCOUNT_COUNT);
  },

  async teardown() {
    process.stdout.write("\n");
    await dynamoDbClient.send(new dynamodb.DeleteTableCommand({ TableName: TABLE_NAME }));
  },

  async request() {
    const txns = buildRandomTransactions(TRANSFERS_PER_BATCH, AccountSelectionStrategy.RANDOM_PEER_TO_PEER, {
      numAccounts: ACCOUNT_COUNT,
    });

    try {
      await createTransfersBatch(documentClient, TABLE_NAME, txns);
      if (count++ % 1_000 === 0) {
        process.stdout.write(".");
      }
    } catch (err) {
      console.log({ message: "Transaction batch failed", batch: { _0: txns[0], xs: "..." }, error: err });
      throw err;
    }
  },
};

const concurrency = 4;
const arrivalRate = 1000; // requests per second
const durationSeconds = 20; // seconds

const loadTest = new LoadTestDriver(test, {
  concurrency,
  arrivalRate,
  durationSeconds,
  transactionsPerRequest: TRANSFERS_PER_BATCH,
});
await loadTest.run();
