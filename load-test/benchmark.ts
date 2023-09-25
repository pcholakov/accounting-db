import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { randomInt } from "crypto";
import { monotonicFactory } from "ulid";
import { Transfer, createTransfer } from "../lib/transactions.js";
import { LoadTestDriver, Test } from "../load-test/driver.js";
import { setupAccounts } from "../lib/benchmarks.js";
import PQueue from "p-queue";

const TABLE_NAME = process.env["TABLE_NAME"] ?? "transactions";
const ACCOUNT_COUNT = 10_000;

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

const ulid = monotonicFactory();

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
    const timestamp = Date.now();

    const fromAccount = 1; // randomInt(1, ACCOUNT_COUNT);
    // let toAccount;
    // do {
    //   toAccount = randomInt(1, ACCOUNT_COUNT);
    // } while (fromAccount === toAccount);
    const toAccount = randomInt(2, ACCOUNT_COUNT);

    const tx: Transfer = {
      id: ulid(),
      ledger: 700,
      amount: 1 + randomInt(0, 1_000_000),
      debit_account_id: fromAccount,
      credit_account_id: toAccount,
      code: 0,
      flags: 0,
      pending_id: undefined,
      timeout: 0,
      timestamp,
    };

    try {
      await createTransfer(documentClient, TABLE_NAME, tx);
    } catch (err) {
      console.log({ message: "Transaction failed", tx, error: err });
    }

    if (count++ % 1_000 === 0) {
      process.stdout.write(".");
    }
  },
};

const concurrency = 4;
const arrivalRate = 1000; // requests per second
const duration = 20; // seconds

const loadTest = new LoadTestDriver(concurrency, arrivalRate, duration, test);
await loadTest.run();
