import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { Handler } from "aws-lambda";
import { randomInt } from "crypto";
import { monotonicFactory } from "ulid";
import { LoadTestDriver, Test } from "../../load-test/driver.js";
import { Transfer, createTransfer } from "../transactions.js";

const TABLE_NAME = process.env["TABLE_NAME"] ?? "transactions";
const ACCOUNT_COUNT = 10_000;

const dynamoDbClient = new dynamodb.DynamoDBClient();
const documentClient = ddc.DynamoDBDocumentClient.from(dynamoDbClient, {
  marshallOptions: { removeUndefinedValues: true },
});

const ulid = monotonicFactory();

let count = 0;

const test: Test = {
  async setup() {},

  async teardown() {},

  async request() {
    const timestamp = Date.now();

    // Random peer-to-peer
    const fromAccount = randomInt(1, ACCOUNT_COUNT);
    let toAccount;
    do {
      toAccount = randomInt(1, ACCOUNT_COUNT);
    } while (fromAccount === toAccount);

    // Single hot account to many peers
    // const fromAccount = 1;
    // const toAccount = randomInt(2, ACCOUNT_COUNT);

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
      // console.log({ message: "Transaction failed", tx, error: err });
    }
  },
};

export const handler: Handler = async (event, context) => {
  const concurrency = event.concurrency ?? 4;
  const arrivalRate = event.arrivalRate ?? 1000; // requests per second
  const duration = event.duration ?? 60; // seconds

  console.log({ message: `Running load test with ${{ concurrency, arrivalRate, duration }}...` });
  const loadTest = new LoadTestDriver(concurrency, arrivalRate, duration, test);
  await loadTest.run();
  console.log({ message: "Done." });
};
