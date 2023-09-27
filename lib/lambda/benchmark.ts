import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { Handler } from "aws-lambda";
import { LoadTestDriver, Test } from "../../load-test/driver.js";
import { AccountSelectionStrategy, buildRandomTransactions } from "../benchmarks.js";
import { createTransfersBatch } from "../transactions.js";
import { inspect } from "util";

inspect.defaultOptions.depth = 5;

const TABLE_NAME = process.env["TABLE_NAME"] ?? "transactions";
const NUMBER_OF_ACCOUNTS = Number.parseInt(process.env["NUMBER_OF_ACCOUNTS"] ?? `${1_000_000}`);
const BATCH_SIZE = Number.parseInt(process.env["BATCH_SIZE"] ?? "33");

const dynamoDbClient = new dynamodb.DynamoDBClient();
const documentClient = ddc.DynamoDBDocumentClient.from(dynamoDbClient, {
  marshallOptions: { removeUndefinedValues: true },
});

const test: Test = {
  async setup() {},

  async teardown() {},

  async request() {
    const txns = buildRandomTransactions(BATCH_SIZE, AccountSelectionStrategy.RANDOM_PEER_TO_PEER, {
      numAccounts: NUMBER_OF_ACCOUNTS,
    });

    try {
      await createTransfersBatch(documentClient, TABLE_NAME, txns);
    } catch (err) {
      console.log({ message: "Transaction batch failed", batch: { _0: txns[0], xs: "..." }, error: err });
      throw err;
    }
  },
};

export const handler: Handler = async (event, context) => {
  const concurrency = event.concurrency ?? 4;
  const arrivalRate = event.arrivalRate ?? 1000;
  const durationSeconds = event.durationSeconds ?? 60;

  console.log({ message: `Running load test with ${{ concurrency, arrivalRate, duration: durationSeconds }}...` });
  const loadTest = new LoadTestDriver(test, {
    concurrency,
    arrivalRate,
    durationSeconds,
    transactionsPerRequest: BATCH_SIZE,
  });
  await loadTest.run();
  console.log({ message: "Done." });
};
