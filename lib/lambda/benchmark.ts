import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { Handler } from "aws-lambda";
import { inspect } from "util";
import { AccountSelectionStrategy } from "../generators.js";
import { LoadTestDriver } from "../load-test-runner.js";
import { CreateTransfers } from "../load-tests.js";

inspect.defaultOptions.depth = 5;

const TABLE_NAME = process.env["TABLE_NAME"] ?? "transactions";
const NUMBER_OF_ACCOUNTS = Number.parseInt(process.env["NUMBER_OF_ACCOUNTS"] ?? `${1_000_000}`);
const BATCH_SIZE = Number.parseInt(process.env["BATCH_SIZE"] ?? "33");

const dynamoDbClient = new dynamodb.DynamoDBClient();
const documentClient = ddc.DynamoDBDocumentClient.from(dynamoDbClient, {
  marshallOptions: { removeUndefinedValues: true },
});

export const handler: Handler = async (event, context) => {
  const concurrency = event.concurrency ?? 4;
  const arrivalRate = event.arrivalRate ?? 1000;
  const durationSeconds = event.durationSeconds ?? 60;
  const batchSize = event.batchSize ?? BATCH_SIZE;
  const numAccounts = event.numAccounts ?? NUMBER_OF_ACCOUNTS;
  const accountSelectionStrategy = event.accountSelectionStrategy ?? AccountSelectionStrategy.RANDOM_PEER_TO_PEER;

  console.log({ message: `Running load test with ${{ concurrency, arrivalRate, duration: durationSeconds }}...` });
  const loadTest = new LoadTestDriver(
    new CreateTransfers({
      documentClient,
      tableName: TABLE_NAME,
      batchSize,
      numAccounts,
      accountSelectionStrategy,
    }),
    {
      concurrency,
      targetRequestRatePerSecond: arrivalRate,
      durationSeconds,
    },
  );
  const result = await loadTest.run();
  console.log({ message: "Done." });
  console.log({ result });
  return result;
};
