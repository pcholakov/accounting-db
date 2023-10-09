import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { Handler } from "aws-lambda";
import { inspect } from "util";
import { AccountSelectionStrategy } from "../generators.js";
import { LoadTestDriver } from "../load-test-runner.js";
import { CreateTransfersLoadTest, ReadAccountBalancesLoadTest } from "../load-tests.js";

inspect.defaultOptions.depth = 5;

const TABLE_NAME = process.env["TABLE_NAME"] ?? "transactions";
const NUMBER_OF_ACCOUNTS = Number.parseInt(process.env["NUMBER_OF_ACCOUNTS"] ?? `${1_000_000}`);
const BATCH_SIZE = Number.parseInt(process.env["BATCH_SIZE"] ?? "33");

const dynamoDbClient = new dynamodb.DynamoDBClient();
const documentClient = ddc.DynamoDBDocumentClient.from(dynamoDbClient, {
  marshallOptions: { removeUndefinedValues: true },
});

export const handler: Handler = async (event, context) => {
  const writeRate = event.writeRate ?? 1000;
  const writeConcurrency = event.writeConcurrency ?? 4;
  const writeBatchSize = event.writeBatchSize ?? BATCH_SIZE;
  const readRate = event.readRate ?? 2000;
  const readConcurrency = event.readConcurrency ?? 4;
  const readBatchSize = event.readBatchSize ?? BATCH_SIZE;
  const durationSeconds = event.durationSeconds ?? 60;
  const numAccounts = event.numAccounts ?? NUMBER_OF_ACCOUNTS;
  const hotAccounts = event.hotAccounts ?? undefined;
  const accountSelectionStrategy =
    parseAccountSelection(event.accountSelectionStrategy) ?? AccountSelectionStrategy.RANDOM_PEER_TO_PEER;

  console.log({
    message: `Starting load tests with configuration: ${{
      concurrency: writeConcurrency,
      arrivalRate: writeRate,
      duration: durationSeconds,
    }}.`,
  });

  const writeDriver = new LoadTestDriver(
    new CreateTransfersLoadTest({
      documentClient,
      tableName: TABLE_NAME,
      batchSize: writeBatchSize,
      numAccounts,
      hotAccounts,
      accountSelectionStrategy,
    }),
    {
      concurrency: writeConcurrency,
      targetRequestRatePerSecond: writeRate,
      durationSeconds,
    },
  );

  const readDriver = new LoadTestDriver(
    new ReadAccountBalancesLoadTest({
      documentClient,
      tableName: TABLE_NAME,
      numAccounts,
      batchSize: readBatchSize,
    }),
    {
      targetRequestRatePerSecond: readRate,
      concurrency: readConcurrency,
      durationSeconds,
    },
  );

  const startTime = Date.now();
  const [write, read] = await Promise.allSettled([writeDriver.run(), readDriver.run()]);
  const result = {
    startTime: new Date(startTime).toISOString(),
    requestId: context.awsRequestId,
    write: write.status === "fulfilled" ? write.value : write,
    read: read.status === "fulfilled" ? read.value : read,
  };
  console.log(result);
  return result;
};

function parseAccountSelection(input: string): AccountSelectionStrategy | undefined {
  for (const enumKey in AccountSelectionStrategy) {
    if (enumKey === input) {
      return enumKey as AccountSelectionStrategy;
    }
  }
  return undefined;
}
