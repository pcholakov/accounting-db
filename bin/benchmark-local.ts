import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { inspect } from "util";
import { AccountSelectionStrategy } from "../lib/generators.js";
import { LoadTestDriver } from "../lib/load-test-runner.js";
import { CreateTransfers, ReadBalances } from "../lib/load-tests.js";

inspect.defaultOptions.depth = 5;

export const TABLE_NAME = process.env["TABLE_NAME"] ?? "transactions";

const dynamoDbClient = new dynamodb.DynamoDBClient({
  region: "localhost",
  endpoint: "http://localhost:8000",
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

const durationPerTestCycleSeconds = 5;
const numAccounts = 10_000;
const readRateBase = 1_000;
const readRateIncrement = 1_000;
const writeRateBase = 1_00;
const writeRateIncrement = 1_00;
const testSteps = 1;

await createDatabaseTable({ recreateIfExists: false });

const results = [];
for (let i = 0; i < testSteps; i++) {
  const createTransfersTest = new CreateTransfers({
    documentClient,
    tableName: TABLE_NAME,
    numAccounts,
    batchSize: 10,
    accountSelectionStrategy: AccountSelectionStrategy.RANDOM_PEER_TO_PEER,
    progressMarker: writeRateIncrement,
  });
  const writeDriver = new LoadTestDriver(createTransfersTest, {
    targetRequestRatePerSecond: writeRateBase + writeRateIncrement * i,
    concurrency: 2,
    durationSeconds: durationPerTestCycleSeconds,
  });

  const readBalancesTest = new ReadBalances({
    documentClient,
    tableName: TABLE_NAME,
    numAccounts,
    batchSize: 100,
    progressMarker: readRateIncrement,
  });
  const readDriver = new LoadTestDriver(readBalancesTest, {
    targetRequestRatePerSecond: readRateBase + readRateIncrement * i,
    concurrency: 2,
    durationSeconds: durationPerTestCycleSeconds,
  });

  const [write, read] = await Promise.allSettled([writeDriver.run(), readDriver.run()]);
  if (write.status === "rejected" || read.status === "rejected") {
    throw new Error("Aborted run: " + { write, read });
  }
  results.push({ write: write.value, read: read.value });
  process.stdout.write("\n");
}

console.log(results);
