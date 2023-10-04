import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { randomInt } from "crypto";
import { inspect } from "util";
import { AccountSelectionStrategy, buildRandomTransactions } from "../lib/generators.js";
import { AbstractBaseTest, LoadTestDriver, Test } from "../lib/load-tests.js";
import { createTransfersBatch, getAccountsBatch } from "../lib/transactions.js";

inspect.defaultOptions.depth = 5;

const TABLE_NAME = process.env["TABLE_NAME"] ?? "transactions";

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

class CreateTransfers extends AbstractBaseTest {
  private readonly transferBatchSize: number;
  private readonly numAccounts: number;
  private readonly accountSelectionStrategy;
  private _globalWriteCounter = 0;
  private _progressMarker: number | undefined;

  constructor(opts: {
    transferBatchSize: number;
    numAccounts: number;
    accountSelectionStrategy: AccountSelectionStrategy;
    progressMarker?: number;
  }) {
    super();
    this.transferBatchSize = opts.transferBatchSize;
    this.numAccounts = opts.numAccounts;
    this.accountSelectionStrategy = opts.accountSelectionStrategy;
    this._progressMarker = opts.progressMarker;
  }

  async request() {
    const txns = buildRandomTransactions(this.transferBatchSize, this.accountSelectionStrategy, {
      numAccounts: this.numAccounts,
    });

    try {
      await createTransfersBatch(documentClient, TABLE_NAME, txns);
      if (this._progressMarker) {
        this._globalWriteCounter += txns.length;
        if (this._globalWriteCounter % this._progressMarker == 0) {
          process.stdout.write("+");
        }
      }
    } catch (err) {
      console.log({ message: "Transaction batch failed", batch: { _0: txns[0], xs: "..." }, error: err });
      throw err;
    }
  }

  requestsPerIteration() {
    return this.transferBatchSize;
  }

  config() {
    return {
      transferBatchSize: this.transferBatchSize,
      numAccounts: this.numAccounts,
      accountSelectionStrategy: this.accountSelectionStrategy,
    };
  }
}

class ReadBalances extends AbstractBaseTest {
  private readonly numAccounts: number;
  private readonly readsPerRequest: number;
  private _globalReadCounter = 0;
  private _progressMarker: number | undefined;

  constructor(opts: { numAccounts: number; readsPerRequest?: number; progressMarker?: number }) {
    super();
    this.numAccounts = opts.numAccounts;
    this.readsPerRequest = opts.readsPerRequest ?? 1;
    this._progressMarker = opts.progressMarker;
  }

  async request() {
    const accountIds = new Set<number>();
    while (accountIds.size < this.readsPerRequest) {
      accountIds.add(randomInt(0, this.numAccounts));
    }
    await getAccountsBatch(documentClient, TABLE_NAME, Array.from(accountIds));
    if (this._progressMarker) {
      this._globalReadCounter += accountIds.size;
      if (this._globalReadCounter % this._progressMarker == 0) {
        process.stdout.write("-");
      }
    }
  }

  requestsPerIteration() {
    return this.readsPerRequest;
  }

  config() {
    return {
      numAccounts: this.numAccounts,
    };
  }
}

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
const testSteps = 10;

await createDatabaseTable({ recreateIfExists: false });

const results = [];
for (let i = 0; i < testSteps; i++) {
  const createTransfersTest = new CreateTransfers({
    numAccounts,
    transferBatchSize: 10,
    accountSelectionStrategy: AccountSelectionStrategy.RANDOM_PEER_TO_PEER,
    progressMarker: writeRateIncrement,
  });
  const writeDriver = new LoadTestDriver(createTransfersTest, {
    targetRequestRatePerSecond: writeRateBase + writeRateIncrement * i,
    concurrency: 2,
    durationSeconds: durationPerTestCycleSeconds,
  });

  const readBalancesTest = new ReadBalances({ numAccounts, readsPerRequest: 100, progressMarker: readRateIncrement });
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
