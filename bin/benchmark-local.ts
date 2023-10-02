import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { AccountSelectionStrategy, buildRandomTransactions } from "../lib/generators.js";
import { createTransfersBatch, getAccount } from "../lib/transactions.js";
import { AbstractBaseTest, LoadTestDriver, Test } from "../lib/load-tests.js";
import { randomInt } from "crypto";
import { inspect } from "util";

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

let _activityProgressCounter = 0;

class CreateTransfers implements Test {
  private readonly transferBatchSize: number;
  private readonly numAccounts: number;
  private readonly accountSelectionStrategy;

  constructor(opts: {
    transferBatchSize: number;
    numAccounts: number;
    accountSelectionStrategy: AccountSelectionStrategy;
  }) {
    this.transferBatchSize = opts.transferBatchSize;
    this.numAccounts = opts.numAccounts;
    this.accountSelectionStrategy = opts.accountSelectionStrategy;
  }

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
  }

  async teardown() {
    process.stdout.write("\n");
    await dynamoDbClient.send(new dynamodb.DeleteTableCommand({ TableName: TABLE_NAME }));
  }

  async request() {
    const txns = buildRandomTransactions(this.transferBatchSize, this.accountSelectionStrategy, {
      numAccounts: this.numAccounts,
    });

    try {
      await createTransfersBatch(documentClient, TABLE_NAME, txns);
      _activityProgressCounter += txns.length;
      if (_activityProgressCounter % 100 == 0) {
        process.stdout.write(".");
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

  constructor(opts: { numAccounts: number; readsPerRequest?: number }) {
    super();
    this.numAccounts = opts.numAccounts;
    this.readsPerRequest = opts.readsPerRequest ?? 1;
  }

  async request() {
    for (let i = 0; i < this.readsPerRequest; i++) {
      await getAccount(documentClient, TABLE_NAME, randomInt(1, this.numAccounts));
    }
  }

  requestsPerIteration(): number {
    return this.readsPerRequest;
  }

  config() {
    return {
      numAccounts: this.numAccounts,
    };
  }
}

const durationSeconds = 10;
const numAccounts = 1_000;

const createTransfersTest = new CreateTransfers({
  transferBatchSize: 10,
  numAccounts,
  accountSelectionStrategy: AccountSelectionStrategy.RANDOM_PEER_TO_PEER,
});
const writeWorkload = new LoadTestDriver(createTransfersTest, {
  targetRequestRatePerSecond: 1_000,
  concurrency: 2,
  durationSeconds,
});

const readBalancesTest = new ReadBalances({ numAccounts, readsPerRequest: 1 });
const readWorkload = new LoadTestDriver(readBalancesTest, {
  targetRequestRatePerSecond: 2_000,
  concurrency: 20,
  durationSeconds,
});

const [write, read] = await Promise.allSettled([writeWorkload.run(), readWorkload.run()]);
console.log({ write, read });
