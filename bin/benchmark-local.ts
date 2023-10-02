import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { AccountSelectionStrategy, buildRandomTransactions } from "../lib/generators.js";
import { createTransfersBatch } from "../lib/transactions.js";
import { LoadTestDriver, Test } from "../lib/load-tests.js";

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

const createTransfersTest = new CreateTransfers({
  transferBatchSize: 33,
  numAccounts: 100_000,
  accountSelectionStrategy: AccountSelectionStrategy.RANDOM_PEER_TO_PEER,
});
const loadTest = new LoadTestDriver(createTransfersTest, {
  targetRequestRatePerSecond: 2_000,
  concurrency: 4,
  durationSeconds: 10,
});
console.log(await loadTest.run());
