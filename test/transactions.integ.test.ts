import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { Transfer, TransferResult, createAccount, createTransfersBatch, getAccount } from "../lib/transactions.js";

const util = require("util");

util.inspect.defaultOptions.depth = null;

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

const TABLE_NAME = "transactions";

// Requires a DynamoDB Local to be running on port 8000.
describe("transactions integration test", () => {
  beforeAll(async () => {
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
  });

  afterAll(async () => {
    // await dynamodbClient.send(new dynamodb.DeleteTableCommand({ TableName: TABLE_NAME }));
  });

  describe("transact between accounts", () => {
    beforeAll(async () => {
      await Promise.all([
        createAccount(documentClient, TABLE_NAME, {
          id: 1,
          ledger: 700,
          debits_pending: 0,
          debits_posted: 0,
          credits_pending: 0,
          credits_posted: 0,
        }),
        createAccount(documentClient, TABLE_NAME, {
          id: 2,
          ledger: 700,
          debits_pending: 0,
          debits_posted: 0,
          credits_pending: 0,
          credits_posted: 0,
        }),
        createAccount(documentClient, TABLE_NAME, {
          id: 3,
          ledger: 700,
          debits_pending: 0,
          debits_posted: 0,
          credits_pending: 0,
          credits_posted: 0,
        }),
      ]);
    });

    test("create transfers batch", async () => {
      const transfers: Transfer[] = [
        {
          id: "4",
          ledger: 700,
          amount: 10,
          debit_account_id: 1,
          credit_account_id: 2,
          code: 0,
          flags: 0,
          pending_id: undefined,
          timeout: 0,
          timestamp: Date.now(),
        },
        {
          id: "5",
          ledger: 700,
          amount: 20,
          debit_account_id: 2,
          credit_account_id: 1,
          code: 0,
          flags: 0,
          pending_id: undefined,
          timeout: 0,
          timestamp: Date.now(),
        },
        {
          id: "6",
          ledger: 700,
          amount: 30,
          debit_account_id: 1,
          credit_account_id: 3,
          code: 0,
          flags: 0,
          pending_id: undefined,
          timeout: 0,
          timestamp: Date.now(),
        },
      ];

      const result = await createTransfersBatch(documentClient, TABLE_NAME, transfers);
      expect(result).toEqual(TransferResult.OK);

      const accounts = await Promise.all([
        getAccount(documentClient, TABLE_NAME, 1),
        getAccount(documentClient, TABLE_NAME, 2),
        getAccount(documentClient, TABLE_NAME, 3),
      ]);

      expect(accounts).toEqual([
        {
          id: 1,
          ledger: 700,
          debits_pending: 0,
          debits_posted: 40,
          credits_pending: 0,
          credits_posted: 20,
        },
        {
          id: 2,
          ledger: 700,
          debits_pending: 0,
          debits_posted: 20,
          credits_pending: 0,
          credits_posted: 10,
        },
        {
          id: 3,
          ledger: 700,
          debits_pending: 0,
          debits_posted: 0,
          credits_pending: 0,
          credits_posted: 30,
        },
      ]);

      // TODO: validate the transfers table
    });
  });
});
