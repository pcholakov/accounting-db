import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { createAccount, createTransfer } from "../lib/transactions.js";

const dynamodbClient = new dynamodb.DynamoDBClient({
  region: "localhost",
  endpoint: "http://localhost:8000",
  credentials: {
    accessKeyId: "a",
    secretAccessKey: "k",
  },
});

const documentClient = ddc.DynamoDBDocumentClient.from(dynamodbClient, {
  marshallOptions: { removeUndefinedValues: true },
});

const TABLE_NAME = "transactions";

describe("transactions", () => {
  beforeAll(async () => {
    try {
      await dynamodbClient.send(new dynamodb.DeleteTableCommand({ TableName: TABLE_NAME }));
    } catch (err) {
      if (!(err instanceof dynamodb.ResourceNotFoundException)) {
        throw err;
      }
    }
    
    await dynamodbClient.send(
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

    test("create transfers", async () => {
      await createTransfer(documentClient, TABLE_NAME, {
        id: "1",
        ledger: 700,
        amount: 10,
        debit_account_id: 1,
        credit_account_id: 2,
        code: 0,
        flags: 0,
        pending_id: undefined,
        timeout: 0,
        timestamp: Date.now(),
      });
      await createTransfer(documentClient, TABLE_NAME, {
        id: "2",
        ledger: 700,
        amount: 10,
        debit_account_id: 2,
        credit_account_id: 1,
        code: 0,
        flags: 0,
        pending_id: undefined,
        timeout: 0,
        timestamp: Date.now(),
      });
      await createTransfer(documentClient, TABLE_NAME, {
        id: "3",
        ledger: 700,
        amount: 10,
        debit_account_id: 1,
        credit_account_id: 3,
        code: 0,
        flags: 0,
        pending_id: undefined,
        timeout: 0,
        timestamp: Date.now(),
      });
    });
  });
});
