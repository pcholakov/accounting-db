import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { randomInt } from "crypto";
import { write } from "../lib/transactions";

const dynamodbClient = new dynamodb.DynamoDBClient({
  region: "localhost",
  endpoint: "http://localhost:8000",
  credentials: {
    accessKeyId: "a",
    secretAccessKey: "k",
  },
});
const documentClient = ddc.DynamoDBDocumentClient.from(dynamodbClient);

describe("transactions", () => {
  beforeAll(async () => {
    await dynamodbClient.send(
      new dynamodb.CreateTableCommand({
        TableName: "transactions",
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
    await dynamodbClient.send(new dynamodb.DeleteTableCommand({ TableName: "transactions" }));
  });

  test("write a transfer", async () => {
    await write(documentClient, "transactions", {
      id: randomInt(Math.pow(2, 31) - 1),
      ledger: 1,
      amount: 1_000,
      credit_account_id: 101,
      debit_account_id: 100,
      code: 0,
      flags: 0,
      pending_id: 0,
      timeout: 0,
      timestamp: Date.now(),
    });
  });
});
