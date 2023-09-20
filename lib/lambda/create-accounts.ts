import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import PQueue from "p-queue";
import { createAccount } from "../transactions.js";

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

const queue = new PQueue({ concurrency: 10 });

// Create a specified number of accounts beginging with id = 1, all starting out
// with zero balances.
export async function setupAccounts(accountCount: number = 10_000) {
  for (let id = 1; id < accountCount; id++) {
    queue.add(async () =>
      createAccount(documentClient, TABLE_NAME, {
        id: id,
        ledger: 700,
        debits_pending: 0,
        debits_posted: 0,
        credits_pending: 0,
        credits_posted: 0,
      }),
    );
  }
}

// setupAccounts();
