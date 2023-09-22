import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { setupAccounts } from "../lib/benchmarks.js";
import PQueue from "p-queue";

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

async function main() {
  setupAccounts(documentClient, TABLE_NAME, queue, 10_000);
}

main();