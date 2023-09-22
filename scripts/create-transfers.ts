import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import PQueue from "p-queue";
import { writeRandomTransactions } from "../lib/benchmarks.js";

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
  await writeRandomTransactions(documentClient, TABLE_NAME, queue, 10_000, 10_000);
}

main();
