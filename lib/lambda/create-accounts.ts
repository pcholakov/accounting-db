import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { Handler } from "aws-lambda";
import PQueue from "p-queue";
import { setupAccounts } from "../benchmarks.js";

const TABLE_NAME = process.env["TABLE_NAME"] ?? "transactions";

const dynamoDbClient = new dynamodb.DynamoDBClient();
const documentClient = ddc.DynamoDBDocumentClient.from(dynamoDbClient, {
  marshallOptions: { removeUndefinedValues: true },
});

const queue = new PQueue({ concurrency: 16 });

export const handler: Handler = async (event, context) => {
  const numAccounts = event.accounts ?? 1_000_000;
  console.log({ message: `Creating ${numAccounts} zero-balance accounts...` });
  await setupAccounts(documentClient, TABLE_NAME, queue, 1_000_000);
  console.log({ message: "Done creating accounts." });
};
