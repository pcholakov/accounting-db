import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { SQSEvent, SQSHandler } from "aws-lambda";
import { createTransfer } from "../transactions";

const TABLE_NAME = process.env.TABLE_NAME ?? "assignments-db";

// const sqsClient = new sqs.SQSClient({});
const dynamoDbClient = new dynamodb.DynamoDBClient({});
const documentClient = ddc.DynamoDBDocumentClient.from(dynamoDbClient);

export const handler: SQSHandler = async (event: SQSEvent) => {
  const records: any[] = event.Records;

  // const batchId = ulid();

  if (records.length != 1) {
    throw new Error("Only one record per batch is supported.");
  }

  const body = JSON.parse(records[0].body);

  createTransfer(documentClient, TABLE_NAME, body);

  // const ack = {
  //   id: writeResult.$metadata.requestId,
  //   status: writeResult.$metadata.httpStatusCode,
  // };

  // await sqsClient.send(
  //   new SendMessageCommand({
  //     QueueUrl: "...",
  //     MessageBody: JSON.stringify(ack),
  //   }),
  // );
};
