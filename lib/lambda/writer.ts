import { SQSEvent, SQSHandler } from "aws-lambda";
import * as sqs from "@aws-sdk/client-sqs";
import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { TransactWriteCommand } from "@aws-sdk/lib-dynamodb";
import { randomUUID } from "crypto";
import { monotonicFactory } from "ulid";

const ulid = monotonicFactory();

type IdType = number;
type AccountId = number;
type ExternalId = number;
type TransferId = number;
type Timeout = number;
type LedgerId = number;
type Reason = number;
type TransferFlags = number;
type Amount = number;
type Timestamp = number;
type AccountType = number;

const QUEUE_NAME = process.env.QUEUE_NAME;
const TABLE_NAME = process.env.TABLE_NAME;

const sqsClient = new sqs.SQSClient({});
const dynamoDbClient = new dynamodb.DynamoDBClient({});
const docClient = ddc.DynamoDBDocumentClient.from(dynamoDbClient);

export const handler: SQSHandler = async (event: SQSEvent) => {
  const records: any[] = event.Records;

  const writes: ddc.TransactWriteCommandInput = {
    ClientRequestToken: randomUUID(),
    TransactItems: [],
  };

  const batchId = ulid();

  for (const record of records) {
    const body = JSON.parse(record.body);
    const write: WriteRequest = body.request;
    const txnId = ulid();
    writes.TransactItems?.push({
      Put: {
        TableName: TABLE_NAME,
        Item: {
          pk: `tx#${txnId}`,
          sk: `tx#${txnId}`,
          // gsi1pk: `acc#${write.debit_account_id}`,
          // gsi1sk: `tx#${txnId}`,
          // gsi2pk: `acc#${write.credit_account_id}`,
          // gsi2sk: `tx#${txnId}`,
          ...write,
        },
      },
    });
  }

  const createResponseQueuePromise = sqsClient.send(
    new sqs.CreateQueueCommand({
      QueueName: `${QUEUE_NAME}-${batchId}`,
      tags: { timestamp: `${Date.now()}` },
    }),
  );
  const writeResultPromise = docClient.send(new TransactWriteCommand({ TransactItems: writes.TransactItems }));

  const writeResult = await writeResultPromise;
  const createTempQueueResult = await createResponseQueuePromise;

  const response: WriteResponse = {
    id: writeResult.$metadata.requestId,
    status: writeResult.$metadata.httpStatusCode,
  };

  sqsClient.send(
    new sqs.SendMessageCommand({
      QueueUrl: createTempQueueResult.QueueUrl,
      MessageBody: JSON.stringify(response),
    }),
  );

  // TODO: sweep old queues periodically based on timestamp

  return;
};

type WriteRequest = Transfer /*| Account*/;

interface WriteResponse {}

interface Transfer {
  id: IdType;
  debit_account_id: AccountId;
  credit_account_id: AccountId;
  user_data: ExternalId;
  pending_id: TransferId;
  timeout: Timeout;
  ledger: LedgerId;
  code: Reason;
  flags: TransferFlags;
  amount: Amount;
  timestamp: Timestamp;
}

type AccountFlags = any; // define the AccountFlags type here

interface Account {
  id: AccountId;
  user_data: ExternalId;
  ledger: LedgerId;
  code: AccountType;
  flags: AccountFlags;
  debits_pending: number;
  debits_posted: number;
  credits_pending: number;
  credits_posted: number;
  timestamp: number;
}
