import * as ddc from "@aws-sdk/lib-dynamodb";
import { randomUUID } from "crypto";

export interface Transfer {
  id: IdType;
  debit_account_id: AccountId;
  credit_account_id: AccountId;
  user_data?: ExternalId;
  pending_id: TransferId;
  timeout: Timeout;
  ledger: LedgerId;
  code?: Reason;
  flags?: TransferFlags;
  amount: Amount;
  timestamp?: Timestamp;
}

interface Account {
  id: AccountId;
  user_data?: ExternalId;
  ledger: LedgerId;
  code?: AccountType;
  flags?: AccountFlags;
  debits_pending: Amount;
  debits_posted: Amount;
  credits_pending: Amount;
  credits_posted: Amount;
  timestamp?: Timestamp;
}

export enum TransferResult {
  OK,
  INSUFFICIENT_FUNDS,
}

export async function createAccount(
  documentClient: ddc.DynamoDBDocumentClient,
  tableName: string,
  account: Account,
): Promise<void> {
  const { id, ledger, debits_pending, debits_posted, credits_pending, credits_posted, timestamp } = account;
  await documentClient.send(
    new ddc.TransactWriteCommand({
      ClientRequestToken: randomUUID(),
      TransactItems: [
        {
          Put: {
            TableName: tableName,
            Item: {
              pk: `account#${id}`,
              sk: `account#${id}`,
              ledger,
              debits_pending,
              debits_posted,
              credits_pending,
              credits_posted,
              timestamp,
            },
            ConditionExpression: "attribute_not_exists(pk)",
          },
        },
      ],
    }),
  );
}

export async function createTransfer(
  documentClient: ddc.DynamoDBDocumentClient,
  tableName: string,
  transfer: Transfer,
): Promise<TransferResult> {
  await documentClient.send(
    new ddc.TransactWriteCommand({
      ClientRequestToken: randomUUID(),
      TransactItems: [
        {
          Put: {
            TableName: tableName,
            Item: {
              pk: `transfer#${transfer.id}`,
              sk: `transfer#${transfer.id}`,
              ...transfer,
            },
            ConditionExpression: "attribute_not_exists(pk)",
          },
        },
        {
          Update: {
            TableName: tableName,
            Key: {
              pk: `account#${transfer.debit_account_id}`,
              sk: `account#${transfer.debit_account_id}`,
            },
            UpdateExpression: "SET debits_posted = debits_posted + :amount",
            ExpressionAttributeValues: {
              ":amount": transfer.amount,
            },
          },
        },
        {
          Update: {
            TableName: tableName,
            Key: {
              pk: `account#${transfer.credit_account_id}`,
              sk: `account#${transfer.credit_account_id}`,
            },
            UpdateExpression: "SET credits_posted = credits_posted + :amount",
            ExpressionAttributeValues: {
              ":amount": transfer.amount,
            },
          },
        },
      ],
    }),
  );

  return TransferResult.OK;
}

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
type AccountFlags = any;
