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
  code: Reason;
  flags: TransferFlags;
  amount: Amount;
  timestamp: Timestamp;
}

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

export enum TransferResult {
  OK,
  INSUFFICIENT_FUNDS,
}

export async function write(
  documentClient: ddc.DynamoDBDocumentClient,
  tableName: string,
  transfer: Transfer,
): Promise<TransferResult> {
  // TODO: validate invariants

  await documentClient.send(
    new ddc.TransactWriteCommand({
      ClientRequestToken: randomUUID(),
      TransactItems: [
        {
          Put: {
            TableName: tableName,
            Item: {
              pk: `tx#${transfer.id}`,
              sk: `tx#${transfer.id}`,
              ...transfer,
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
