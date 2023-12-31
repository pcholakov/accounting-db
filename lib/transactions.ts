import * as ddc from "@aws-sdk/lib-dynamodb";
import { TransactWriteCommandInput } from "@aws-sdk/lib-dynamodb";
import { MetadataBearer } from "@aws-sdk/types";
import assert from "assert";
import { randomUUID } from "crypto";

export interface Transfer {
  id: IdType;
  debit_account_id: AccountId;
  credit_account_id: AccountId;
  user_data?: ExternalId;
  pending_id?: IdType;
  timeout: Timeout;
  ledger: LedgerId;
  code?: Reason;
  flags?: TransferFlags;
  amount: Amount;
  timestamp?: Timestamp;
}

export interface Account {
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
  OK = "OK",
  INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS",
}

export interface CreateTransfersResult extends MetadataBearer {
  overallResult: TransferResult;
  itemsWritten: number;
  consumedWriteCapacity: number;
}

export interface GetAccountsResult extends MetadataBearer {
  accounts: Account[] | undefined;
  consumedReadCapacity: number;
}

type TransactItems = TransactWriteCommandInput["TransactItems"];
type ItemType = NonNullable<TransactItems>[number];

function noRetry<T>(fn: () => Promise<T>) {
  return fn();
}

export async function createAccount(
  documentClient: ddc.DynamoDBDocumentClient,
  tableName: string,
  account: Account,
): Promise<void> {
  return createAccountsBatch(documentClient, tableName, [account]);
}

export async function createAccountsBatch(
  documentClient: ddc.DynamoDBDocumentClient,
  tableName: string,
  accounts: Account[],
): Promise<void> {
  const items: TransactItems = [];
  for (const account of accounts) {
    const { id, ledger, debits_pending, debits_posted, credits_pending, credits_posted, timestamp } = account;
    items.push({
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
    });
  }
  await documentClient.send(
    new ddc.TransactWriteCommand({
      ClientRequestToken: randomUUID(),
      TransactItems: items,
    }),
  );
}

export async function getAccount(
  client: ddc.DynamoDBDocumentClient,
  tableName: string,
  accountId: AccountId,
): Promise<Account | undefined> {
  const result = await client.send(
    new ddc.GetCommand({ TableName: tableName, Key: { pk: `account#${accountId}`, sk: `account#${accountId}` } }),
  );
  if (!result.Item) {
    return undefined;
  }

  const { pk, sk, ...account } = result.Item;
  return { id: accountId, ...account } as Account;
}

export async function getAccountsBatch(
  client: ddc.DynamoDBDocumentClient,
  tableName: string,
  accountIds: AccountId[],
): Promise<GetAccountsResult> {
  const result = await client.send(
    new ddc.BatchGetCommand({
      RequestItems: {
        [tableName]: { Keys: accountIds.map((id) => ({ pk: `account#${id}`, sk: `account#${id}` })) },
      },
      ReturnConsumedCapacity: "TOTAL",
    }),
  );
  const consumedReadCapacity = (result.ConsumedCapacity ?? []).reduce(
    (acc, item) => acc + (item.CapacityUnits ?? 0),
    0,
  );
  return {
    accounts: result.Responses?.[tableName].map((item) => {
      const { pk, sk, ...account } = item;
      return { id: Number.parseInt(pk.split("#")[1]), ...account } as Account;
    }),
    $metadata: result.$metadata,
    consumedReadCapacity,
  };
}

export async function createTransfersBatch(
  documentClient: ddc.DynamoDBDocumentClient,
  tableName: string,
  batch: Transfer[],
  retry = noRetry<CreateTransfersResult>,
): Promise<CreateTransfersResult> {
  const items: TransactItems = [];
  const pendingUpdates: Map<AccountId, ItemType> = new Map();

  batch.forEach((transfer) => {
    items.push({
      Put: {
        TableName: tableName,
        Item: {
          // Transfers get partitioned on their unique id which keeps them separate from accounts to maximally distribute the load:
          pk: `transfer#${transfer.id}`,
          sk: `transfer#${transfer.id}`,

          // Alternatively, we could cluster transfers under one of the participating accounts – e.g. alongside the sender:
          // pk: `account#${transfer.debit_account_id}`,
          // sk: `account#${transfer.debit_account_id}#transfer#${transfer.id}`,

          ...transfer,
        },
        ConditionExpression: "attribute_not_exists(pk)",
      },
    });

    // Notes on DynamoDB account item operations:
    // - We use ADD instead of SET for balance updates, which effectively turns account balance updates into "upserts"
    //   and avoids needing to create millions of accounts upfront.
    // - We don't enforce this in benchmark mode, but we could delegate business invariants to DynamoDB using conditions like this:
    //   ConditionExpression: "debits_posted >= credits_posted"

    const debitAccountPendingTx = pendingUpdates.get(transfer.debit_account_id);
    if (debitAccountPendingTx) {
      const debit_amount = debitAccountPendingTx?.Update?.ExpressionAttributeValues?.[":debit_amount"];
      assert(debit_amount !== undefined);
      debitAccountPendingTx!.Update!.ExpressionAttributeValues![":debit_amount"] = debit_amount + transfer.amount;
    } else {
      const updateDebitBalance = {
        Update: {
          TableName: tableName,
          Key: {
            pk: `account#${transfer.debit_account_id}`,
            sk: `account#${transfer.debit_account_id}`,
          },
          UpdateExpression: "ADD debits_posted :debit_amount, credits_posted :credit_amount",
          ExpressionAttributeValues: {
            ":debit_amount": transfer.amount,
            ":credit_amount": 0,
          },
        },
      } as ItemType;
      items.push(updateDebitBalance);
      pendingUpdates.set(transfer.debit_account_id, updateDebitBalance);
    }

    const creditAccountPendingTx = pendingUpdates.get(transfer.credit_account_id);
    if (creditAccountPendingTx) {
      const credit_amount = creditAccountPendingTx?.Update?.ExpressionAttributeValues?.[":credit_amount"];
      assert(credit_amount !== undefined);
      creditAccountPendingTx!.Update!.ExpressionAttributeValues![":credit_amount"] = credit_amount + transfer.amount;
    } else {
      const updateCreditBalance = {
        Update: {
          TableName: tableName,
          Key: {
            pk: `account#${transfer.credit_account_id}`,
            sk: `account#${transfer.credit_account_id}`,
          },
          UpdateExpression: "ADD debits_posted :debit_amount, credits_posted :credit_amount",
          ExpressionAttributeValues: {
            ":debit_amount": 0,
            ":credit_amount": transfer.amount,
          },
        },
      } as ItemType;
      items.push(updateCreditBalance);
      pendingUpdates.set(transfer.credit_account_id, updateCreditBalance);
    }
  });

  const result = await retry(async () => {
    const result = await documentClient.send(
      new ddc.TransactWriteCommand({
        ClientRequestToken: randomUUID(),
        TransactItems: items,
        ReturnConsumedCapacity: "TOTAL",
      }),
    );
    const consumedWriteCapacity = (result.ConsumedCapacity ?? []).reduce(
      (acc, item) => acc + (item.CapacityUnits ?? 0),
      0,
    );
    return {
      overallResult: TransferResult.OK,
      itemsWritten: items.length,
      consumedWriteCapacity,
      $metadata: result?.$metadata,
    };
  });

  return result;
}

type IdType = string;
type AccountId = number;
type ExternalId = number;
type Timeout = number;
type LedgerId = number;
type Reason = number;
type TransferFlags = number;
type Amount = number;
type Timestamp = number;
type AccountType = number;
type AccountFlags = any;
