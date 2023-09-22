import * as ddc from "@aws-sdk/lib-dynamodb";
import { randomInt } from "crypto";
import PQueue from "p-queue";
import { Transfer, createAccount, createTransfer } from "./transactions.js";
import { monotonicFactory } from "ulid";

const ulid = monotonicFactory();

// Create a specified number of accounts beginging with id = 1, all starting out
// with zero balances.
export async function setupAccounts(
  documentClient: ddc.DynamoDBDocumentClient,
  tableName: string,
  queue: PQueue,
  accountCount: number,
) {
  for (let id = 1; id < accountCount; id++) {
    queue.add(async () =>
      createAccount(documentClient, tableName, {
        id: id,
        ledger: 700,
        debits_pending: 0,
        debits_posted: 0,
        credits_pending: 0,
        credits_posted: 0,
      }),
    );
  }
  await queue.onEmpty();
}

// Create a specified number of random transactions between accounts.
export async function writeRandomTransactions(
  documentClient: ddc.DynamoDBDocumentClient,
  tableName: string,
  queue: PQueue,
  accountCount: number,
  txCount: number,
) {
  let timestamp = Date.now();

  let count = 0;
  while (count++ < txCount) {
    if ((count & 1023) === 1023) {
      timestamp = Date.now();
    }

    // Don't blow out the in-memory queue size
    if (queue.size > 1_000) {
      do {
        await sleep(5);
      } while (queue.size > 200);
    }

    const fromAccount = randomInt(1, accountCount);
    let toAccount;
    do {
      toAccount = randomInt(1, accountCount);
    } while (fromAccount === toAccount);

    const tx: Transfer = {
      id: ulid(),
      ledger: 700,
      amount: 1 + randomInt(0, 1_000_000),
      debit_account_id: fromAccount,
      credit_account_id: toAccount,
      code: 0,
      flags: 0,
      pending_id: undefined,
      timeout: 0,
      timestamp,
    };

    queue.add(async () => {
      try {
        await createTransfer(documentClient, tableName, tx);
      } catch (err) {
        console.log({ message: "Transaction failed", tx, error: err });
      }
    });
  }

  await queue.onEmpty();
}

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
