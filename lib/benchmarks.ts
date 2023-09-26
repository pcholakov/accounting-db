import * as ddc from "@aws-sdk/lib-dynamodb";
import PQueue from "p-queue";
import { monotonicFactory } from "ulid";
import { Transfer, createAccount } from "./transactions.js";
import { randomInt } from "crypto";

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

export enum AccountSelectionStrategy {
  RANDOM_PEER_TO_PEER,
  HOT_SPOT_RANDOM_PEERS,
}

export function buildRandomTransactions(
  count: number,
  accountSelection: AccountSelectionStrategy,
  opts: {
    maxAccount: number;
  },
): Transfer[] {
  const timestamp = Date.now();

  const transfers: Transfer[] = [];

  for (let i = 0; i < count; i++) {
    let fromAccount, toAccount;

    switch (accountSelection) {
      case AccountSelectionStrategy.RANDOM_PEER_TO_PEER:
        fromAccount = randomInt(1, opts.maxAccount);
        do {
          toAccount = randomInt(1, opts.maxAccount);
        } while (fromAccount === toAccount);
        break;

      case AccountSelectionStrategy.HOT_SPOT_RANDOM_PEERS:
        fromAccount = 1;
        toAccount = randomInt(2, opts.maxAccount);
        break;

      default:
        throw new Error("Unsupported account selection strategy");
    }

    transfers.push({
      id: ulid(),
      ledger: 700,
      amount: 1 + randomInt(0, opts.maxAccount),
      debit_account_id: fromAccount,
      credit_account_id: toAccount,
      code: 0,
      flags: 0,
      pending_id: undefined,
      timeout: 0,
      timestamp,
    });
  }

  return transfers;
}
