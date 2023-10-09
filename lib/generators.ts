import { randomInt } from "crypto";
import { monotonicFactory } from "ulid";
import { Transfer } from "./transactions.js";

const ulid = monotonicFactory();

export enum AccountSelectionStrategy {
  RANDOM_PEER_TO_PEER = "RANDOM_PEER_TO_PEER",
  HOT_SPOT_RANDOM_PEERS = "HOT_SPOT_RANDOM_PEERS",
}

export function buildRandomTransactions(
  count: number,
  accountSelection: AccountSelectionStrategy,
  opts: {
    numAccounts: number;
    hotAccounts?: number;
  },
): Transfer[] {
  const timestamp = Date.now();

  const transfers: Transfer[] = [];

  for (let i = 0; i < count; i++) {
    let fromAccount, toAccount;

    switch (accountSelection) {
      case AccountSelectionStrategy.RANDOM_PEER_TO_PEER:
        fromAccount = randomInt(1, opts.numAccounts);
        do {
          toAccount = randomInt(1, opts.numAccounts);
        } while (fromAccount === toAccount);
        break;

      case AccountSelectionStrategy.HOT_SPOT_RANDOM_PEERS:
        fromAccount = opts.hotAccounts && opts.hotAccounts > 1 ? randomInt(1, 1 + opts.hotAccounts) : 1;
        toAccount = randomInt(1 + (opts.hotAccounts ?? 1), 1 + opts.numAccounts);
        break;

      default:
        throw new Error("Unsupported account selection strategy");
    }

    transfers.push({
      id: ulid(),
      ledger: 700,
      amount: 1 + randomInt(0, opts.numAccounts),
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
