import * as ddc from "@aws-sdk/lib-dynamodb";
import PQueue from "p-queue";
import { createAccount } from "./transactions.js";

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
