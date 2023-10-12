import * as dynamodb from "@aws-sdk/client-dynamodb";
import * as ddc from "@aws-sdk/lib-dynamodb";
import { mockClient } from "aws-sdk-client-mock";
import "aws-sdk-client-mock-jest";
import { CreateTransfersResult, Transfer, TransferResult, createTransfersBatch } from "../lib/transactions.js";

const ddbMock = mockClient(ddc.DynamoDBDocumentClient);

const dynamoDbClient = new dynamodb.DynamoDBClient();
const dynamoDbDocumentClient = ddc.DynamoDBDocumentClient.from(dynamoDbClient, {
  marshallOptions: { removeUndefinedValues: true },
});

const TABLE_NAME = "transactions";

const txnCommon = {
  ledger: 700,
  amount: 10,
  code: 0,
  flags: 0,
  pending_id: undefined,
  timeout: 0,
  timestamp: Date.now(),
};
const txn1 = { ...txnCommon, id: "1", debit_account_id: 1, credit_account_id: 2 };
const txn2 = { ...txnCommon, id: "2", debit_account_id: 3, credit_account_id: 4, amount: 20 };

describe("transactions", () => {
  describe("conflicting items within a batch get retried as individual writes", () => {
    test("create transfers batch", async () => {
      ddbMock.on(ddc.TransactWriteCommand).rejectsOnce(
        new dynamodb.TransactionCanceledException({
          $metadata: {},
          message: "Transaction cancelled, please refer cancellation reasons for specific reasons",
          CancellationReasons: [
            { Code: "None" },
            {
              Code: "TransactionConflict",
              Message: "Transaction is ongoing for the item",
            },
          ],
        }),
      );

      const transfers: Transfer[] = [txn1, txn2];

      // Can't get p-Retry to work under Jest; trivial retry strategy to verify it is being used
      const testRetryStrategy = async (fn: () => Promise<CreateTransfersResult>) => {
        try {
          return await fn();
        } catch (err) {
          // Retry just once on TransactionCanceledException:
          if (err instanceof dynamodb.TransactionCanceledException) {
            return await fn();
          }
          throw err;
        }
      };

      const result = await createTransfersBatch(dynamoDbDocumentClient, TABLE_NAME, transfers, testRetryStrategy);
      expect(result.overallResult).toEqual(TransferResult.OK);
      expect(ddbMock).toHaveReceivedCommandTimes(ddc.TransactWriteCommand, 2);
    });
  });
});
