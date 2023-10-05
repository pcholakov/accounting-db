import * as ddc from "@aws-sdk/lib-dynamodb";
import { randomInt } from "crypto";
import pRetry from "p-retry";
import { AccountSelectionStrategy, buildRandomTransactions } from "./generators.js";
import { AbstractBaseTest } from "./load-test-runner.js";
import { createTransfersBatch, getAccountsBatch } from "./transactions.js";

export class CreateTransfers extends AbstractBaseTest {
  private readonly documentClient: ddc.DynamoDBDocumentClient;
  private readonly tableName: string;
  private readonly transferBatchSize: number;
  private readonly numAccounts: number;
  private readonly accountSelectionStrategy;
  private readonly retryStrategy: (fn: () => Promise<void>) => Promise<void>;
  private readonly _progressMarker: number | undefined;
  private _globalWriteCounter = 0;

  constructor(opts: {
    documentClient: ddc.DynamoDBDocumentClient;
    tableName: string;
    batchSize: number;
    numAccounts: number;
    accountSelectionStrategy: AccountSelectionStrategy;
    progressMarker?: number;
  }) {
    super();
    this.documentClient = opts.documentClient;
    this.tableName = opts.tableName;
    this.transferBatchSize = opts.batchSize;
    this.numAccounts = opts.numAccounts;
    this.accountSelectionStrategy = opts.accountSelectionStrategy;
    this._progressMarker = opts.progressMarker;

    // Naively retry the entire batch. A better approach may be to split out just
    // the conflicting items and retry those in a separate transaction. Since we
    // don't return partial success currently, it doesn't make much difference,
    // but in a highly contended scenario that would increase the goodput.
    this.retryStrategy = async (fn: () => Promise<void>) =>
      pRetry(
        async () => {
          await fn();
        },
        {
          retries: 3,
          minTimeout: 20, // ~half of empirically observed p50 latency for large transactions
          factor: 1.2,
          randomize: true, // apply a random 100-200% jitter to retry intervals
          maxTimeout: 60,
        },
      );
  }

  async request() {
    const txns = buildRandomTransactions(this.transferBatchSize, this.accountSelectionStrategy, {
      numAccounts: this.numAccounts,
    });

    try {
      await createTransfersBatch(this.documentClient, this.tableName, txns, this.retryStrategy);
      if (this._progressMarker) {
        this._globalWriteCounter += txns.length;
        if (this._globalWriteCounter % this._progressMarker == 0) {
          process.stdout.write("+");
        }
      }
    } catch (err) {
      console.log({ message: "Transaction batch failed", batch: { _0: txns[0], xs: "..." }, error: err });
      throw err;
    }
  }

  requestsPerIteration() {
    return this.transferBatchSize;
  }

  config() {
    return {
      transferBatchSize: this.transferBatchSize,
      numAccounts: this.numAccounts,
      accountSelectionStrategy: this.accountSelectionStrategy,
    };
  }
}

export class ReadBalances extends AbstractBaseTest {
  private readonly documentClient: ddc.DynamoDBDocumentClient;
  private readonly tableName: string;
  private readonly numAccounts: number;
  private readonly batchSize: number;
  private _globalReadCounter = 0;
  private _progressMarker: number | undefined;

  constructor(opts: {
    documentClient: ddc.DynamoDBDocumentClient;
    tableName: string;
    numAccounts: number;
    batchSize: number;
    progressMarker?: number;
  }) {
    super();
    this.documentClient = opts.documentClient;
    this.tableName = opts.tableName;
    this.numAccounts = opts.numAccounts;
    this.batchSize = opts.batchSize;
    this._progressMarker = opts.progressMarker;
  }

  async request() {
    const accountIds = new Set<number>();
    while (accountIds.size < this.batchSize) {
      accountIds.add(randomInt(0, this.numAccounts));
    }
    await getAccountsBatch(this.documentClient, this.tableName, Array.from(accountIds));
    if (this._progressMarker) {
      this._globalReadCounter += accountIds.size;
      if (this._globalReadCounter % this._progressMarker == 0) {
        process.stdout.write("-");
      }
    }
  }

  requestsPerIteration() {
    return this.batchSize;
  }

  config() {
    return {
      numAccounts: this.numAccounts,
    };
  }
}
