import * as ddc from "@aws-sdk/lib-dynamodb";
import { MetadataBearer } from "@aws-sdk/types";
import { randomInt } from "crypto";
import pRetry from "p-retry";
import { AccountSelectionStrategy, buildRandomTransactions } from "./generators.js";
import { AbstractBaseTest } from "./load-test-runner.js";
import { CreateTranfersResult, createTransfersBatch, getAccountsBatch } from "./transactions.js";

export class CreateTransfersLoadTest extends AbstractBaseTest {
  private readonly documentClient: ddc.DynamoDBDocumentClient;
  private readonly tableName: string;
  private readonly transferBatchSize: number;
  private readonly numAccounts: number;
  private readonly accountSelectionStrategy;
  private readonly retryStrategy: (fn: () => Promise<CreateTranfersResult>) => Promise<CreateTranfersResult>;
  private readonly _progressMarker: number | undefined;
  private _globalWriteCounter = 0;
  private _sdk_retryDelay = 0;
  private _sdk_retryAttempts = 0;
  private _conflicts_retryDelay = 0;
  private _conflicts_retryAttempts = 0;

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
    this.retryStrategy = async (fn: () => Promise<CreateTranfersResult>) => {
      // Hack to track the p-Retry backoff time per batch while reusing the
      // stock calculation. This variable is in the anonymous closure created
      // for each call to createTransfersBatch, so it's safe to hold some state
      // specific to the particular batch here.
      let startTime = 0;

      return pRetry(
        async () => {
          // If this is not the very first attempt, record the retry delay
          if (startTime != 0) {
            this._conflicts_retryDelay += performance.now() - startTime;
          }
          return await fn();
        },
        {
          retries: 3,
          minTimeout: 20,
          factor: 1.2,
          randomize: true,
          maxTimeout: 60,
          onFailedAttempt: (error) => {
            this._conflicts_retryAttempts += 1;
            startTime = performance.now();
          },
        },
      );
    };
  }

  async request() {
    const txns = buildRandomTransactions(this.transferBatchSize, this.accountSelectionStrategy, {
      numAccounts: this.numAccounts,
    });

    try {
      const result = await createTransfersBatch(this.documentClient, this.tableName, txns, this.retryStrategy);
      if (this._progressMarker) {
        this._globalWriteCounter += txns.length;
        if (this._globalWriteCounter % this._progressMarker == 0) {
          process.stdout.write("+");
        }
      }
      // See https://github.com/aws/aws-sdk-js-v3/blob/f1fe216ef15d6b7503755cb3ef8568d00c04b6f8/packages/middleware-retry/src/defaultStrategy.ts#L113-L147
      this._sdk_retryAttempts += (result?.$metadata?.attempts ?? 1) - 1;
      this._sdk_retryDelay += result?.$metadata?.totalRetryDelay ?? 0;
    } catch (err) {
      this._sdk_retryAttempts += ((err as MetadataBearer)?.$metadata?.attempts ?? 1) - 1;
      this._sdk_retryDelay += (err as MetadataBearer)?.$metadata?.totalRetryDelay ?? 0;
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
      retries: {
        sdk_retryAttempts: this._sdk_retryAttempts,
        sdk_retryDelay: this._sdk_retryDelay,
        conflicts_retryAttempts: this._conflicts_retryAttempts,
        conflicts_retryDelay: this._conflicts_retryDelay,
      },
    };
  }
}

export class ReadAccountBalancesLoadTest extends AbstractBaseTest {
  private readonly documentClient: ddc.DynamoDBDocumentClient;
  private readonly tableName: string;
  private readonly numAccounts: number;
  private readonly batchSize: number;
  private readonly progressMarker: number | undefined;
  private _globalReadCounter = 0;
  private _sdk_retryDelay = 0;
  private _sdk_retryAttempts = 0;

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
    this.progressMarker = opts.progressMarker;
  }

  async request() {
    const accountIds = new Set<number>();
    while (accountIds.size < this.batchSize) {
      accountIds.add(randomInt(0, this.numAccounts));
    }
    try {
      const result = await getAccountsBatch(this.documentClient, this.tableName, Array.from(accountIds));
      if (this.progressMarker) {
        this._globalReadCounter += accountIds.size;
        if (this._globalReadCounter % this.progressMarker == 0) {
          process.stdout.write("-");
        }
      }
      // See https://github.com/aws/aws-sdk-js-v3/blob/f1fe216ef15d6b7503755cb3ef8568d00c04b6f8/packages/middleware-retry/src/defaultStrategy.ts#L113-L147
      this._sdk_retryAttempts += (result?.$metadata?.attempts ?? 1) - 1;
      this._sdk_retryDelay += result?.$metadata?.totalRetryDelay ?? 0;
    } catch (err) {
      this._sdk_retryAttempts += ((err as MetadataBearer)?.$metadata?.attempts ?? 1) - 1;
      this._sdk_retryDelay += (err as MetadataBearer)?.$metadata?.totalRetryDelay ?? 0;
      throw err;
    }
  }

  requestsPerIteration() {
    return this.batchSize;
  }

  config() {
    return {
      numAccounts: this.numAccounts,
      retries: {
        sdk_retryAttempts: this._sdk_retryAttempts,
        sdk_retryDelay: this._sdk_retryDelay,
      },
    };
  }
}
