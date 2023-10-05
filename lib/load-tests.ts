import * as ddc from "@aws-sdk/lib-dynamodb";
import { randomInt } from "crypto";
import { AccountSelectionStrategy, buildRandomTransactions } from "./generators.js";
import { AbstractBaseTest } from "./load-test-runner.js";
import { createTransfersBatch, getAccountsBatch } from "./transactions.js";

export class CreateTransfers extends AbstractBaseTest {
  private readonly documentClient: ddc.DynamoDBDocumentClient;
  private readonly tableName: string;
  private readonly transferBatchSize: number;
  private readonly numAccounts: number;
  private readonly accountSelectionStrategy;
  private readonly _progressMarker: number | undefined;
  private _globalWriteCounter = 0;

  constructor(opts: {
    documentClient: ddc.DynamoDBDocumentClient;
    tableName: string;
    transferBatchSize: number;
    numAccounts: number;
    accountSelectionStrategy: AccountSelectionStrategy;
    progressMarker?: number;
  }) {
    super();
    this.documentClient = opts.documentClient;
    this.tableName = opts.tableName;
    this.transferBatchSize = opts.transferBatchSize;
    this.numAccounts = opts.numAccounts;
    this.accountSelectionStrategy = opts.accountSelectionStrategy;
    this._progressMarker = opts.progressMarker;
  }

  async request() {
    const txns = buildRandomTransactions(this.transferBatchSize, this.accountSelectionStrategy, {
      numAccounts: this.numAccounts,
    });

    try {
      await createTransfersBatch(this.documentClient, this.tableName, txns);
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
  private readonly readsPerRequest: number;
  private _globalReadCounter = 0;
  private _progressMarker: number | undefined;

  constructor(opts: {
    documentClient: ddc.DynamoDBDocumentClient;
    tableName: string;
    numAccounts: number;
    readsPerRequest?: number;
    progressMarker?: number;
  }) {
    super();
    this.documentClient = opts.documentClient;
    this.tableName = opts.tableName;
    this.numAccounts = opts.numAccounts;
    this.readsPerRequest = opts.readsPerRequest ?? 1;
    this._progressMarker = opts.progressMarker;
  }

  async request() {
    const accountIds = new Set<number>();
    while (accountIds.size < this.readsPerRequest) {
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
    return this.readsPerRequest;
  }

  config() {
    return {
      numAccounts: this.numAccounts,
    };
  }
}
