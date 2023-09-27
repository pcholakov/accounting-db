import { Histogram, RecordableHistogram, createHistogram, performance } from "perf_hooks";

export interface Test {
  setup(): Promise<void>;
  teardown(): Promise<void>;
  request(): Promise<void>;
}

export class LoadTestDriver {
  private concurrency: number;
  private arrivalRate: number;
  private test: Test;
  private overallDurationMs: number;
  private warmupDurationMs: number;
  private _durationMicros: RecordableHistogram;
  private _requestCount: number = 0;
  private _workUnitsCount: number = 0;
  private _errorCount: number = 0;
  private benchmarkDurationMs: number;
  private transactionsPerRequest: number;

  constructor(
    test: Test,
    opts: {
      concurrency: number;
      arrivalRate: number;
      durationSeconds: number;
      transactionsPerRequest?: number;
    },
  ) {
    this.concurrency = opts.concurrency;
    this.arrivalRate = opts.arrivalRate;
    this.overallDurationMs = opts.durationSeconds * 1_000;
    this.transactionsPerRequest = opts.transactionsPerRequest ?? 1;
    this.warmupDurationMs = Math.min(this.overallDurationMs / 10, 10_000); // 10% of duration or 10s, whichever is smaller
    this.benchmarkDurationMs = this.overallDurationMs - this.warmupDurationMs;
    this.test = test;
    this._durationMicros = createHistogram();
  }

  async run(): Promise<any> {
    await this.test.setup();

    const intervalMs = (1000 * this.concurrency) / this.arrivalRate;
    const startTime = performance.now();
    const warmupEndime = startTime + this.warmupDurationMs;
    const endTime = startTime + this.overallDurationMs;

    const workers: Array<Promise<void>> = [];

    const concurrentWorkerLoop = async (id: string) => {
      await sleep(Math.random() * intervalMs); // startup jitter

      // Warmup - run work loop without measuring
      while (performance.now() < warmupEndime) {
        const iterationStart = performance.now();
        try {
          await this.test.request();
        } catch (error) {
          // ignore errors during warmup
        }
        const iterationEnd = performance.now();
        const iterationDuration = iterationEnd - iterationStart;
        if (iterationDuration < intervalMs) {
          await sleep(intervalMs - iterationDuration);
        }
      }

      // Measurement loop
      do {
        const iterationStart = performance.now();
        try {
          await this.test.request();
        } catch (error) {
          this._errorCount += 1;
        }
        const iterationEnd = performance.now();

        const iterationDurationMicros = (iterationEnd - iterationStart) * 1_000;
        this._requestCount += 1;
        this._workUnitsCount += this.transactionsPerRequest;
        const durationInt = Math.floor(iterationDurationMicros);
        if (durationInt > 0) {
          // TODO: surface zero durations to the user
          this._durationMicros.record(durationInt);
        }

        if (iterationDurationMicros < intervalMs) {
          await sleep(intervalMs - iterationDurationMicros);
        }
      } while (performance.now() < endTime);
    };

    for (let i = 0; i < this.concurrency; i++) {
      workers.push(concurrentWorkerLoop(`w${i}`));
    }

    await Promise.all(workers);
    return this.stop();
  }

  async stop(): Promise<any> {
    await this.test.teardown();

    return {
      configuration: {
        concurrency: this.concurrency,
        targetArrivalRate: this.arrivalRate,
        duration: this.overallDurationMs,
        warmup: this.warmupDurationMs,
      },
      requestCount: this._requestCount,
      errorCount: this._errorCount,
      errorRate: this._errorCount / this._requestCount,
      requestRate: (this._requestCount * 1_000) / this.benchmarkDurationMs,
      workUnitThroughput: (this._workUnitsCount * 1_000) / this.benchmarkDurationMs,
      arrivalRateRatio: (this._requestCount * 1_000) / this.benchmarkDurationMs / this.arrivalRate,
      durationsMillis: {
        avg: this._durationMicros.mean / 1_000,
        p0: this._durationMicros.min / 1_000,
        p50: this._durationMicros.percentile(50) / 1_000,
        p75: this._durationMicros.percentile(75) / 1_000,
        p90: this._durationMicros.percentile(90) / 1_000,
        p95: this._durationMicros.percentile(95) / 1_000,
        p99: this._durationMicros.percentile(99) / 1_000,
        p99_9: this._durationMicros.percentile(99.9) / 1_000,
        p100: this._durationMicros.max / 1_000,
      },
    };
  }
}

export async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
