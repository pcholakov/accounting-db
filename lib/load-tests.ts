import { Histogram, RecordableHistogram, createHistogram, performance } from "perf_hooks";

export interface Test {
  setup(): Promise<void>;
  teardown(): Promise<void>;
  /// Drive requests to the system under test. If you perform more than one unit
  /// of work, override `requestsPerIteration` to return the number of units of
  /// work performed.
  request(): Promise<void>;
  /// Number of work items performed per iteration. Defaults to 1.
  requestsPerIteration(): number;
  /// Return additional configuration information to be included in the results.
  config(): any;
}

export abstract class AbstractBaseTest implements Test {
  async setup(): Promise<void> {}
  async teardown(): Promise<void> {}
  abstract request(): Promise<void>;
  requestsPerIteration() {
    return 1;
  }
  config() {
    return undefined;
  }
}

export class LoadTestDriver {
  private concurrency: number;
  private targetRps: number;
  private test: Test;
  private overallDurationMs: number;
  private warmupDurationMs: number;
  private durationMicros: RecordableHistogram;
  private iterationCount: number = 0;
  private failedIterationCount: number = 0;
  private benchmarkDurationMs: number;
  private requestsPerIteration: number;
  private workerRunTime: number = 0;
  private workerBackoffTime: number = 0;

  constructor(
    test: Test,
    opts: {
      concurrency: number;
      targetRequestRatePerSecond: number;
      durationSeconds: number;
      transactionsPerRequest?: number;
    },
  ) {
    this.concurrency = opts.concurrency;
    this.targetRps = opts.targetRequestRatePerSecond;
    this.overallDurationMs = opts.durationSeconds * 1_000;
    this.requestsPerIteration = opts.transactionsPerRequest ?? test.requestsPerIteration();
    this.warmupDurationMs = Math.min(this.overallDurationMs / 10, 10_000); // 10% of duration or 10s, whichever is smaller
    this.benchmarkDurationMs = this.overallDurationMs - this.warmupDurationMs;
    this.test = test;
    this.durationMicros = createHistogram();
  }

  async run(): Promise<any> {
    await this.test.setup();

    const avgIterationDuration = this.concurrency / (this.targetRps / this.requestsPerIteration);
    const workerIterationLengthMs = 1000 * avgIterationDuration;
    const startTime = performance.now();
    const warmupEndime = startTime + this.warmupDurationMs;
    const endTime = startTime + this.overallDurationMs;

    const workers: Array<Promise<void>> = [];

    const concurrentWorkerLoop = async (id: string) => {
      await sleep(Math.random() * workerIterationLengthMs); // startup jitter

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
        if (iterationDuration < workerIterationLengthMs) {
          await sleep(workerIterationLengthMs - iterationDuration);
        }
      }

      // Measurement loop
      do {
        const iterationStart = performance.now();
        try {
          await this.test.request();
        } catch (error) {
          this.failedIterationCount += 1;
        }
        const iterationEnd = performance.now();
        this.iterationCount += 1;

        const iterationDurationMillis = iterationEnd - iterationStart;
        {
          // Record durations in the histogram in microseconds as it doesn't accept zero values.
          // TODO: report dropped zero-length durations
          const iterationDurationMicros = Math.floor(iterationDurationMillis * 1_000);
          if (iterationDurationMicros > 0) {
            this.durationMicros.record(iterationDurationMicros);
          }
        }

        this.workerRunTime += iterationDurationMillis;
        if (iterationDurationMillis < workerIterationLengthMs) {
          const backoffTimeMillis = workerIterationLengthMs - iterationDurationMillis;
          this.workerBackoffTime += backoffTimeMillis;
          await sleep(backoffTimeMillis);
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

    const totalRequestCount = this.iterationCount * this.requestsPerIteration;
    return {
      configuration: {
        targetArrivalRate: this.targetRps,
        concurrency: this.concurrency,
        duration: this.overallDurationMs,
        warmup: this.warmupDurationMs,
        test: this.test.config(),
      },
      requests: totalRequestCount,
      iterations: this.iterationCount,
      requestsPerIteration: this.requestsPerIteration,
      failedIterations: this.failedIterationCount,
      errorRatio: this.failedIterationCount / this.iterationCount,
      iterationsPerSecondOverall: (this.iterationCount * 1_000) / this.benchmarkDurationMs,
      iterationsPerSecondPerWorker: (this.iterationCount * 1_000) / this.concurrency / this.benchmarkDurationMs,
      requestsPerSecondOverall: (totalRequestCount * 1_000) / this.benchmarkDurationMs,
      targetArrivalRateRatio: (totalRequestCount * 1_000) / this.benchmarkDurationMs / this.targetRps,
      durationStatsMillis: {
        avg: this.durationMicros.mean / 1_000,
        p0: this.durationMicros.min / 1_000,
        p25: this.durationMicros.percentile(25) / 1_000,
        p50: this.durationMicros.percentile(50) / 1_000,
        p75: this.durationMicros.percentile(75) / 1_000,
        p90: this.durationMicros.percentile(90) / 1_000,
        p95: this.durationMicros.percentile(95) / 1_000,
        p99: this.durationMicros.percentile(99) / 1_000,
        p99_9: this.durationMicros.percentile(99.9) / 1_000,
        p100: this.durationMicros.max / 1_000,
      },
      workerUtilization: {
        runTimeMillis: this.workerRunTime,
        backoffTimeMillis: this.workerBackoffTime,
        utilization: this.workerRunTime / (this.workerRunTime + this.workerBackoffTime),
      },
    };
  }
}

export async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
