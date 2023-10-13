import { RecordableHistogram, createHistogram, performance } from "perf_hooks";

export interface Test {
  setup(): Promise<void>;
  teardown(): Promise<void>;
  /// Drive requests to the system under test. If you perform more than one unit
  /// of work, override `requestsPerIteration` to return the number of units of
  /// work performed.
  performIteration(): Promise<void>;
  /// Number of work items performed per iteration. Defaults to 1.
  requestsPerIteration(): number;
  /// Return additional configuration information to be included in the results.
  testRunData(): any;
}

export abstract class AbstractBaseTest implements Test {
  async setup(): Promise<void> {}
  async teardown(): Promise<void> {}
  abstract performIteration(): Promise<void>;
  requestsPerIteration() {
    return 1;
  }
  testRunData(): object {
    return {};
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
  private errorCount: number = 0;
  private missedIterationCount: number = 0;
  private benchmarkDurationMs: number;
  private requestsPerIteration: number;
  private workerRunTime: number = 0;
  private workerBackoffTime: number = 0;
  private workerBehindScheduleTime: number = 0;

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
    if (this.targetRps == 0) {
      return;
    }

    await this.test.setup();

    const workerIterationLengthMs = (1000 * this.concurrency) / (this.targetRps / this.requestsPerIteration);
    const startTime = performance.now();
    const warmupEndTime = startTime + this.warmupDurationMs;
    const endTime = startTime + this.overallDurationMs;

    const workers: Array<Promise<void>> = [];

    const concurrentWorkerLoop = async (id: string) => {
      await sleep(Math.random() * workerIterationLengthMs); // startup jitter

      // Warmup - run work loop without measuring
      while (performance.now() < warmupEndTime) {
        const iterationStart = performance.now();
        try {
          await this.test.performIteration();
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
      let iterationStart = performance.now();
      do {
        try {
          await this.test.performIteration();
        } catch (error) {
          if (this.errorCount % 1000 == 0) {
            console.error(`Worker ${id} failed iteration ${this.iterationCount}`, error);
          }
          this.errorCount += 1;
        }
        const iterationEnd = performance.now();
        this.iterationCount += 1;

        const iterationDurationMillis = iterationEnd - iterationStart;
        {
          const iterationDurationMicros = Math.round(iterationDurationMillis * 1_000);
          if (iterationDurationMicros > 0) {
            this.durationMicros.record(iterationDurationMicros); // Doesn't accept 0 values
          } else {
            // Record (highly unlikely) sub-microsecond durations as 1us – this
            // is fine since we're reporting the final results in milliseconds.
            this.durationMicros.record(1);
          }
        }

        this.workerRunTime += iterationDurationMillis;

        const nextIterationStartMillis = iterationStart + workerIterationLengthMs;
        const backoffTimeMillis = nextIterationStartMillis - performance.now();
        if (backoffTimeMillis > 0) {
          this.workerBackoffTime += backoffTimeMillis;
          await sleep(backoffTimeMillis);
        } else {
          this.workerBehindScheduleTime += -backoffTimeMillis;
          if (-backoffTimeMillis > workerIterationLengthMs) {
            // If we're more than 1 full cycle behind schedule, record the appropriate number of skipped iterations due to backpressure
            this.missedIterationCount += Math.floor(-backoffTimeMillis / workerIterationLengthMs);
          }
        }

        iterationStart = nextIterationStartMillis;
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
      },
      testRunData: this.test.testRunData(),
      requests: totalRequestCount,
      iterations: this.iterationCount,
      requestsPerIteration: this.requestsPerIteration,
      errorCount: this.errorCount,
      missedIterationCount: this.missedIterationCount,
      failedIterationsRatio:
        (this.errorCount + this.missedIterationCount) / (this.iterationCount + this.missedIterationCount),
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
        behindScheduleTimeMillis: this.workerBehindScheduleTime,
        utilization: this.workerRunTime / (this.workerRunTime + this.workerBackoffTime),
      },
    };
  }
}

export async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
