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
  private requestLatencyMicros: RecordableHistogram;
  private serviceTimeMicros: RecordableHistogram;
  private iterationCount: number = 0;
  private errorIterations: number = 0;
  private missedIterations: number = 0;
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
    this.requestLatencyMicros = createHistogram();
    this.serviceTimeMicros = createHistogram();
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
      // Jitter startup and perform the warmup iterations without measuring
      await sleep(Math.random() * workerIterationLengthMs);
      await this.doWarmup(warmupEndTime, workerIterationLengthMs);

      // Measurement loop
      let iterationStart = performance.now();
      do {
        const requestStart = performance.now();
        try {
          await this.test.performIteration();
        } catch (error) {
          if (this.errorIterations % 1000 == 0) {
            console.error(`Worker ${id} failed iteration ${this.iterationCount}`, error);
          }
          this.errorIterations += 1;
        }
        const iterationCompleted = performance.now();
        this.iterationCount += 1;

        const iterationDurationMillis = iterationCompleted - iterationStart;
        this.recordDuration(this.requestLatencyMicros, Math.round(iterationDurationMillis * 1000));
        this.recordDuration(this.serviceTimeMicros, Math.round((iterationCompleted - requestStart) * 1000));

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
            this.missedIterations += Math.floor(-backoffTimeMillis / workerIterationLengthMs);
          }
        }

        iterationStart = nextIterationStartMillis;
      } while (iterationStart < endTime);
    };

    for (let i = 0; i < this.concurrency; i++) {
      workers.push(concurrentWorkerLoop(`w${i}`));
    }

    await Promise.all(workers);
    return this.stop();
  }

  private recordDuration(histogram: RecordableHistogram, iterationDurationMicros: number) {
    if (iterationDurationMicros > 0) {
      histogram.record(iterationDurationMicros); // Doesn't accept 0 values
    } else {
      // Record (highly unlikely) sub-microsecond durations as 1us – this
      // is fine since we're reporting the final results in milliseconds.
      histogram.record(1);
    }
  }

  private async doWarmup(warmupEndTime: number, workerIterationLengthMs: number) {
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
      iterations: this.iterationCount,
      requests: totalRequestCount,
      errorIterations: this.errorIterations,
      missedIterations: this.missedIterations,
      failedIterationsRatio:
        (this.errorIterations + this.missedIterations) / (this.iterationCount + this.missedIterations),
      iterationsPerSecondOverall: (this.iterationCount * 1_000) / this.benchmarkDurationMs,
      iterationsPerSecondPerWorker: (this.iterationCount * 1_000) / this.concurrency / this.benchmarkDurationMs,
      requestsPerSecondOverall: (totalRequestCount * 1_000) / this.benchmarkDurationMs,
      targetArrivalRateRatio: (totalRequestCount * 1_000) / this.benchmarkDurationMs / this.targetRps,
      requestLatencyStatsMillis: {
        avg: this.requestLatencyMicros.mean / 1_000,
        p0: this.requestLatencyMicros.min / 1_000,
        p25: this.requestLatencyMicros.percentile(25) / 1_000,
        p50: this.requestLatencyMicros.percentile(50) / 1_000,
        p75: this.requestLatencyMicros.percentile(75) / 1_000,
        p90: this.requestLatencyMicros.percentile(90) / 1_000,
        p95: this.requestLatencyMicros.percentile(95) / 1_000,
        p99: this.requestLatencyMicros.percentile(99) / 1_000,
        p99_9: this.requestLatencyMicros.percentile(99.9) / 1_000,
        p100: this.requestLatencyMicros.max / 1_000,
      },
      serviceTimeStatsMillis: {
        avg: this.serviceTimeMicros.mean / 1_000,
        p0: this.serviceTimeMicros.min / 1_000,
        p25: this.serviceTimeMicros.percentile(25) / 1_000,
        p50: this.serviceTimeMicros.percentile(50) / 1_000,
        p75: this.serviceTimeMicros.percentile(75) / 1_000,
        p90: this.serviceTimeMicros.percentile(90) / 1_000,
        p95: this.serviceTimeMicros.percentile(95) / 1_000,
        p99: this.serviceTimeMicros.percentile(99) / 1_000,
        p99_9: this.serviceTimeMicros.percentile(99.9) / 1_000,
        p100: this.serviceTimeMicros.max / 1_000,
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
