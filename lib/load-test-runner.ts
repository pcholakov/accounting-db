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
  private readonly concurrency: number;
  private readonly targetRps: number;
  private readonly workerCycleTimeMs: number;
  private readonly test: Test;
  private readonly overallDurationMs: number;
  private readonly warmupDurationMs: number;
  private readonly requestLatencyMicros: RecordableHistogram;
  private readonly serviceTimeMicros: RecordableHistogram;
  private readonly timeoutValueMs: number;
  private iterationCount: number = 0;
  private scheduleIterationCount: number = 0;
  private workQueue: number[] = [];
  private requestCount: number = 0;
  private errorIterations: number = 0;
  private missedIterations: number = 0;
  private benchmarkDurationMs: number;
  private readonly requestsPerIteration: number;
  private workerRunTime: number = 0;
  private workerBackoffTime: number = 0;
  private workerBehindScheduleTime: number = 0;

  constructor(
    test: Test,
    opts: {
      concurrency: number;
      targetRequestRatePerSecond: number;
      durationSeconds: number;
      /**
       * The request latency value to record for missed iterations, in milliseconds.
       */
      timeoutValueMs?: number;
      skipWarmup?: boolean;
    },
  ) {
    this.concurrency = opts.concurrency;
    this.targetRps = opts.targetRequestRatePerSecond;
    this.overallDurationMs = opts.durationSeconds * 1_000;
    this.requestsPerIteration = test.requestsPerIteration();
    this.workerCycleTimeMs = (1000 * this.concurrency) / (this.targetRps / this.requestsPerIteration);
    this.warmupDurationMs = opts.skipWarmup ?? false ? 0 : Math.min(this.overallDurationMs / 10, 10_000); // 10% of duration or 10s, whichever is smaller
    this.timeoutValueMs = opts.timeoutValueMs ?? this.workerCycleTimeMs; // default to the worker cycle time; it's the fastest we could reasonably react to a missed request
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

    const startTime = performance.now();
    const measurementStartTime = startTime + this.warmupDurationMs;
    const endTime = startTime + this.overallDurationMs;

    const workers: Array<Promise<void>> = [];

    const concurrentWorkerLoop = async (id: string) => {
      // Jitter startup and perform the warmup iterations without measuring
      await sleep(
        Math.round(
          Math.random() *
            (this.warmupDurationMs > 2 * this.workerCycleTimeMs ? this.warmupDurationMs / 2 : this.workerCycleTimeMs),
        ),
      );
      await this.doWarmup(measurementStartTime, this.workerCycleTimeMs);

      const workStartTime = performance.now();
      const workArrivalSchedule = setInterval(() => {
        let nextRequestTime = workStartTime + this.scheduleIterationCount * this.workerCycleTimeMs;
        while (this.workQueue.length < 100) {
          this.scheduleIterationCount += 1;
          this.workQueue.push(nextRequestTime);
          nextRequestTime += this.workerCycleTimeMs;
        }
      }, this.workerCycleTimeMs);

      do {
        while (this.workQueue.length == 0) {
          await sleep(0); // yield to the control loop
        }
        const workerLoopStart = performance.now();

        // If we're finished, drain the remaining requests from the queue and record their latencies (if applicable)
        if (workerLoopStart > endTime) {
          this.workQueue.forEach((arrivalTime) => {
            if (arrivalTime < endTime - this.workerCycleTimeMs) {
              this.missedIterations += 1;
              this.recordDuration(
                this.requestLatencyMicros,
                Math.round(Math.max(workerLoopStart - arrivalTime, this.timeoutValueMs) * 1000),
              );
            } else {
              this.scheduleIterationCount -= 1;
            }
          });
          break;
        }

        let arrivalTime = this.workQueue.shift();
        // skip over any "timed out in queue" requests
        while (arrivalTime !== undefined && workerLoopStart - arrivalTime > this.timeoutValueMs) {
          this.missedIterations += 1;
          this.recordDuration(
            this.requestLatencyMicros,
            Math.round(Math.max(workerLoopStart - arrivalTime, this.timeoutValueMs) * 1000),
          );
          arrivalTime = this.workQueue.shift();
        }

        if (arrivalTime === undefined) continue; // Only timed out requests in the queue, no real work left to do
        const iterationStart = arrivalTime; // Treat the intended request start time as the start of the measurement iteration

        const backoffTimeMillis = arrivalTime - workerLoopStart;
        // Are we ahead of schedule?
        if (backoffTimeMillis > 0) {
          this.workerBackoffTime += backoffTimeMillis;
          if (backoffTimeMillis >= 1.0) {
            await sleep(Math.floor(backoffTimeMillis));
          }
          while (performance.now() < arrivalTime) {
            await sleep(0);
          }
        } else {
          this.workerBehindScheduleTime += -backoffTimeMillis;
        }

        const requestStart = performance.now();
        try {
          await this.test.performIteration();
        } catch (err) {
          // Log a sample of error messages to aid in debugging user test code
          // if (this.errorIterations % 1000 == 0) {
          //   console.error(`Worker ${id} failed iteration ${this.iterationCount}`, err);
          // }
          this.errorIterations += 1;
        }

        const iterationCompleted = performance.now();
        const iterationDurationMillis = iterationCompleted - iterationStart;
        const serviceTimeMillis = iterationCompleted - requestStart;
        this.recordDuration(this.requestLatencyMicros, Math.round(iterationDurationMillis * 1000));
        this.recordDuration(this.serviceTimeMicros, Math.round(serviceTimeMillis * 1000));
        this.iterationCount += 1;
        this.requestCount += this.requestsPerIteration;
        this.workerRunTime += serviceTimeMillis;
      } while (true);

      clearInterval(workArrivalSchedule);
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
      } catch (err) {
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

    return {
      configuration: {
        targetArrivalRate: this.targetRps,
        concurrency: this.concurrency,
        duration: this.overallDurationMs,
        warmup: this.warmupDurationMs,
      },
      testRunData: this.test.testRunData(),
      targetIterations: this.scheduleIterationCount,
      completedIterations: this.iterationCount,
      missedIterations: this.missedIterations,
      errorIterations: this.errorIterations,
      failedIterationsRatio:
        (this.errorIterations + this.missedIterations) / (this.iterationCount + this.missedIterations),
      workerCycleTimeMillis: this.workerCycleTimeMs,
      totalRequestsCompleted: this.requestCount,
      throughputOverall: (this.requestCount * 1_000) / this.benchmarkDurationMs,
      iterationsPerSecondPerWorker: (this.iterationCount * 1_000) / this.concurrency / this.benchmarkDurationMs,
      targetArrivalRateRatio: (this.requestCount * 1_000) / this.benchmarkDurationMs / this.targetRps,
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
