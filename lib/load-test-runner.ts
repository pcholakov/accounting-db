import assert from "assert";
import { createHistogram, performance, RecordableHistogram } from "perf_hooks";
import { Configuration, createMetricsLogger, MetricsLogger, StorageResolution, Unit } from "aws-embedded-metrics";

export interface TestMetadata {
  /**
   * The test name will be used as a metric dimension for reporting.
   */
  name: string;
}

export interface Test {
  metadata(): TestMetadata;

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
  metadata() {
    return {
      name: this.constructor.name,
    };
  }

  async setup(): Promise<void> {
  }

  async teardown(): Promise<void> {
  }

  abstract performIteration(): Promise<void>;

  requestsPerIteration() {
    return 1;
  }

  testRunData(): object {
    return {};
  }
}

export const METRIC_NAMESPACE = "AccountingDB";
Configuration.namespace = METRIC_NAMESPACE;

export type MetricNames = "Success" // overall success (1) / failure (0) of a single batch
  | "Latency" // elapsed time from batch arrival time to commit
  | "ServiceTime" // elapsed time from batch processing start time to commit
  | "BatchSize"; // number of successfully processed items in a single batch; not emitted for failed transactions

const METRICS_RESOLUTION = StorageResolution.High;

export class LoadTestDriver {
  private readonly concurrency: number;
  private readonly targetRps: number;
  private readonly workerCycleTimeMs: number;
  private readonly arrivalIntervalTimeMs: number;
  private readonly test: Test;
  private readonly name: string;
  private readonly overallDurationMs: number;
  private readonly warmupDurationMs: number;
  private readonly timeoutValueMs: number;
  private completedIterationsCount: number = 0;
  private scheduledIterationsCount: number = 0;
  // The work queue is a list of timestamps representing the "arrival" time of requests.
  private workQueue: number[] = [];
  private requestCount: number = 0;
  private errorIterations: number = 0;
  private missedIterations: number = 0;
  private readonly benchmarkDurationMs: number;
  private readonly requestsPerIteration: number;
  private workerRunTime: number = 0;
  private workerBackoffTime: number = 0;
  private workerBehindScheduleTime: number = 0;
  // We track metrics internally, and optionally post them to CloudWatch. The latter is great for distributed use.
  private readonly requestLatencyMicros: RecordableHistogram;
  private readonly serviceTimeMicros: RecordableHistogram;
  private readonly metrics: MetricsLogger;

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
    this.arrivalIntervalTimeMs = 1000 / (this.targetRps / this.requestsPerIteration);
    this.warmupDurationMs = opts.skipWarmup ?? false ? 0 : Math.min(this.overallDurationMs / 10, 10_000); // 10% of duration or 10s, whichever is smaller
    this.timeoutValueMs = opts.timeoutValueMs ?? this.workerCycleTimeMs; // default to the worker cycle time; it's the fastest we could reasonably react to a missed request
    this.benchmarkDurationMs = this.overallDurationMs - this.warmupDurationMs;
    this.test = test;
    this.requestLatencyMicros = createHistogram();
    this.serviceTimeMicros = createHistogram();
    this.metrics = createMetricsLogger();
    this.name = test.metadata().name;
  }

  async run(): Promise<any> {
    if (this.targetRps <= 0) {
      return;
    }

    await this.test.setup();

    const startTime = performance.now();
    const measurementStartTime = startTime + this.warmupDurationMs;
    const endTime = startTime + this.overallDurationMs;

    const tasks: Array<Promise<void>> = [];

    const requestSchedulerLoop = async () => {
      let nextRequestTime = startTime;

      while (nextRequestTime < endTime) {
        const cutoffTime = performance.now() - this.timeoutValueMs;
        // Prune expired-in-queue requests from the work queue and record timeouts:
        while (this.workQueue.length > 0 && this.workQueue[0] < cutoffTime) {
          assert(this.workQueue.shift() !== undefined);
          this.metrics.putDimensions({ Name: this.name });
          this.metrics.putMetric($m("Latency"), this.timeoutValueMs, Unit.Milliseconds, METRICS_RESOLUTION);
          this.metrics.putMetric($m("Success"), 0, Unit.None, METRICS_RESOLUTION);
          this.missedIterations += 1;
          this.recordDuration(this.requestLatencyMicros, this.timeoutValueMs * 1000);
        }
        await this.metrics.flush();

        while (this.workQueue.length < this.concurrency * 2 && nextRequestTime < endTime) {
          this.workQueue.push(nextRequestTime);
          this.scheduledIterationsCount += 1;
          nextRequestTime += this.arrivalIntervalTimeMs;
        }

        await sleep(this.arrivalIntervalTimeMs / 2);
      }
    };

    const concurrentWorkerLoop = async () => {
      do {
        const workerLoopStart = performance.now();
        if (workerLoopStart > endTime) break; // Do not start any new work after the scheduled run end time

        // Skip over any scheduled iterations that have already timed out in-queue
        const cutoffTime = workerLoopStart - this.timeoutValueMs;
        let arrivalTime = this.workQueue.shift();
        this.metrics.putDimensions({ Name: this.name });
        for (; arrivalTime !== undefined && arrivalTime < cutoffTime; arrivalTime = this.workQueue.shift()) {
          // Only record timeouts post-warmup
          if (arrivalTime > measurementStartTime) {
            this.missedIterations += 1;
            this.recordDuration(this.requestLatencyMicros, Math.round(this.timeoutValueMs * 1000));
            this.metrics.putMetric($m("Latency"), this.timeoutValueMs, Unit.Milliseconds, METRICS_RESOLUTION);
            this.metrics.putMetric($m("Success"), 0, Unit.None, METRICS_RESOLUTION);
            arrivalTime = this.workQueue.shift();
          }
        }
        await this.metrics.flush();

        // No more work for this worker to do
        if (arrivalTime === undefined) {
          await sleep(0); // Yield to other tasks
          continue;
        }

        // Treat the intended request start time as the start of the measurement iteration
        let iterationStart = arrivalTime;
        const backoffTimeMillis = arrivalTime - workerLoopStart;

        // Are we ahead of schedule?
        if (backoffTimeMillis > 0) {
          this.workerBackoffTime += backoffTimeMillis;
          if (backoffTimeMillis >= 1.0) {
            await sleep(Math.floor(backoffTimeMillis));
          }
          while ((iterationStart = performance.now()) < arrivalTime) {
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
        if (arrivalTime >= measurementStartTime) {
          this.recordDuration(this.requestLatencyMicros, Math.round(iterationDurationMillis * 1000));
          this.recordDuration(this.serviceTimeMicros, Math.round(serviceTimeMillis * 1000));
          this.completedIterationsCount += 1;
          this.requestCount += this.requestsPerIteration;
          this.workerRunTime += serviceTimeMillis;

          this.metrics.putDimensions({ Name: this.name });
          this.metrics.putMetric($m("Latency"), iterationDurationMillis, Unit.Milliseconds, METRICS_RESOLUTION);
          this.metrics.putMetric($m("ServiceTime"), serviceTimeMillis, Unit.Milliseconds, METRICS_RESOLUTION);
          this.metrics.putMetric($m("BatchSize"), this.requestsPerIteration, Unit.Count, METRICS_RESOLUTION);
          this.metrics.putMetric($m("Success"), 1, Unit.None, METRICS_RESOLUTION);
          await this.metrics.flush();
        }
      } while (true);
    };

    tasks.push(requestSchedulerLoop());
    for (let i = 0; i < this.concurrency; i++) {
      tasks.push(concurrentWorkerLoop());
    }

    await Promise.all(tasks);
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

  async stop(): Promise<any> {
    await this.test.teardown();

    return {
      configuration: {
        targetArrivalRate: this.targetRps,
        concurrency: this.concurrency,
        overallDurationMillis: this.overallDurationMs,
        warmupMillis: this.warmupDurationMs,
        requestTimeoutMillis: this.timeoutValueMs,
      },
      testRunData: this.test.testRunData(),
      completedIterations: this.completedIterationsCount,
      missedIterations: this.missedIterations,
      errorIterations: this.errorIterations,
      failedIterationsRatio:
        (this.errorIterations + this.missedIterations) / (this.completedIterationsCount + this.missedIterations),
      workerCycleTimeMillis: this.workerCycleTimeMs,
      totalRequestsCompleted: this.requestCount,
      throughputOverall: (this.requestCount * 1_000) / this.benchmarkDurationMs,
      iterationsPerSecondPerWorker:
        (this.completedIterationsCount * 1_000) / this.concurrency / this.benchmarkDurationMs,
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
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function $m(metric: MetricNames) {
  return metric;
}
