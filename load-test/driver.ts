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
  private _durationMicrosHistogram: RecordableHistogram;
  private _requestCount: number = 0;
  private _errorCount: number = 0;
  private benchmarkDurationMs: number;

  constructor(concurrency: number, arrivalRate: number, durationSeconds: number, test: Test) {
    this.concurrency = concurrency;
    this.arrivalRate = arrivalRate;
    this.overallDurationMs = durationSeconds * 1_000;
    this.warmupDurationMs = Math.min(this.overallDurationMs / 10, 10_000); // 10% of duration or 10s, whichever is smaller
    this.benchmarkDurationMs = this.overallDurationMs - this.warmupDurationMs;
    this.test = test;
    this._durationMicrosHistogram = createHistogram();
  }

  async run(): Promise<void> {
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
          this._errorCount++;
        }
        const iterationEnd = performance.now();

        const iterationDurationMicros = (iterationEnd - iterationStart) * 1_000;
        this._requestCount++;
        const durationInt = Math.floor(iterationDurationMicros);
        if (durationInt > 0) {
          // TODO: surface zero durations to the user
          this._durationMicrosHistogram.record(durationInt);
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
    await this.stop();
  }

  async stop(): Promise<void> {
    await this.test.teardown();

    console.log({
      configuration: {
        concurrency: this.concurrency,
        targetArrivalRate: this.arrivalRate,
        duration: this.overallDurationMs,
        warmup: this.warmupDurationMs,
      },
      requestCount: this.requestCount(),
      errorCount: this.errorCount(),
      errorRate: this.errorCount() / this.requestCount(),
      throughput: (this.requestCount() * 1_000) / this.benchmarkDurationMs,
      arrivalRateRatio: (this.requestCount() * 1_000) / this.benchmarkDurationMs / this.arrivalRate,
      durationsMillis: {
        avg: this.histogram().mean / 1_000,
        p0: this.histogram().min / 1_000,
        p50: this.histogram().percentile(50) / 1_000,
        p75: this.histogram().percentile(75) / 1_000,
        p90: this.histogram().percentile(90) / 1_000,
        p95: this.histogram().percentile(95) / 1_000,
        p99: this.histogram().percentile(99) / 1_000,
        p99_9: this.histogram().percentile(99.9) / 1_000,
        p100: this.histogram().max / 1_000,
      },
    });
  }

  requestCount() {
    return this._requestCount;
  }

  errorCount() {
    return this._errorCount;
  }

  histogram(): Histogram {
    return this._durationMicrosHistogram;
  }
}

export async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
