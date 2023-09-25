import { createHistogram, Histogram, PerformanceObserver, performance, RecordableHistogram } from "perf_hooks";

export interface Test {
  setup(): Promise<void>;
  teardown(): Promise<void>;
  request(): Promise<void>;
}

export class LoadTestDriver {
  private concurrency: number;
  private arrivalRate: number;
  private test: Test;
  private duration: number;
  private _histogram: RecordableHistogram;
  private _requestCount: number = 0;

  constructor(concurrency: number, arrivalRate: number, duration: number, test: Test) {
    this.concurrency = concurrency;
    this.arrivalRate = arrivalRate;
    this.duration = duration;
    this.test = test;
    this._histogram = createHistogram();
  }

  async run(): Promise<void> {
    await this.test.setup();

    const intervalMs = (1000 * this.concurrency) / this.arrivalRate;
    const startTime = performance.now();
    const endTime = startTime + this.duration * 1000;

    const workers: Array<Promise<void>> = [];

    const concurrentWorkerLoop = async (id: string) => {
      await sleep(Math.random() * intervalMs); // startup jitter

      do {
        const iterationStart = performance.now();
        await this.test.request();
        const iterationEnd = performance.now();

        const iterationDuration = iterationEnd - iterationStart;
        this._requestCount++;
        const durationInt = Math.floor(iterationDuration);
        if (durationInt > 0) {
          this._histogram.record(durationInt);
        }

        if (iterationDuration < intervalMs) {
          await sleep(intervalMs - iterationDuration);
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
      count: this.requestCount(),
      throughput: this.requestCount() / this.duration,
      arrivalRateRatio: this.requestCount() / this.duration / this.arrivalRate,
      avg: this.histogram().mean,
      min: this.histogram().min,
      max: this.histogram().max,
      p50: this.histogram().percentile(50),
      p75: this.histogram().percentile(75),
      p90: this.histogram().percentile(90),
      p95: this.histogram().percentile(95),
      p99: this.histogram().percentile(99),
      p999: this.histogram().percentile(99.9),
    });
  }

  requestCount() {
    return this._requestCount;
  }

  histogram(): Histogram {
    return this._histogram;
  }
}

export async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
