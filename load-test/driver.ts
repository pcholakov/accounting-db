import { createHistogram, Histogram, PerformanceObserver, performance, RecordableHistogram } from "perf_hooks";

interface Test {
  setup(): Promise<void>;
  teardown(): Promise<void>;
  request(): Promise<void>;
}

class LoadTestDriver {
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
  }

  requestCount() {
    return this._requestCount;
  }

  histogram(): Histogram {
    return this._histogram;
  }
}

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const test: Test = {
  async setup() {},

  async teardown() {
    process.stdout.write("\n");
  },

  async request() {
    await sleep(10);
    process.stdout.write(".");
  },
};

const concurrency = 10;
const arrivalRate = 100; // requests per second
const duration = 3; // seconds

const loadTest = new LoadTestDriver(concurrency, arrivalRate, duration, test);
await loadTest.run();

console.log({
  count: loadTest.requestCount(),
  throughput: loadTest.requestCount() / duration,
  arrivalRateRatio: loadTest.requestCount() / duration / arrivalRate,
  avg: loadTest.histogram().mean,
  min: loadTest.histogram().min,
  max: loadTest.histogram().max,
  p50: loadTest.histogram().percentile(50),
  p75: loadTest.histogram().percentile(75),
  p90: loadTest.histogram().percentile(90),
  p95: loadTest.histogram().percentile(95),
  p99: loadTest.histogram().percentile(99),
  p999: loadTest.histogram().percentile(99.9),
});
console.log();
