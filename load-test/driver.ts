import { performance } from "perf_hooks";

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

  constructor(concurrency: number, arrivalRate: number, duration: number, test: Test) {
    this.concurrency = concurrency;
    this.arrivalRate = arrivalRate;
    this.duration = duration;
    this.test = test;
  }

  async run(): Promise<void> {
    await this.test.setup();

    const intervalMs = (1000 * this.concurrency) / this.arrivalRate;
    const startTime = performance.now();
    const endTime = startTime + this.duration * 1000;

    const runTest = async () => {
      await this.test.request();
    };

    const workers: Array<Promise<void>> = [];

    const concurrentWorkerLoop = async () => {
      await sleep(Math.random() * intervalMs); // startup jitter

      let iterationStart = performance.now();
      do {
        await runTest();
        const iterationEnd = performance.now();
        const iterationDuration = iterationEnd - iterationStart;
        if (iterationDuration < intervalMs) {
          await sleep(intervalMs - iterationDuration);
        }
        iterationStart = iterationEnd;
      } while (iterationStart < endTime);
    };

    for (let i = 0; i < this.concurrency; i++) {
      workers.push(concurrentWorkerLoop());
    }

    await Promise.all(workers);
    await this.stop();
  }

  async stop(): Promise<void> {
    await this.test.teardown();
  }
}

const test: Test = {
  async setup() {},

  async teardown() {
    process.stdout.write("\n");
  },

  async request() {
    sleep(10);
    process.stdout.write(".");
  },
};

async function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const concurrency = 10;
const arrivalRate = 10; // requests per second
const duration = 5; // seconds

const loadTest = new LoadTestDriver(concurrency, arrivalRate, duration, test);
loadTest.run();
