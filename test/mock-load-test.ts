import { LoadTestDriver, Test, sleep } from "../load-test/driver.js";

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