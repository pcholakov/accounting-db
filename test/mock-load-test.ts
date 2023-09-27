import { LoadTestDriver, Test, sleep } from "../lib/load-tests.js";

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
const durationSeconds = 3;

const loadTest = new LoadTestDriver(test, { concurrency, arrivalRate, durationSeconds });
console.log(await loadTest.run());
