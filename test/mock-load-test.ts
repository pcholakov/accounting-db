import { AbstractBaseTest, LoadTestDriver, sleep } from "../lib/load-test-runner.js";

class MockTest extends AbstractBaseTest {
  async teardown() {
    process.stdout.write("\n");
  }

  async performIteration() {
    await sleep(10);
    process.stdout.write(".");
  }
}

const concurrency = 10;
const arrivalRate = 100; // requests per second
const durationSeconds = 3;

const loadTest = new LoadTestDriver(new MockTest(), {
  concurrency,
  targetRequestRatePerSecond: arrivalRate,
  durationSeconds,
});
console.log(await loadTest.run());
