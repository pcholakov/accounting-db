import { setupAccounts } from "../lib/lambda/create-accounts.js";

async function main() {
  setupAccounts(10_000);
}

main();
