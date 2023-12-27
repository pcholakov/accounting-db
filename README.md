# AccountingDB: A high-performance financial ledger based on DynamoDB

This project implements a financial tracking component that heavily leans on AWS
components. At the core, durable stage storage is provided by DynamoDB. Other
aspects of the solution are implemented by maximally relying on a serverless
approach in order to achieve extremely low human operational cost.

## Operations

Transfers are transactional debits/credits between pairs of accounts. You
specify a credit and debit account, and an amount, and the system will accept or
reject the transfer depending on which logic rules are active.

## Deploying a test stack

```shell
npm run deploy
eval $(./bin/get-benchmark-function-name.sh)
```
