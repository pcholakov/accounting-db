# AccountingDB: A high-performance financial ledger based on DynamoDB

This project implements a financial tracking component that heavily leans on AWS components. At the core, durable stage
storage is provided by DynamoDB. Other aspects of the solution are implemented by maximally relying on a serverless
approach in order to achieve extremely low human operational cost.

## Operations

Transfers are transactional debits/credits between pairs of accounts. You specify a credit and debit account, and an
amount, and the system will accept or reject the transfer depending on which logic rules are active.

## Deploying the stack

The stack creates a single on-demand billing DynamoDB table and the benchmark runner Lambda Function. These have no idle
cost if left running. Depending on how many other dashboards you have in your account, the benchmark dashboard may
exceed your free tier allowance and attract a charge. Data stored in DynamoDB will be billed according to the

```shell
npm run deploy
eval $(./bin/get-benchmark-function-name.sh)
```

### Read benchmark and sparse account balances

Note that for the read workload to provide representative data, you will want to ensure that account entries exist for
all accounts in the range. We don't yet have a mechanism to fill these in. For accounts that do not exist, the cost of
returning "not found" may be different from the cost of performing a balance read; hence be careful when setting up the
read benchmark.

## Running load tests

The scripts `invoke-benchmark.sh` and `invoke-parallel.sh` can be used to start 1 or N parallel runs with the
configuration defined in `benchmark-request.json`. When using parallel benchmarks, the aggregations returned by any of
the runners only capture the data of that particular runner; instead look to the CloudWatch Dashboard for the aggregated
statistics.

Note that with high-resolution metrics, you only have three hours to see the second-level resolution data.