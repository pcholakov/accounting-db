#!/bin/zsh
set -eu -o pipefail

echo "export BENCHMARK_FUNCTION=$(aws cloudformation describe-stacks \
    --stack-name "AccountingDb" \
    --query "Stacks[0].Outputs[?OutputKey=='BenchmarkFunctionName'].OutputValue" \
    --output text)"