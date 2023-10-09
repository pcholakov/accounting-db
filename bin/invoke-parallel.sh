#!/bin/zsh
set -eu -o pipefail

script_dir=$(dirname "$0")

for _ in {1..10}; do
    ${script_dir}/invoke-benchmark.sh &
done

wait
