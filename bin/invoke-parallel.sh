#!/bin/zsh
set -eu -o pipefail

script_dir=$(dirname "$0")

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <workers>"
    exit 1
fi

workers=$1

if ! [[ $workers =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: <workers> must be an integer"
    exit 1
fi

date
echo "Running $workers tests in parallel..."
for _ in $(seq 1 $workers); do
    echo "Starting worker..."
    ${script_dir}/invoke-benchmark.sh &
done

date
echo "Waiting for workers to finish..."
wait
date
