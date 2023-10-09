#!/bin/zsh
set -eu -o pipefail

script_dir=$(dirname "$0")
output=$(mktemp -t output-$(date +%s)-XXXXXXXXXX.json)

aws lambda invoke \
    --function-name ${BENCHMARK_FUNCTION} \
    --payload fileb://${script_dir}/benchmark-request.json \
    --output json ${output}
jq . ${output}

final_output=output-$(jq -r '.startTime + "_" + .requestId' ${output}).json
mv ${output} ${final_output}
