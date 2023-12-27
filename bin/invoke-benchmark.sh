#!/bin/zsh
set -eu -o pipefail

script_dir=$(dirname "$0")
output_dir=${script_dir}/../output
mkdir -p "${output_dir}"

tmp_output=$(mktemp -t "benchmark-response-$(date +%s)-XXXXXXXXXX.json")

aws lambda invoke \
    --function-name "${BENCHMARK_FUNCTION}" \
    --payload "fileb://${script_dir}/benchmark-request.json" \
    --output json "${tmp_output}"

final_output=${output_dir}/benchmark-$(jq -r '.startTime + "_" + .requestId' "${tmp_output}").json
mv "${tmp_output}" "${final_output}"
echo "Benchmark result (saved in $(basename .../output/${final_output})):"
jq . "${final_output}"
