#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  generate_bioproject_manifest.sh [output_json]

Defaults:
  output_json -> manifests/bioprojects/default.json
EOF
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

OUT_JSON="${1:-$PWD/manifests/bioprojects/default.json}"

PROJECTS=(
  "PRJNA1368480|fastq|s3://btc-osteo/staging/PRJNA1368480/"
  "PRJNA1367604|bam|s3://btc-osteo/staging/PRJNA1367604/"
  "PRJNA1374203|bam|s3://btc-osteo/staging/PRJNA1374203/"
)

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'Missing required command: %s\n' "$1" >&2
    exit 1
  }
}

need_cmd curl
need_cmd jq

mkdir -p "$(dirname "$OUT_JSON")"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

json_items=()

for spec in "${PROJECTS[@]}"; do
  IFS='|' read -r bioproject data_type destination_folder <<<"$spec"
  run_txt="$tmpdir/${bioproject}.txt"

  curl -fsSL "https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/runinfo?acc=${bioproject}" \
    | awk -F, 'NR>1 && $1 != "" { print $1 }' \
    >"$run_txt"

  project_json="$(jq -Rn \
    --arg bioproject_accession "$bioproject" \
    --arg data_type "$data_type" \
    --arg destination_folder "$destination_folder" \
    --slurpfile run_accessions <(jq -R . <"$run_txt" | jq -s .) \
    '{
      bioproject_accession: $bioproject_accession,
      data_type: $data_type,
      destination_folder: $destination_folder,
      run_accessions: $run_accessions[0]
    }')"

  json_items+=("$project_json")
done

printf '%s\n' "${json_items[@]}" \
  | jq -s '{generated_at: now | todate, projects: .}' \
  >"$OUT_JSON"

printf 'Wrote %s\n' "$OUT_JSON"
