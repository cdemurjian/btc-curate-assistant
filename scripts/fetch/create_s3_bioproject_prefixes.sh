#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  create_s3_bioproject_prefixes.sh

Environment overrides:
  AWS_PROFILE    Optional AWS profile for aws cli commands
EOF
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

BUCKET="btc-osteo"
BASE_PREFIX="staging"
PROJECTS=(
  "PRJNA1368480"
  "PRJNA1367604"
  "PRJNA1374203"
)

AWS_ARGS=()
if [[ -n "${AWS_PROFILE:-}" ]]; then
  AWS_ARGS+=(--profile "$AWS_PROFILE")
fi

for project in "${PROJECTS[@]}"; do
  aws "${AWS_ARGS[@]}" s3api put-object \
    --bucket "$BUCKET" \
    --key "${BASE_PREFIX}/${project}/" \
    >/dev/null
  printf 'Created s3://%s/%s/%s/\n' "$BUCKET" "$BASE_PREFIX" "$project"
done
