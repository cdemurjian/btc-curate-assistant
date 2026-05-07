#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  fetch_bioprojects_to_s3.sh s3://bucket/prefix

Environment overrides:
  AWS_PROFILE              Optional AWS profile name for aws cli commands
  THREADS                  fasterq-dump threads (default: 8)
  WORK_ROOT                Staging root for downloads (default: ./data/workspace/fetch/work)
  TMP_ROOT                 Temporary scratch for fasterq-dump (default: $WORK_ROOT/tmp)
  TOOL_ROOT                Local install root for SRA Toolkit (default: ./data/workspace/fetch/tools)
  PROJECT_FILTER           Optional comma-separated BioProject subset to run
  KEEP_LOCAL=1             Keep downloaded/converted files after upload
  DRY_RUN=1                Print planned actions without downloading or uploading

This script processes:
  PRJNA1368480 -> FASTQ (.fastq.gz)
  PRJNA1367604 -> BAM   (.bam)
  PRJNA1374203 -> BAM   (.bam)

Output layout in S3:
  s3://bucket/prefix/PRJNA1368480/<run files>
  s3://bucket/prefix/PRJNA1367604/<run files>
  s3://bucket/prefix/PRJNA1374203/<run files>
EOF
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 2
fi

S3_ROOT="$1"
THREADS="${THREADS:-8}"
WORK_ROOT="${WORK_ROOT:-$PWD/data/workspace/fetch/work}"
TMP_ROOT="${TMP_ROOT:-$WORK_ROOT/tmp}"
TOOL_ROOT="${TOOL_ROOT:-$PWD/data/workspace/fetch/tools}"
STATE_DIR="$WORK_ROOT/state"
LOG_DIR="$WORK_ROOT/logs"
DATA_ROOT="$WORK_ROOT/data"
KEEP_LOCAL="${KEEP_LOCAL:-0}"
DRY_RUN="${DRY_RUN:-0}"
PROJECT_FILTER="${PROJECT_FILTER:-}"
AWS_ARGS=()

if [[ -n "${AWS_PROFILE:-}" ]]; then
  AWS_ARGS+=(--profile "$AWS_PROFILE")
fi

PROJECTS=(
  "PRJNA1368480 fastq"
  "PRJNA1367604 bam"
  "PRJNA1374203 bam"
)

mkdir -p "$WORK_ROOT" "$TMP_ROOT" "$TOOL_ROOT" "$STATE_DIR" "$LOG_DIR" "$DATA_ROOT"

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*" >&2
}

die() {
  log "ERROR: $*"
  exit 1
}

run_cmd() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf 'DRY_RUN:'
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

ensure_base_tools() {
  need_cmd aws
  need_cmd curl
  need_cmd gzip
  need_cmd samtools
}

extract_original_bam_metadata() {
  local run="$1"
  local xml

  xml="$(curl -fsSL "https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/run_new?acc=${run}")" || return 1

  printf '%s\n' "$xml" \
    | sed 's/></>\
</g' \
    | awk '
      /<SRAFile / && /semantic_name="bam"/ && /supertype="Original"/ {
        in_bam = 1
        if (match($0, /filename="[^"]+"/)) {
          filename = substr($0, RSTART + 10, RLENGTH - 11)
        }
        next
      }
      in_bam && /<Alternatives / && /url="s3:\/\// {
        if (match($0, /url="[^"]+"/)) {
          url = substr($0, RSTART + 5, RLENGTH - 6)
          print filename "\t" url
          exit
        }
      }
      /<\/SRAFile>/ {
        in_bam = 0
        filename = ""
      }
    '
}

download_original_bam() {
  local bam_url="$1"
  local bam_path="$2"

  run_cmd aws s3 cp --no-sign-request "$bam_url" "$bam_path" --only-show-errors
}

ensure_aws_auth() {
  if ! aws "${AWS_ARGS[@]}" sts get-caller-identity >/dev/null 2>&1; then
    die "AWS credentials are not active. Run 'aws sso login${AWS_PROFILE:+ --profile $AWS_PROFILE}' and retry."
  fi
}

install_sra_toolkit() {
  local url archive bindir
  if command -v prefetch >/dev/null 2>&1 && command -v fasterq-dump >/dev/null 2>&1 && command -v sam-dump >/dev/null 2>&1; then
    return 0
  fi

  url="https://ftp.ncbi.nlm.nih.gov/sra/sdk/current/sratoolkit.current-ubuntu64.tar.gz"
  archive="$TOOL_ROOT/sratoolkit.current-ubuntu64.tar.gz"

  if [[ ! -f "$archive" ]]; then
    log "Downloading SRA Toolkit"
    run_cmd curl -fL "$url" -o "$archive"
  fi

  if [[ ! -L "$TOOL_ROOT/sratoolkit.3.4.1-ubuntu64/bin/prefetch" ]] && ! find -L "$TOOL_ROOT" -maxdepth 5 \( -name prefetch -o -name prefetch.3 -o -name prefetch.3.4.1 \) | grep -q .; then
    log "Extracting SRA Toolkit"
    run_cmd tar -xzf "$archive" -C "$TOOL_ROOT"
  fi

  bindir="$(find -L "$TOOL_ROOT" -maxdepth 5 \( -name prefetch -o -name prefetch.3 -o -name prefetch.3.4.1 \) -printf '%h\n' | head -n 1)"
  [[ -n "$bindir" ]] || die "Could not find prefetch after extracting SRA Toolkit"
  export PATH="$bindir:$PATH"

  need_cmd prefetch
  need_cmd fasterq-dump
  need_cmd sam-dump
  if command -v vdb-config >/dev/null 2>&1; then
    run_cmd vdb-config --prefetch-to-cwd >/dev/null 2>&1 || true
  fi
}

fetch_run_list() {
  local project="$1"
  curl -fsSL "https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/runinfo?acc=${project}" \
    | awk -F, 'NR>1 && $1 != "" { print $1 }'
}

gzip_fastqs() {
  local outdir="$1"
  local f
  shopt -s nullglob
  for f in "$outdir"/*.fastq; do
    log "Compressing $(basename "$f")"
    run_cmd gzip -f "$f"
  done
  shopt -u nullglob
}

upload_outputs() {
  local src_dir="$1"
  local project="$2"
  local s3_dest="${S3_ROOT%/}/${project}/"
  local f

  shopt -s nullglob
  for f in "$src_dir"/*; do
    [[ -f "$f" ]] || continue
    log "Uploading $(basename "$f") to ${s3_dest}"
    run_cmd aws "${AWS_ARGS[@]}" s3 cp "$f" "$s3_dest" --only-show-errors
  done
  shopt -u nullglob
}

cleanup_run() {
  local dir="$1"
  if [[ "$KEEP_LOCAL" == "1" ]]; then
    return 0
  fi
  run_cmd rm -rf "$dir"
}

mark_done() {
  local state_file="$1"
  local run="$2"
  printf '%s\n' "$run" >>"$state_file"
}

already_done() {
  local state_file="$1"
  local run="$2"
  [[ -f "$state_file" ]] && grep -Fxq "$run" "$state_file"
}

project_selected() {
  local project="$1"
  [[ -z "$PROJECT_FILTER" ]] && return 0
  printf '%s\n' "$PROJECT_FILTER" | tr ',' '\n' | grep -Fxq "$project"
}

download_fastq_run() {
  local project="$1"
  local run="$2"
  local run_dir="$DATA_ROOT/$project/$run"
  mkdir -p "$run_dir"

  log "Prefetching $run"
  run_cmd prefetch --progress --max-size u -O "$run_dir" "$run"

  log "Converting $run to FASTQ"
  run_cmd fasterq-dump \
    --threads "$THREADS" \
    --split-files \
    --temp "$TMP_ROOT" \
    -O "$run_dir" \
    "$run_dir/$run"

  gzip_fastqs "$run_dir"
  upload_outputs "$run_dir" "$project"
  cleanup_run "$run_dir"
}

download_bam_run() {
  local project="$1"
  local run="$2"
  local run_dir="$DATA_ROOT/$project/$run"
  local bam_name bam_url bam_path bam_metadata
  mkdir -p "$run_dir"

  bam_metadata="$(extract_original_bam_metadata "$run" || true)"
  if [[ -n "$bam_metadata" ]]; then
    IFS=$'\t' read -r bam_name bam_url <<<"$bam_metadata"
    bam_path="$run_dir/$bam_name"
    log "Downloading original BAM for $run from SRA source bucket"
    download_original_bam "$bam_url" "$bam_path"
  else
    log "Original BAM not available for $run; falling back to sam-dump conversion"
    log "Prefetching $run"
    run_cmd prefetch --progress --max-size u -O "$run_dir" "$run"

    log "Streaming $run to BAM"
    if [[ "$DRY_RUN" == "1" ]]; then
      printf 'DRY_RUN: sam-dump --header %q | samtools view -b -o %q -\n' "$run_dir/$run" "$run_dir/${run}.bam"
    else
      sam-dump --header "$run_dir/$run" | samtools view -b -o "$run_dir/${run}.bam" -
    fi
  fi

  upload_outputs "$run_dir" "$project"
  cleanup_run "$run_dir"
}

process_project() {
  local project="$1"
  local mode="$2"
  local state_file="$STATE_DIR/${project}.done"
  local project_log="$LOG_DIR/${project}.log"
  local run

  mkdir -p "$DATA_ROOT/$project"
  log "Enumerating runs for $project"

  while IFS= read -r run; do
    [[ -n "$run" ]] || continue
    if already_done "$state_file" "$run"; then
      log "Skipping completed run $run"
      continue
    fi

    {
      log "Starting $project $run ($mode)"
      case "$mode" in
        fastq) download_fastq_run "$project" "$run" ;;
        bam) download_bam_run "$project" "$run" ;;
        *) die "Unsupported mode: $mode" ;;
      esac
      mark_done "$state_file" "$run"
      log "Finished $project $run"
    } 2>&1 | tee -a "$project_log"
  done < <(fetch_run_list "$project")
}

main() {
  ensure_base_tools
  install_sra_toolkit
  ensure_aws_auth

  local item project mode
  for item in "${PROJECTS[@]}"; do
    project="${item%% *}"
    mode="${item##* }"
    if ! project_selected "$project"; then
      log "Skipping $project because it is not in PROJECT_FILTER"
      continue
    fi
    process_project "$project" "$mode"
  done

  log "All projects completed"
}

main
