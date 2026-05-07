#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  fetch_from_manifest.sh /path/to/bioproject_manifest.json

Environment overrides:
  AWS_PROFILE              Optional AWS profile name for aws cli commands
  THREADS                  fasterq-dump threads per run (default: 8)
  MAX_PARALLEL_RUNS        Concurrent runs per project (default: 2)
  PIGZ_THREADS             pigz threads per file when available (default: 8)
  WORK_ROOT                Staging root (default: ./data/workspace/fetch/work)
  TMP_ROOT                 Temporary scratch root (default: $WORK_ROOT/tmp)
  TOOL_ROOT                Local SRA Toolkit install root (default: ./data/workspace/fetch/tools)
  KEEP_LOCAL=1             Keep local files after successful upload
  DRY_RUN=1                Print planned actions without executing
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

MANIFEST_PATH="$1"
THREADS="${THREADS:-8}"
MAX_PARALLEL_RUNS="${MAX_PARALLEL_RUNS:-2}"
PIGZ_THREADS="${PIGZ_THREADS:-8}"
WORK_ROOT="${WORK_ROOT:-$PWD/data/workspace/fetch/work}"
TMP_ROOT="${TMP_ROOT:-$WORK_ROOT/tmp}"
TOOL_ROOT="${TOOL_ROOT:-$PWD/data/workspace/fetch/tools}"
STATE_DIR="$WORK_ROOT/state"
LOG_DIR="$WORK_ROOT/logs"
DATA_ROOT="$WORK_ROOT/data"
CHECKSUM_DIR="$WORK_ROOT/checksums"
KEEP_LOCAL="${KEEP_LOCAL:-0}"
DRY_RUN="${DRY_RUN:-0}"
AWS_ARGS=()

if [[ -n "${AWS_PROFILE:-}" ]]; then
  AWS_ARGS+=(--profile "$AWS_PROFILE")
fi

mkdir -p "$WORK_ROOT" "$TMP_ROOT" "$TOOL_ROOT" "$STATE_DIR" "$LOG_DIR" "$DATA_ROOT" "$CHECKSUM_DIR"

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
  need_cmd jq
  need_cmd sha256sum
  need_cmd stat
  need_cmd samtools
  need_cmd tee
  if ! command -v pigz >/dev/null 2>&1; then
    need_cmd gzip
  fi
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

ensure_inputs() {
  [[ -f "$MANIFEST_PATH" ]] || die "Manifest not found: $MANIFEST_PATH"
  jq -e '.projects and (.projects | type == "array")' "$MANIFEST_PATH" >/dev/null \
    || die "Manifest is missing a valid .projects array: $MANIFEST_PATH"
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

already_done() {
  local state_file="$1"
  local run="$2"
  [[ -f "$state_file" ]] && grep -Fxq "$run" "$state_file"
}

mark_done() {
  local state_file="$1"
  local run="$2"
  printf '%s\n' "$run" >>"$state_file"
}

cleanup_run() {
  local run_dir="$1"
  local run_checksum_dir="$2"
  local run_tmp_dir="$3"
  if [[ "$KEEP_LOCAL" == "1" ]]; then
    return 0
  fi
  run_cmd rm -rf "$run_dir" "$run_checksum_dir" "$run_tmp_dir"
}

cleanup_stale_run() {
  local project="$1"
  local run="$2"
  local run_dir="$DATA_ROOT/$project/$run"
  local run_checksum_dir="$CHECKSUM_DIR/$project/$run"
  local run_tmp_dir="$TMP_ROOT/$project/$run"

  if [[ -d "$run_dir" || -d "$run_checksum_dir" || -d "$run_tmp_dir" ]]; then
    log "Cleaning stale partial data for $run"
    run_cmd rm -rf "$run_dir" "$run_checksum_dir" "$run_tmp_dir"
  fi
}

gzip_fastqs() {
  local outdir="$1"
  local f
  shopt -s nullglob
  for f in "$outdir"/*.fastq; do
    if command -v pigz >/dev/null 2>&1; then
      log "Compressing $(basename "$f") with pigz"
      run_cmd pigz -p "$PIGZ_THREADS" -f "$f"
    else
      log "Compressing $(basename "$f") with gzip"
      run_cmd gzip -f "$f"
    fi
  done
  shopt -u nullglob
}

write_checksum_file() {
  local file_path="$1"
  local checksum_path="$2"
  if [[ "$DRY_RUN" == "1" ]]; then
    printf 'DRY_RUN: sha256sum %q > %q\n' "$file_path" "$checksum_path"
  else
    sha256sum "$file_path" >"$checksum_path"
  fi
}

verify_s3_size() {
  local local_path="$1"
  local s3_uri="$2"
  local bucket key local_size remote_size

  bucket="$(printf '%s\n' "$s3_uri" | sed -E 's#^s3://([^/]+)/.*#\1#')"
  key="$(printf '%s\n' "$s3_uri" | sed -E 's#^s3://[^/]+/(.*)#\1#')"
  local_size="$(stat -c '%s' "$local_path")"
  remote_size="$(aws "${AWS_ARGS[@]}" s3api head-object --bucket "$bucket" --key "$key" --query 'ContentLength' --output text)"

  [[ "$local_size" == "$remote_size" ]] || die "Size mismatch for $s3_uri local=$local_size remote=$remote_size"
}

upload_and_verify_file() {
  local file_path="$1"
  local destination_folder="$2"
  local checksum_path="$3"
  local file_name checksum_name s3_uri checksum_uri

  file_name="$(basename "$file_path")"
  checksum_name="$(basename "$checksum_path")"
  s3_uri="${destination_folder%/}/${file_name}"
  checksum_uri="${destination_folder%/}/${checksum_name}"

  log "Uploading $file_name"
  run_cmd aws "${AWS_ARGS[@]}" s3 cp "$file_path" "$s3_uri" --only-show-errors
  if [[ "$DRY_RUN" != "1" ]]; then
    log "Verifying size for $file_name"
    verify_s3_size "$file_path" "$s3_uri"
  fi
  log "Uploading $checksum_name"
  run_cmd aws "${AWS_ARGS[@]}" s3 cp "$checksum_path" "$checksum_uri" --only-show-errors
}

collect_outputs() {
  local run_dir="$1"
  find "$run_dir" -maxdepth 1 -type f ! -name '.*' | sort
}

process_output_files() {
  local project="$1"
  local run_dir="$2"
  local destination_folder="$3"
  local run="$4"
  local output_file checksum_path found_output=0

  while IFS= read -r output_file; do
    [[ -n "$output_file" ]] || continue
    found_output=1
    checksum_path="$CHECKSUM_DIR/${project}/${run}/$(basename "$output_file").sha256"
    mkdir -p "$(dirname "$checksum_path")"
    write_checksum_file "$output_file" "$checksum_path"
    upload_and_verify_file "$output_file" "$destination_folder" "$checksum_path"
  done < <(collect_outputs "$run_dir" "$run")

  [[ "$found_output" == "1" ]] || die "No output files were produced for $run in $run_dir"
}

download_fastq_run() {
  local run="$1"
  local run_dir="$2"
  local run_tmp_dir="$3"

  mkdir -p "$run_tmp_dir"
  log "Prefetching $run"
  run_cmd prefetch --progress --max-size u -O "$run_dir" "$run"

  log "Converting $run to FASTQ"
  run_cmd fasterq-dump \
    --threads "$THREADS" \
    --split-files \
    --temp "$run_tmp_dir" \
    -O "$run_dir" \
    "$run_dir/$run"

  gzip_fastqs "$run_dir"
}

download_bam_run() {
  local run="$1"
  local run_dir="$2"
  local bam_name bam_url bam_path bam_metadata

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

    log "Writing $run.bam"
    if [[ "$DRY_RUN" == "1" ]]; then
      printf 'DRY_RUN: sam-dump --header %q | samtools view -b -o %q -\n' "$run_dir/$run" "$run_dir/${run}.bam"
    else
      sam-dump --header "$run_dir/$run" | samtools view -b -o "$run_dir/${run}.bam" -
    fi
  fi
}

process_run() {
  local project="$1"
  local data_type="$2"
  local destination_folder="$3"
  local run="$4"
  local state_file="$5"
  local project_log="$6"
  local run_dir="$DATA_ROOT/$project/$run"
  local run_checksum_dir="$CHECKSUM_DIR/$project/$run"
  local run_tmp_dir="$TMP_ROOT/$project/$run"
  local status=0

  {
    if already_done "$state_file" "$run"; then
      log "Skipping completed run $run"
      exit 0
    fi

    cleanup_stale_run "$project" "$run"
    mkdir -p "$run_dir"
    log "Starting project=$project run=$run type=$data_type"

    case "$data_type" in
      fastq) download_fastq_run "$run" "$run_dir" "$run_tmp_dir" ;;
      bam) download_bam_run "$run" "$run_dir" ;;
      *) die "Unsupported data_type '$data_type' for $project" ;;
    esac

    process_output_files "$project" "$run_dir" "$destination_folder" "$run"
    mark_done "$state_file" "$run"
    cleanup_run "$run_dir" "$run_checksum_dir" "$run_tmp_dir"
    log "Completed project=$project run=$run"
  } > >(tee -a "$project_log") 2>&1 || status=$?

  if [[ "$status" -ne 0 ]]; then
    {
      log "FAILED project=$project run=$run exit_code=$status"
      cleanup_stale_run "$project" "$run"
    } >>"$project_log" 2>&1
  fi

  return "$status"
}

wait_for_slot() {
  local -n pids_ref="$1"
  local pid status i
  local -a live_pids=()

  while ((${#pids_ref[@]} >= MAX_PARALLEL_RUNS)); do
    status=0
    wait -n "${pids_ref[@]}" || status=$?

    live_pids=()
    for pid in "${pids_ref[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then
        live_pids+=("$pid")
      fi
    done
    pids_ref=("${live_pids[@]}")

    if [[ "$status" -ne 0 ]]; then
      for pid in "${pids_ref[@]}"; do
        kill "$pid" 2>/dev/null || true
      done
      wait "${pids_ref[@]}" 2>/dev/null || true
      return "$status"
    fi
  done
}

wait_for_all() {
  local -n pids_ref="$1"
  local pid status=0

  for pid in "${pids_ref[@]}"; do
    if ! wait "$pid"; then
      [[ "$status" -eq 0 ]] && status=1
    fi
  done
  return "$status"
}

process_project() {
  local project="$1"
  local data_type="$2"
  local destination_folder="$3"
  local state_file="$STATE_DIR/${project}.done"
  local project_log="$LOG_DIR/${project}.log"
  local run
  local -a pids=()

  mkdir -p "$DATA_ROOT/$project" "$(dirname "$project_log")" "$CHECKSUM_DIR/$project" "$TMP_ROOT/$project"
  log "Processing bioproject=$project type=$data_type destination=$destination_folder parallel_runs=$MAX_PARALLEL_RUNS"

  while IFS= read -r run; do
    [[ -n "$run" ]] || continue
    wait_for_slot pids
    process_run "$project" "$data_type" "$destination_folder" "$run" "$state_file" "$project_log" &
    pids+=("$!")
  done < <(jq -r --arg project "$project" '.projects[] | select(.bioproject_accession == $project) | .run_accessions[]' "$MANIFEST_PATH")

  wait_for_all pids
}

process_manifest() {
  local project data_type destination_folder

  while IFS=$'\t' read -r project data_type destination_folder; do
    [[ -n "$project" ]] || continue
    process_project "$project" "$data_type" "$destination_folder"
  done < <(jq -r '.projects[] | [.bioproject_accession, .data_type, .destination_folder] | @tsv' "$MANIFEST_PATH")
}

main() {
  ensure_base_tools
  ensure_inputs
  install_sra_toolkit
  ensure_aws_auth
  process_manifest
  log "All projects completed"
}

main
