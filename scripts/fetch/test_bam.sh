#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  test_bam.sh /path/to/bioproject_manifest.json

Environment overrides:
  REFERENCE_FASTA          Reference FASTA to verify against
                           default: /mnt/disk1/home/dlt/globusconnectpersonal/UU_Cfam/UU_Cfam_GSD_1.0_ROSY.fa
  TOOL_ROOT                Local SRA Toolkit install root (default: ./data/workspace/fetch/tools)
  WORK_ROOT                Working root for test artifacts (default: ./data/workspace/fetch/bam_test_work)
  AWS_PROFILE              Ignored here; accepted for shell consistency
  DRY_RUN=1                Print planned actions without executing

This script:
  1. Selects one random SRR from each manifest project with data_type == "bam"
  2. Downloads that SRR with prefetch
  3. Converts it to BAM
  4. Compares BAM @SQ contig names and lengths to the exact local FASTA .fai
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
REFERENCE_FASTA="${REFERENCE_FASTA:-/mnt/disk1/home/dlt/globusconnectpersonal/UU_Cfam/UU_Cfam_GSD_1.0_ROSY.fa}"
TOOL_ROOT="${TOOL_ROOT:-$PWD/data/workspace/fetch/tools}"
WORK_ROOT="${WORK_ROOT:-$PWD/data/workspace/fetch/bam_test_work}"
DRY_RUN="${DRY_RUN:-0}"
DATA_ROOT="$WORK_ROOT/data"
REPORT_ROOT="$WORK_ROOT/reports"

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

ensure_inputs() {
  [[ -f "$MANIFEST_PATH" ]] || die "Manifest not found: $MANIFEST_PATH"
  [[ -f "$REFERENCE_FASTA" ]] || die "Reference FASTA not found: $REFERENCE_FASTA"
  jq -e '.projects and (.projects | type == "array")' "$MANIFEST_PATH" >/dev/null \
    || die "Manifest is missing a valid .projects array: $MANIFEST_PATH"
}

ensure_base_tools() {
  need_cmd curl
  need_cmd jq
  need_cmd samtools
  need_cmd shuf
  need_cmd diff
  need_cmd awk
  need_cmd grep
  need_cmd cut
  need_cmd tee
}

install_sra_toolkit() {
  local url archive bindir
  if command -v prefetch >/dev/null 2>&1 && command -v sam-dump >/dev/null 2>&1; then
    return 0
  fi

  url="https://ftp.ncbi.nlm.nih.gov/sra/sdk/current/sratoolkit.current-ubuntu64.tar.gz"
  archive="$TOOL_ROOT/sratoolkit.current-ubuntu64.tar.gz"
  mkdir -p "$TOOL_ROOT"

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
  need_cmd sam-dump
}

index_reference() {
  if [[ -f "${REFERENCE_FASTA}.fai" ]]; then
    return 0
  fi
  log "Indexing reference FASTA"
  run_cmd samtools faidx "$REFERENCE_FASTA"
}

extract_reference_contigs() {
  local out_path="$1"
  if [[ "$DRY_RUN" == "1" ]]; then
    printf 'DRY_RUN: cut -f1,2 %q > %q\n' "${REFERENCE_FASTA}.fai" "$out_path"
  else
    cut -f1,2 "${REFERENCE_FASTA}.fai" >"$out_path"
  fi
}

extract_bam_contigs() {
  local bam_path="$1"
  local header_path="$2"
  local contigs_path="$3"

  if [[ "$DRY_RUN" == "1" ]]; then
    printf 'DRY_RUN: samtools view -H %q | grep %q > %q\n' "$bam_path" '^@SQ' "$header_path"
    printf 'DRY_RUN: awk ... < %q > %q\n' "$header_path" "$contigs_path"
    return 0
  fi

  samtools view -H "$bam_path" | grep '^@SQ' >"$header_path"
  awk -F'\t' '
    /^@SQ/ {
      sn = ""; ln = "";
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^SN:/) sn = substr($i, 4);
        if ($i ~ /^LN:/) ln = substr($i, 4);
      }
      print sn "\t" ln;
    }
  ' "$header_path" >"$contigs_path"
}

pick_random_runs_shell() {
  local project
  for project in $(jq -r '.projects[] | select(.data_type == "bam") | .bioproject_accession' "$MANIFEST_PATH"); do
    jq -r --arg project "$project" '
      .projects[]
      | select(.bioproject_accession == $project)
      | .run_accessions[]
    ' "$MANIFEST_PATH" | shuf -n 1 | awk -v p="$project" '{print p "\t" $0}'
  done
}

verify_one_project() {
  local project="$1"
  local run="$2"
  local run_dir="$DATA_ROOT/$project/$run"
  local bam_path="$run_dir/${run}.bam"
  local report_dir="$REPORT_ROOT/$project/$run"
  local ref_contigs="$report_dir/reference_contigs.txt"
  local header_sq="$report_dir/bam_header_sq.txt"
  local bam_contigs="$report_dir/bam_contigs.txt"
  local diff_path="$report_dir/contig_diff.txt"

  mkdir -p "$run_dir" "$report_dir"

  log "Testing project=$project run=$run"
  log "Prefetching $run"
  run_cmd prefetch --progress --max-size u -O "$run_dir" "$run"

  log "Writing test BAM for $run"
  if [[ "$DRY_RUN" == "1" ]]; then
    printf 'DRY_RUN: sam-dump --header %q | samtools view -b -o %q -\n' "$run_dir/$run" "$bam_path"
  else
    sam-dump --header "$run_dir/$run" | samtools view -b -o "$bam_path" -
  fi

  extract_reference_contigs "$ref_contigs"
  extract_bam_contigs "$bam_path" "$header_sq" "$bam_contigs"

  log "Comparing contigs for $run"
  if [[ "$DRY_RUN" == "1" ]]; then
    printf 'DRY_RUN: diff -u %q %q > %q\n' "$ref_contigs" "$bam_contigs" "$diff_path"
    return 0
  fi

  if diff -u "$ref_contigs" "$bam_contigs" >"$diff_path"; then
    log "MATCH project=$project run=$run"
  else
    log "MISMATCH project=$project run=$run diff=$diff_path"
    return 1
  fi
}

main() {
  ensure_inputs
  ensure_base_tools
  install_sra_toolkit
  index_reference

  mkdir -p "$DATA_ROOT" "$REPORT_ROOT"

  local project run
  while IFS=$'\t' read -r project run; do
    [[ -n "$project" && -n "$run" ]] || continue
    verify_one_project "$project" "$run"
  done < <(pick_random_runs_shell)

  log "BAM verification completed"
}

main
