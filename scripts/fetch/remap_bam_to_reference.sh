#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  remap_bam_to_reference.sh /path/to/input.bam [output_prefix]

Environment overrides:
  REFERENCE_FASTA   Reference FASTA to validate/remap against
                    default: ./data/reference/fetch/UU_Cfam_GSD_1.0_ROSY.fa
  WORK_ROOT         Working directory for reports and temporary files
                    default: ./data/workspace/fetch/bam_test_work/remap
  THREADS           Threads for samtools/alignment steps
                    default: 8
  REPORT_ONLY       1 = only validate/report; skip FASTQ extraction/remap
                    default: 0

What it does:
  1. Indexes the reference FASTA if needed
  2. Extracts BAM @SQ contigs and compares them to the FASTA .fai
  3. Writes a compact validation report
  4. If an aligner is available, extracts FASTQ from the BAM and remaps it

Aligners:
  - Uses minimap2 if available
  - Falls back to bwa if available
EOF
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage >&2
  exit 2
fi

INPUT_BAM="$1"
OUTPUT_PREFIX="${2:-$(basename "${INPUT_BAM%.bam}")}"
REFERENCE_FASTA="${REFERENCE_FASTA:-$PWD/data/reference/fetch/UU_Cfam_GSD_1.0_ROSY.fa}"
WORK_ROOT="${WORK_ROOT:-$PWD/data/workspace/fetch/bam_test_work/remap}"
THREADS="${THREADS:-8}"
REPORT_ONLY="${REPORT_ONLY:-0}"

RUN_DIR="$WORK_ROOT/$OUTPUT_PREFIX"
REPORT_DIR="$RUN_DIR/report"
TMP_DIR="$RUN_DIR/tmp"
FASTQ_DIR="$RUN_DIR/fastq"
ALIGN_DIR="$RUN_DIR/alignment"

REF_CONTIGS="$REPORT_DIR/reference_contigs.tsv"
BAM_CONTIGS="$REPORT_DIR/bam_contigs.tsv"
EXACT_DIFF="$REPORT_DIR/contig_diff_exact.txt"
SUMMARY_TXT="$REPORT_DIR/summary.txt"
LENGTH_REPORT="$REPORT_DIR/length_comparison.txt"

PAIRED_R1="$FASTQ_DIR/${OUTPUT_PREFIX}_R1.fastq.gz"
PAIRED_R2="$FASTQ_DIR/${OUTPUT_PREFIX}_R2.fastq.gz"
SINGLETON_FQ="$FASTQ_DIR/${OUTPUT_PREFIX}_singletons.fastq.gz"
REMAP_BAM="$ALIGN_DIR/${OUTPUT_PREFIX}.remapped.bam"
REMAP_SINGLE_BAM="$ALIGN_DIR/${OUTPUT_PREFIX}.singletons.remapped.bam"
REMAP_MERGED_BAM="$ALIGN_DIR/${OUTPUT_PREFIX}.merged.remapped.bam"

log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*" >&2
}

die() {
  log "ERROR: $*"
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

choose_aligner() {
  if command -v minimap2 >/dev/null 2>&1; then
    printf 'minimap2\n'
    return 0
  fi
  if command -v bwa >/dev/null 2>&1; then
    printf 'bwa\n'
    return 0
  fi
  return 1
}

ensure_inputs() {
  [[ -f "$INPUT_BAM" ]] || die "BAM not found: $INPUT_BAM"
  [[ -f "$REFERENCE_FASTA" ]] || die "Reference FASTA not found: $REFERENCE_FASTA"
}

ensure_tools() {
  need_cmd samtools
  need_cmd awk
  need_cmd diff
  need_cmd sort
  need_cmd gzip
}

prepare_dirs() {
  mkdir -p "$REPORT_DIR" "$TMP_DIR"
  if [[ "$REPORT_ONLY" != "1" ]]; then
    mkdir -p "$FASTQ_DIR" "$ALIGN_DIR"
  fi
}

index_reference() {
  if [[ ! -f "${REFERENCE_FASTA}.fai" ]]; then
    log "Indexing reference FASTA"
    samtools faidx "$REFERENCE_FASTA"
  fi
}

extract_reference_contigs() {
  cut -f1,2 "${REFERENCE_FASTA}.fai" >"$REF_CONTIGS"
}

extract_bam_contigs() {
  samtools view -H "$INPUT_BAM" \
    | awk -F'\t' '
        /^@SQ/ {
          sn = ""; ln = "";
          for (i = 1; i <= NF; i++) {
            if ($i ~ /^SN:/) sn = substr($i, 4);
            if ($i ~ /^LN:/) ln = substr($i, 4);
          }
          print sn "\t" ln;
        }
      ' >"$BAM_CONTIGS"
}

write_contig_report() {
  local exact_status="mismatch"
  local aligner="none"

  if diff -u "$REF_CONTIGS" "$BAM_CONTIGS" >"$EXACT_DIFF"; then
    exact_status="match"
  fi

  if aligner="$(choose_aligner 2>/dev/null)"; then
    :
  else
    aligner="none"
  fi

  awk -v exact_status="$exact_status" -v aligner="$aligner" '
    NR == FNR {
      ref_name[++ref_n] = $1;
      ref_len[$1] = $2;
      ref_len_count[$2]++;
      next;
    }
    {
      bam_name[++bam_n] = $1;
      bam_len[$1] = $2;
      bam_len_count[$2]++;
    }
    END {
      print "exact_match\t" exact_status;
      print "reference_contigs\t" ref_n;
      print "bam_contigs\t" bam_n;
      print "aligner_available\t" aligner;

      ref_only_lengths = 0;
      for (len in ref_len_count) {
        diff = ref_len_count[len] - bam_len_count[len];
        if (diff > 0) ref_only_lengths += diff;
      }

      bam_only_lengths = 0;
      for (len in bam_len_count) {
        diff = bam_len_count[len] - ref_len_count[len];
        if (diff > 0) bam_only_lengths += diff;
      }

      print "lengths_missing_from_bam\t" ref_only_lengths;
      print "lengths_missing_from_reference\t" bam_only_lengths;
    }
  ' "$REF_CONTIGS" "$BAM_CONTIGS" >"$SUMMARY_TXT"

  awk '
    NR == FNR {
      ref_len_count[$2]++;
      next;
    }
    {
      bam_len_count[$2]++;
    }
    END {
      print "reference_only_lengths";
      shown = 0;
      for (len in ref_len_count) {
        diff = ref_len_count[len] - bam_len_count[len];
        while (diff > 0 && shown < 20) {
          print len;
          diff--;
          shown++;
        }
      }

      print "";
      print "bam_only_lengths";
      shown = 0;
      for (len in bam_len_count) {
        diff = bam_len_count[len] - ref_len_count[len];
        while (diff > 0 && shown < 20) {
          print len;
          diff--;
          shown++;
        }
      }
    }
  ' "$REF_CONTIGS" "$BAM_CONTIGS" >"$LENGTH_REPORT"
}

extract_fastq() {
  log "Extracting FASTQ from BAM"
  samtools collate -@ "$THREADS" -O -u "$INPUT_BAM" \
    | samtools fastq \
        -@ "$THREADS" \
        -n \
        -1 >(gzip -c >"$PAIRED_R1") \
        -2 >(gzip -c >"$PAIRED_R2") \
        -s >(gzip -c >"$SINGLETON_FQ") \
        -0 /dev/null \
        -
}

ensure_bwa_index() {
  if [[ -f "${REFERENCE_FASTA}.bwt" ]]; then
    return 0
  fi
  log "Indexing reference for bwa"
  bwa index "$REFERENCE_FASTA"
}

align_paired() {
  local aligner="$1"

  if [[ ! -s "$PAIRED_R1" || ! -s "$PAIRED_R2" ]]; then
    log "No paired FASTQ output detected; skipping paired remap"
    return 0
  fi

  log "Remapping paired reads with $aligner"
  case "$aligner" in
    minimap2)
      minimap2 -t "$THREADS" -ax sr "$REFERENCE_FASTA" "$PAIRED_R1" "$PAIRED_R2" \
        | samtools sort -@ "$THREADS" -o "$REMAP_BAM" -
      ;;
    bwa)
      ensure_bwa_index
      bwa mem -t "$THREADS" "$REFERENCE_FASTA" "$PAIRED_R1" "$PAIRED_R2" \
        | samtools sort -@ "$THREADS" -o "$REMAP_BAM" -
      ;;
    *)
      die "Unsupported aligner: $aligner"
      ;;
  esac
}

align_singletons() {
  local aligner="$1"

  if [[ ! -s "$SINGLETON_FQ" ]]; then
    log "No singleton FASTQ output detected; skipping singleton remap"
    return 0
  fi

  log "Remapping singleton reads with $aligner"
  case "$aligner" in
    minimap2)
      minimap2 -t "$THREADS" -ax sr "$REFERENCE_FASTA" "$SINGLETON_FQ" \
        | samtools sort -@ "$THREADS" -o "$REMAP_SINGLE_BAM" -
      ;;
    bwa)
      ensure_bwa_index
      bwa mem -t "$THREADS" "$REFERENCE_FASTA" "$SINGLETON_FQ" \
        | samtools sort -@ "$THREADS" -o "$REMAP_SINGLE_BAM" -
      ;;
    *)
      die "Unsupported aligner: $aligner"
      ;;
  esac
}

merge_outputs() {
  if [[ -s "$REMAP_BAM" && -s "$REMAP_SINGLE_BAM" ]]; then
    log "Merging paired and singleton remap outputs"
    samtools merge -@ "$THREADS" -f "$REMAP_MERGED_BAM" "$REMAP_BAM" "$REMAP_SINGLE_BAM"
    samtools index -@ "$THREADS" "$REMAP_MERGED_BAM"
    return 0
  fi

  if [[ -s "$REMAP_BAM" ]]; then
    samtools index -@ "$THREADS" "$REMAP_BAM"
    return 0
  fi

  if [[ -s "$REMAP_SINGLE_BAM" ]]; then
    samtools index -@ "$THREADS" "$REMAP_SINGLE_BAM"
    return 0
  fi
}

main() {
  local aligner

  ensure_inputs
  ensure_tools
  prepare_dirs
  index_reference
  extract_reference_contigs
  extract_bam_contigs
  write_contig_report

  log "Validation report written to $REPORT_DIR"

  if [[ "$REPORT_ONLY" == "1" ]]; then
    return 0
  fi

  if ! aligner="$(choose_aligner)"; then
    log "No supported aligner found. Report is available, but remap was skipped."
    return 0
  fi

  extract_fastq
  align_paired "$aligner"
  align_singletons "$aligner"
  merge_outputs

  log "Remap completed under $ALIGN_DIR"
}

main "$@"
