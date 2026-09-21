"""Estimate genome coverage per FASTQ from FastQC reports (.html, or .zip as fallback) on S3.

coverage = (read_length * total_sequences) / genome_size

Two input modes:
  * CSV  -> columns file_source (s3 path) and biospecimen_trial_id; output includes biospecimen.
  * s3:// prefix -> recursive listing; output has no biospecimen column.

FASTQ -> FastQC HTML matching:
  * exact:  X.fastq.gz -> X_fastqc.html  (also X.tag.fastq.gz -> X_fastqc.tag.html)
  * merged: D24-5311_1-merged.fastq.gz -> every *_D24-5311[-N]_1_sequence_fastqc.html in the
            same directory (one per lane/library merged in); their coverages are summed.
A report's .zip is only used when its sibling .html is absent.
"""
from __future__ import annotations

import argparse
import csv
import posixpath
import io
import re
import subprocess
import sys
import zipfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from btc.common.aws import list_s3_keys, parse_s3_uri
from btc.common.config import Settings

DEFAULT_GENOME_SIZE = 3.1e9  # approx. haploid human genome (GRCh38 ~3.1 Gbp)

FASTQ_RE = re.compile(r"\.(fastq|fq)(\.gz)?$")
MERGED_RE = re.compile(r"^(?P<base>.+)_(?P<read>[12])-merged\.(fastq|fq)\.gz$")
HTML_KEY_RE = re.compile(
    r"^(?:\d{6}[A-Za-z]+_)?(?P<base>.+?)_(?P<read>[12])_sequence_fastqc(?:\.[^._]+)?\.(?:html|zip)$"
)
TOTAL_RE = re.compile(r"<td>\s*Total Sequences\s*</td>\s*<td>\s*(\d+)\s*</td>", re.I)
LENGTH_RE = re.compile(r"<td>\s*Sequence length\s*</td>\s*<td>\s*(\d+)(?:\s*-\s*(\d+))?\s*</td>", re.I)
TXT_TOTAL_RE = re.compile(r"^Total Sequences\t(\d+)\s*$", re.M)
TXT_LENGTH_RE = re.compile(r"^Sequence length\t(\d+)(?:-(\d+))?\s*$", re.M)


@dataclass
class FastqcStats:
    total_sequences: int
    read_length: float
    length_is_range: bool = False

    def coverage(self, genome_size: float) -> float:
        return self.read_length * self.total_sequences / genome_size


@dataclass
class Row:
    fastq: str
    biospecimen: str = ""
    htmls: list[str] = field(default_factory=list)
    coverage: float | None = None
    total_reads: int | None = None
    read_length: str = ""
    note: str = ""


def parse_fastqc_html(html: str) -> FastqcStats:
    return _stats_from_matches(TOTAL_RE.search(html), LENGTH_RE.search(html))


def parse_fastqc_zip(data: bytes) -> FastqcStats:
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        member = next((n for n in zf.namelist() if n.endswith("/fastqc_data.txt") or n == "fastqc_data.txt"), None)
        if member is None:
            raise ValueError("fastqc_data.txt not found in zip")
        text = zf.read(member).decode(errors="replace")
    return _stats_from_matches(TXT_TOTAL_RE.search(text), TXT_LENGTH_RE.search(text))


def parse_fastqc_report(uri: str, data: bytes) -> FastqcStats:
    if uri.endswith(".zip"):
        return parse_fastqc_zip(data)
    return parse_fastqc_html(data.decode(errors="replace"))


def _stats_from_matches(total, length) -> FastqcStats:
    if not total or not length:
        raise ValueError("Basic Statistics (Total Sequences / Sequence length) not found")
    lo, hi = int(length.group(1)), length.group(2)
    if hi:  # trimmed reads: FastQC reports a range; use the midpoint
        return FastqcStats(int(total.group(1)), (lo + int(hi)) / 2, length_is_range=True)
    return FastqcStats(int(total.group(1)), float(lo))


# --- matching -----------------------------------------------------------------------------


def exact_html_candidates(fastq: str) -> list[str]:
    d, name = posixpath.split(fastq)
    stem = FASTQ_RE.sub("", name)
    cands = [f"{stem}_fastqc.html"]
    if "." in stem:
        head, _, tag = stem.rpartition(".")
        cands.append(f"{head}_fastqc.{tag}.html")
    cands += [c[: -len(".html")] + ".zip" for c in cands]
    return [posixpath.join(d, c) for c in cands]


def match_htmls(fastqs: list[str], htmls: list[str]) -> dict[str, list[str]]:
    html_set = set(htmls)
    by_dir: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    all_keyed: list[tuple[str, str, str]] = []
    for h in htmls:
        m = HTML_KEY_RE.match(posixpath.basename(h))
        if m:
            entry = (h, m["base"], m["read"])
            by_dir[posixpath.dirname(h)].append(entry)
            all_keyed.append(entry)

    def is_lane_of(entry: tuple[str, str, str], base: str, read: str) -> bool:
        _, hbase, hread = entry
        return hread == read and (hbase == base or re.fullmatch(re.escape(base) + r"-\d+", hbase) is not None)

    out: dict[str, list[str]] = {}
    for fq in fastqs:
        exact = [c for c in exact_html_candidates(fq) if c in html_set]
        if exact:
            out[fq] = exact[:1]
            continue
        m = MERGED_RE.match(posixpath.basename(fq))
        if not m:
            out[fq] = []
            continue
        base, read = m["base"], m["read"]
        found = [e[0] for e in by_dir.get(posixpath.dirname(fq), []) if is_lane_of(e, base, read)]
        if not found:  # lanes may live elsewhere in the input set
            found = [e[0] for e in all_keyed if is_lane_of(e, base, read)]
        out[fq] = sorted(found)
    return out


# --- AWS ------------------------------------------------------------------------------------


def fetch_report(uri: str, profile: str | None, region: str | None) -> bytes:
    cmd = ["aws", "s3", "cp", uri, "-", "--only-show-errors"]
    if profile:
        cmd += ["--profile", profile]
    if region:
        cmd += ["--region", region]
    res = subprocess.run(cmd, capture_output=True, check=False)
    if res.returncode != 0:
        raise RuntimeError(res.stderr.decode(errors="replace").strip() or "aws s3 cp failed")
    return res.stdout


def load_stats(
    uris: list[str], profile: str | None, region: str | None, workers: int
) -> dict[str, FastqcStats | str]:
    """Returns stats per URI, or an error string."""

    def one(uri: str) -> tuple[str, FastqcStats | str]:
        try:
            return uri, parse_fastqc_report(uri, fetch_report(uri, profile, region))
        except Exception as exc:  # noqa: BLE001 - reported per-row
            return uri, str(exc)

    done = 0
    results: dict[str, FastqcStats | str] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for uri, res in pool.map(one, uris):
            results[uri] = res
            done += 1
            if done % 25 == 0 or done == len(uris):
                print(f"  parsed {done}/{len(uris)} FastQC reports", file=sys.stderr)
    return results


# --- inputs ---------------------------------------------------------------------------------


def read_csv_inputs(path: str) -> tuple[list[str], dict[str, str]]:
    """Returns (all s3 paths, path -> biospecimen)."""
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        cols = reader.fieldnames or []
        path_col = next((c for c in ("file_source", "file_path", "s3_path", "path") if c in cols), None)
        bio_col = next((c for c in ("biospecimen_trial_id", "biospecimen", "biospecimen_key") if c in cols), None)
        if not path_col:
            raise SystemExit(f"{path}: no s3 path column found (looked for file_source/file_path/s3_path/path)")
        paths: list[str] = []
        bio: dict[str, str] = {}
        for row in reader:
            p = (row[path_col] or "").strip()
            if p.startswith("s3://"):
                paths.append(p)
                bio[p] = (row.get(bio_col) or "").strip() if bio_col else ""
    return paths, bio


def list_s3_inputs(prefix: str, settings: Settings) -> list[str]:
    loc = parse_s3_uri(prefix)
    return [loc.object_uri(k) for k in list_s3_keys(prefix, settings)]


# --- main -----------------------------------------------------------------------------------


def find_reports(paths) -> list[str]:
    """FastQC reports in the input; a .zip is dropped when its sibling .html is also present."""
    pset = set(paths)
    return sorted(
        p for p in pset
        if "_fastqc" in posixpath.basename(p)
        and (p.endswith(".html") or (p.endswith(".zip") and p[:-4] + ".html" not in pset))
    )


def build_rows(paths, bio, genome_size, stats_of_uri) -> list[Row]:
    fastqs = sorted({p for p in paths if FASTQ_RE.search(p)})
    htmls = find_reports(paths)
    matches = match_htmls(fastqs, htmls)
    used = {h for hs in matches.values() for h in hs}

    rows: list[Row] = []
    for fq in fastqs:
        rows.append(_make_row(fq, bio.get(fq, ""), matches[fq], genome_size, stats_of_uri))
    for h in htmls:  # FastQC reports whose FASTQ is not in the input
        if h not in used:
            row = _make_row("", bio.get(h, ""), [h], genome_size, stats_of_uri)
            row.note = ("no FASTQ in input; " + row.note).strip("; ")
            rows.append(row)
    return rows


def _make_row(fq, biospec, htmls, genome_size, stats_of_uri) -> Row:
    row = Row(fastq=fq, biospecimen=biospec, htmls=htmls)
    if not htmls:
        row.note = "no matching FastQC report"
        return row
    stats = [stats_of_uri(h) for h in htmls]
    errors = [s for s in stats if isinstance(s, str)]
    if errors:
        row.note = f"could not read {len(errors)}/{len(htmls)} report: {errors[0]}"
        return row
    row.coverage = sum(s.coverage(genome_size) for s in stats)
    row.total_reads = sum(s.total_sequences for s in stats)
    row.read_length = ",".join(f"{s.read_length:g}" for s in stats)
    notes = []
    if len(htmls) > 1:
        notes.append(f"merged: summed {len(htmls)} FastQC reports")
    if any(s.length_is_range for s in stats):
        notes.append("read length was a range; used midpoint")
    row.note = "; ".join(notes)
    return row


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(
        prog="btc qc coverage",
        description="Genome coverage per FASTQ from FastQC HTML reports on S3: (read_length * n_reads) / genome_size.",
    )
    ap.add_argument("input", help="CSV (with file_source + biospecimen_trial_id) or an s3:// prefix.")
    ap.add_argument("-o", "--output", default="fastq_coverage.csv", help="Output CSV (default: %(default)s).")
    ap.add_argument("-g", "--genome-size", type=float, default=DEFAULT_GENOME_SIZE,
                    help="Haploid genome length in bp (default: %(default).3g).")
    ap.add_argument("--profile", help="AWS profile (default: AWS_PROFILE from .env).")
    ap.add_argument("--region", help="AWS region (default: AWS_REGION from .env).")
    ap.add_argument("-j", "--workers", type=int, default=8, help="Parallel downloads (default: %(default)s).")
    args = ap.parse_args(argv)

    settings = Settings.from_env()
    profile = args.profile or settings.aws_profile
    region = args.region or settings.aws_region

    csv_mode = not args.input.startswith(("s3://", "http://", "https://"))
    if csv_mode:
        paths, bio = read_csv_inputs(args.input)
    else:
        print(f"Listing {args.input} ...", file=sys.stderr)
        paths, bio = list_s3_inputs(args.input, settings), {}
    print(f"{len(paths)} paths in input", file=sys.stderr)

    # Two passes: match first so only the needed htmls are downloaded.
    n_fastqs = sum(1 for p in set(paths) if FASTQ_RE.search(p))
    htmls = find_reports(paths)
    if not htmls:
        raise SystemExit("No FastQC html/zip reports found in input.")
    print(f"{n_fastqs} FASTQs, {len(htmls)} FastQC reports; downloading ...", file=sys.stderr)
    stats = load_stats(htmls, profile, region, args.workers)

    rows = build_rows(paths, bio, args.genome_size, stats.__getitem__)

    header = ["fastq_path"] + (["biospecimen"] if csv_mode else []) + [
        "coverage", "total_reads", "read_length", "fastqc_report_paths", "notes"]
    with open(args.output, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for r in rows:
            cov = "" if r.coverage is None else f"{r.coverage:.2f}"
            w.writerow([r.fastq] + ([r.biospecimen] if csv_mode else []) + [
                cov, "" if r.total_reads is None else r.total_reads, r.read_length,
                ";".join(r.htmls), r.note])

    ok = sum(r.coverage is not None for r in rows)
    print(f"Wrote {len(rows)} rows ({ok} with coverage, {len(rows) - ok} without) -> {args.output}", file=sys.stderr)
