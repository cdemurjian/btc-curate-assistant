from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MANIFEST_DIR = REPO_ROOT / "manifests" / "bioprojects"
FETCH_FROM_MANIFEST_SCRIPT = REPO_ROOT / "scripts" / "fetch" / "fetch_from_manifest.sh"
VALID_DATA_TYPES = {"fastq", "bam"}


def _runinfo_url(bioproject: str) -> str:
    return f"https://trace.ncbi.nlm.nih.gov/Traces/sra-db-be/runinfo?acc={bioproject}"


def fetch_run_accessions(bioproject: str) -> list[str]:
    try:
        with urlopen(_runinfo_url(bioproject)) as response:
            body = response.read().decode("utf-8")
    except HTTPError as error:
        raise SystemExit(f"Failed to fetch run info for {bioproject}: HTTP {error.code}") from error
    except URLError as error:
        raise SystemExit(f"Failed to fetch run info for {bioproject}: {error.reason}") from error

    lines = [line.strip() for line in body.splitlines() if line.strip()]
    if len(lines) < 2:
        raise SystemExit(f"No run metadata returned for {bioproject}")

    run_accessions: list[str] = []
    for row in lines[1:]:
        first_column = row.split(",", 1)[0].strip()
        if first_column:
            run_accessions.append(first_column)

    if not run_accessions:
        raise SystemExit(f"No run accessions found for {bioproject}")
    return run_accessions


def manifest_data(bioproject: str, data_type: str, destination_folder: str) -> dict[str, object]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "projects": [
            {
                "bioproject_accession": bioproject,
                "data_type": data_type,
                "destination_folder": destination_folder,
                "run_accessions": fetch_run_accessions(bioproject),
            }
        ],
    }


def default_manifest_path(bioproject: str) -> Path:
    return DEFAULT_MANIFEST_DIR / f"{bioproject.lower()}.json"


def write_manifest(output_path: Path, payload: dict[str, object]) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="btc fetch bioproject",
        description="Create a one-project manifest from a BioProject accession and optionally run fetch-from-manifest.",
    )
    parser.add_argument("bioproject", help="BioProject accession, for example PRJNA1368480")
    parser.add_argument("data_type", choices=sorted(VALID_DATA_TYPES), help="Expected output type.")
    parser.add_argument("destination_folder", help="Destination S3 prefix, for example s3://bucket/prefix/PRJNA1368480/")
    parser.add_argument(
        "--manifest-out",
        type=Path,
        default=None,
        help="Where to write the generated manifest. Defaults to manifests/bioprojects/<bioproject>.json.",
    )
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="After generating the manifest, run scripts/fetch/fetch_from_manifest.sh against it.",
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="Print the generated manifest JSON to stdout instead of writing a file.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    payload = manifest_data(args.bioproject, args.data_type, args.destination_folder)

    if args.print_only:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return

    output_path = write_manifest(args.manifest_out or default_manifest_path(args.bioproject), payload)
    print(f"Wrote {output_path}")

    if not args.fetch:
        return

    result = subprocess.run([str(FETCH_FROM_MANIFEST_SCRIPT), str(output_path)], check=False)
    raise SystemExit(result.returncode)
