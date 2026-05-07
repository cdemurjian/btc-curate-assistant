from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from btc.fetch import bioproject


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = REPO_ROOT / "scripts" / "fetch"

SCRIPT_MAP = {
    ("bioproject",): None,
    ("bioprojects-to-s3",): "fetch_bioprojects_to_s3.sh",
    ("from-manifest",): "fetch_from_manifest.sh",
    ("manifest", "generate"): "generate_bioproject_manifest.sh",
    ("s3-prefixes", "create"): "create_s3_bioproject_prefixes.sh",
    ("gsm-cat",): "cat_gsm_fastqs.sh",
    ("bam", "test"): "test_bam.sh",
    ("bam", "remap"): "remap_bam_to_reference.sh",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="btc fetch",
        description="BTC data acquisition and staging workflows.",
    )
    parser.add_argument("args", nargs=argparse.REMAINDER, help="Fetch subcommand and its arguments.")
    return parser


def _resolve_script(argv: list[str]) -> tuple[Path | None, list[str]]:
    for key, script_name in sorted(SCRIPT_MAP.items(), key=lambda item: len(item[0]), reverse=True):
        if argv[: len(key)] == list(key):
            return (SCRIPTS_DIR / script_name if script_name else None), argv[len(key) :]
    available = ", ".join(" ".join(parts) for parts in SCRIPT_MAP)
    raise SystemExit(f"Unknown fetch command. Available commands: {available}")


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.args:
        parser.print_help()
        return

    script_path, script_args = _resolve_script(args.args)
    if args.args[:1] == ["bioproject"]:
        bioproject.main(script_args)
        return

    result = subprocess.run([str(script_path), *script_args], check=False)
    raise SystemExit(result.returncode)
