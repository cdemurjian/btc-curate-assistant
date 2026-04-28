from __future__ import annotations

import argparse
from pathlib import Path

from btc_manifest.aws import (
    check_aws_sso,
    list_s3_keys,
    move_s3_object,
    parse_s3_uri,
    plan_s3_parenthesis_renames,
)
from btc_manifest.config import Settings
from btc_manifest.templates import replace_strings_in_xlsx


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Remove parentheses from S3 object keys and update XLSX manifests to match.",
    )
    parser.add_argument("s3_uri", help="S3 prefix to scan, for example s3://bucket/path/")
    parser.add_argument(
        "xlsx_paths",
        nargs="*",
        type=Path,
        help="Optional .xlsx files whose exact path strings should be updated after the S3 moves.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute S3 moves and rewrite the XLSX files. Default is dry-run.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional override for Settings.from_env().",
    )
    return parser


def print_plan(renames: list[object]) -> None:
    for rename in renames:
        print(f"{rename.old_uri} -> {rename.new_uri}")


def validate_xlsx_paths(paths: list[Path]) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise SystemExit("Missing XLSX file(s):\n" + "\n".join(f"  {path}" for path in missing))


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = Settings.from_env(output_dir=args.output_dir)
    s3_uri = parse_s3_uri(args.s3_uri).to_uri()

    aws_ok, aws_message = check_aws_sso(settings)
    if not aws_ok:
        login_hint = (
            f"\nRun `aws sso login --profile {settings.aws_profile}` and rerun this command."
            if settings.aws_profile
            else ""
        )
        raise SystemExit(f"AWS is not ready: {aws_message}{login_hint}")

    validate_xlsx_paths(args.xlsx_paths)
    keys = list_s3_keys(s3_uri, settings)
    renames, collisions = plan_s3_parenthesis_renames(s3_uri, keys)

    print(f"AWS SSO OK: {aws_message}")
    print(f"Scanned {len(keys)} object(s) under {s3_uri}")

    if collisions:
        raise SystemExit("Refusing to continue because of rename collisions:\n" + "\n".join(collisions))

    if not renames:
        print("No object keys contain parentheses. Nothing to rename.")
        return

    print(f"Planned {len(renames)} rename(s):")
    print_plan(renames)

    if not args.apply:
        print("\nDry run only. Re-run with --apply to execute the S3 moves and update the XLSX files.")
        return

    completed: list[object] = []
    for rename in renames:
        result = move_s3_object(rename, settings)
        if result.returncode != 0:
            message = (result.stderr or result.stdout).strip()
            raise SystemExit(
                "S3 rename failed after partial progress. XLSX files were not updated.\n"
                f"Failed move: {rename.old_uri} -> {rename.new_uri}\n"
                f"{message}"
            )
        completed.append(rename)

    replacements = {rename.old_uri: rename.new_uri for rename in completed}
    for xlsx_path in args.xlsx_paths:
        changed_cells = replace_strings_in_xlsx(xlsx_path, replacements)
        print(f"Updated {xlsx_path}: {changed_cells} cell(s)")

    print(f"Completed {len(completed)} S3 rename(s).")


if __name__ == "__main__":
    main()
