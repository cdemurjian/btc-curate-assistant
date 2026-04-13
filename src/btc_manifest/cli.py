from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from shlex import quote
from typing import Any
from urllib.parse import urlparse

from btc_manifest.config import Settings


@dataclass(frozen=True)
class S3Location:
    bucket: str
    prefix: str

    def to_uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}" if self.prefix else f"s3://{self.bucket}"


@dataclass(frozen=True)
class CurationPlan:
    s3: S3Location
    run_id: str
    output_dir: Path
    templates_dir: Path
    files_dir: Path
    checks: dict[str, Any] = field(default_factory=dict)
    steps: list[str] = field(default_factory=list)
    commands: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["s3"]["uri"] = self.s3.to_uri()
        data["output_dir"] = str(self.output_dir)
        data["templates_dir"] = str(self.templates_dir)
        data["files_dir"] = str(self.files_dir)
        return data


def parse_s3_uri(uri: str) -> S3Location:
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"expected an s3:// URI, got {uri!r}")
    if not parsed.netloc:
        raise ValueError(f"S3 URI is missing a bucket: {uri!r}")
    return S3Location(bucket=parsed.netloc, prefix=parsed.path.lstrip("/"))


def build_aws_s3_inventory_command(s3_uri: str, settings: Settings) -> str:
    aws_parts = ["aws", "s3", "ls", "--recursive", quote(s3_uri)]
    if settings.aws_profile:
        aws_parts.extend(["--profile", quote(settings.aws_profile)])
    if settings.aws_region:
        aws_parts.extend(["--region", quote(settings.aws_region)])
    aws_command = " ".join(aws_parts)
    awk_command = (
        "awk 'BEGIN {OFS=\"\\t\"} "
        "{key=$4; for (i=5; i<=NF; i++) key=key \" \" $i; print $1, $2, $3, key}'"
    )
    return f"{aws_command} | {awk_command}"


def check_aws_sso(settings: Settings) -> tuple[bool, str]:
    if shutil.which("aws") is None:
        return False, "aws CLI was not found on PATH"
    if not settings.aws_profile:
        return False, "AWS_PROFILE is not set in .env"

    command = ["aws", "sts", "get-caller-identity", "--profile", settings.aws_profile]
    if settings.aws_region:
        command.extend(["--region", settings.aws_region])

    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode == 0:
        identity = json.loads(result.stdout)
        return True, f"authenticated as {identity.get('Arn', 'unknown AWS identity')}"

    message = (result.stderr or result.stdout).strip()
    return False, message or "AWS SSO check failed"


def run_s3_inventory(command: str, output_path: Path) -> subprocess.CompletedProcess[str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as output_file:
        return subprocess.run(
            command,
            shell=True,
            text=True,
            stdout=output_file,
            stderr=subprocess.PIPE,
            check=False,
        )


def build_plan(s3_uri: str, settings: Settings) -> CurationPlan:
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir = settings.output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    template_workbooks = (
        sorted(settings.templates_dir.glob("*.xlsx")) if settings.templates_dir.exists() else []
    )
    plan = CurationPlan(
        s3=parse_s3_uri(s3_uri),
        run_id=run_id,
        output_dir=run_dir,
        templates_dir=settings.templates_dir,
        files_dir=settings.files_dir,
        checks={
            "aws_profile_configured": bool(settings.aws_profile),
            "aws_region_configured": bool(settings.aws_region),
            "mongodb_uri_configured": bool(settings.mongodb_uri),
            "mongodb_database_configured": bool(settings.mongodb_database),
            "templates_dir_exists": settings.templates_dir.exists(),
            "template_workbook_count": len(template_workbooks),
            "files_dir_exists": settings.files_dir.exists(),
            "files_gbm_dir_exists": (settings.files_dir / "gbm").exists(),
            "files_kras_dir_exists": (settings.files_dir / "kras").exists(),
            "files_other_dir_exists": (settings.files_dir / "other").exists(),
        },
        commands={"s3_inventory": build_aws_s3_inventory_command(s3_uri, settings)},
        steps=[
            "parse_s3_uri",
            "load_runtime_config",
            "check_aws_sso",
            "discover_s3_objects",
            "pull_mongodb_metadata",
            "resolve_templates",
            "resolve_project_files",
            "render_curated_workbooks",
        ],
    )
    (run_dir / "plan.json").write_text(json.dumps(plan.to_dict(), indent=2, sort_keys=True) + "\n")
    return plan


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="btc-manfiest",
        description="BTC metadata manifest curation workflow tools.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for generated run artifacts. Defaults to BTC_MANFIEST_OUTPUT_DIR or runs/.",
    )
    subparsers = parser.add_subparsers(dest="command")

    plan = subparsers.add_parser("plan", help="Create a local curation run plan.")
    plan.add_argument("s3_uri", help="S3 project root, for example s3://bucket/prefix/")

    return parser


def run_interactive(settings: Settings) -> None:
    print("Checking AWS SSO...")
    aws_ok, aws_message = check_aws_sso(settings)
    if not aws_ok:
        raise SystemExit(
            f"AWS SSO is not ready: {aws_message}\n"
            "Run `aws sso login --profile <profile>` or update AWS_PROFILE in `.env`."
        )
    print(f"AWS SSO OK: {aws_message}")

    s3_uri = input("Paste AWS S3 path: ").strip()
    parse_s3_uri(s3_uri)

    plan = build_plan(s3_uri, settings)
    inventory_path = plan.output_dir / "s3_inventory.tsv"
    print(f"Running: {plan.commands['s3_inventory']}")
    result = run_s3_inventory(plan.commands["s3_inventory"], inventory_path)
    if result.returncode != 0:
        raise SystemExit(f"S3 inventory failed: {result.stderr.strip()}")

    print(f"Wrote plan: {plan.output_dir / 'plan.json'}")
    print(f"Wrote S3 inventory: {inventory_path}")


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = Settings.from_env(output_dir=args.output_dir)

    if args.command == "plan":
        plan = build_plan(args.s3_uri, settings)
        print(json.dumps(plan.to_dict(), indent=2, sort_keys=True))
        return

    if args.command is None:
        run_interactive(settings)
        return

    parser.error(f"unknown command: {args.command}")


if __name__ == "__main__":
    main()
