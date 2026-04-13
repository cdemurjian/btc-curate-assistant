from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from shlex import quote
from urllib.parse import urlparse

from btc_manifest.config import Settings


@dataclass(frozen=True)
class S3Location:
    bucket: str
    prefix: str

    def to_uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}" if self.prefix else f"s3://{self.bucket}"


def normalize_s3_uri(uri: str) -> str:
    parsed = urlparse(uri.strip())
    if parsed.scheme == "s3":
        return uri.strip()
    if parsed.scheme in {"http", "https"}:
        host_parts = parsed.netloc.split(".")
        if len(host_parts) >= 5 and host_parts[1] == "s3" and host_parts[3] == "amazonaws":
            bucket = host_parts[0]
            prefix = parsed.path.lstrip("/")
            return f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"
    return uri.strip()


def parse_s3_uri(uri: str) -> S3Location:
    uri = normalize_s3_uri(uri)
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"expected an s3:// URI, got {uri!r}")
    if not parsed.netloc:
        raise ValueError(f"S3 URI is missing a bucket: {uri!r}")
    return S3Location(bucket=parsed.netloc, prefix=parsed.path.lstrip("/"))


def build_aws_s3_inventory_command(s3_uri: str, settings: Settings) -> str:
    s3_uri = normalize_s3_uri(s3_uri)
    s3_location = parse_s3_uri(s3_uri)
    s3_base = f"s3://{s3_location.bucket}/"
    aws_parts = ["aws", "s3", "ls", quote(s3_uri), "--recursive"]
    if settings.aws_profile:
        aws_parts.extend(["--profile", quote(settings.aws_profile)])
    aws_command = " ".join(aws_parts)
    awk_command = (
        f"awk -v base={quote(s3_base)} "
        "'BEGIN {print \"file_path,size\"} "
        "{key=$4; for (i=5; i<=NF; i++) key=key \" \" $i} "
        "key == \"\" {next} "
        "key ~ /(^|\\/)\\.DS_Store$/ {next} "
        "key ~ /(^|\\/)\\._/ {next} "
        "key ~ /(^|\\/)\\.$/ {next} "
        "key ~ /\\/$/ {next} "
        "{print base key \",\" $3}'"
    )
    return f"{aws_command} | {awk_command}"


def check_aws_sso(settings: Settings) -> tuple[bool, str]:
    if shutil.which("aws") is None:
        return False, "aws CLI was not found on PATH. Install or activate aws-cli first."
    if not settings.aws_profile:
        return False, "AWS_PROFILE is not set in .env"

    profiles_result = subprocess.run(
        ["aws", "configure", "list-profiles"],
        capture_output=True,
        text=True,
        check=False,
    )
    if profiles_result.returncode != 0:
        message = (profiles_result.stderr or profiles_result.stdout).strip()
        return False, message or "Could not list AWS CLI profiles"

    profiles = {profile.strip() for profile in profiles_result.stdout.splitlines() if profile.strip()}
    if settings.aws_profile not in profiles:
        available = ", ".join(sorted(profiles)) or "none"
        return (
            False,
            f"AWS_PROFILE={settings.aws_profile!r} is not configured. "
            f"Available profiles: {available}.",
        )

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
