from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


@dataclass(frozen=True)
class Settings:
    mongodb_uri: str | None
    mongodb_database: str | None
    aws_profile: str | None
    aws_region: str | None
    output_dir: Path
    templates_dir: Path
    files_dir: Path

    @classmethod
    def from_env(cls, output_dir: Path | None = None) -> "Settings":
        load_dotenv()
        return cls(
            mongodb_uri=os.getenv("MONGODB_URI"),
            mongodb_database=os.getenv("MONGODB_DATABASE"),
            aws_profile=os.getenv("AWS_PROFILE"),
            aws_region=os.getenv("AWS_REGION"),
            output_dir=output_dir or Path(os.getenv("BTC_MANFIEST_OUTPUT_DIR", "runs")),
            templates_dir=Path(os.getenv("BTC_MANFIEST_TEMPLATES_DIR", "templates")),
            files_dir=Path(os.getenv("BTC_MANFIEST_FILES_DIR", "files")),
        )
