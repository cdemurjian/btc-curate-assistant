from __future__ import annotations

import os
import socket
import subprocess
from datetime import datetime
from pathlib import Path

from btc.common.config import Settings
from btc.curate.references import latest_reference_csv


def on_btc_vm() -> bool:
    override = os.getenv("BTC_CURATE_ASSISTANT_ON_VM")
    if override is not None:
        return override.lower() in {"1", "true", "yes", "y"}
    hostname = socket.gethostname().lower()
    return hostname.startswith("btc-") or "btc-prod" in hostname


def mongo_export_paths(settings: Settings) -> tuple[Path, Path]:
    today = datetime.now().strftime("%y%m%d")
    mongo_dir = settings.files_dir / "mongo"
    return mongo_dir / f"subject-{today}.csv", mongo_dir / f"biospecimen-{today}.csv"


def mongo_exports_are_current(settings: Settings) -> bool:
    subject_path, biospecimen_path = mongo_export_paths(settings)
    return subject_path.exists() and biospecimen_path.exists()


def cached_mongo_exports(settings: Settings) -> tuple[Path | None, Path | None]:
    return (
        latest_reference_csv(settings.files_dir, "subject"),
        latest_reference_csv(settings.files_dir, "biospecimen"),
    )


def mongo_is_configured(settings: Settings) -> bool:
    return bool(settings.mongodb_uri and settings.mongodb_database)


def use_cached_mongo_exports(settings: Settings, reason: str) -> bool:
    subject_path, biospecimen_path = cached_mongo_exports(settings)
    if subject_path and biospecimen_path:
        print(
            f"{reason}; using cached Mongo reference files: "
            f"{subject_path}, {biospecimen_path}"
        )
        return True
    return False


def ensure_current_mongo_exports(settings: Settings) -> None:
    if not on_btc_vm():
        print("Not on BTC VM; using existing gitignored reference files under data/reference/ if present.")
        return

    subject_path, biospecimen_path = mongo_export_paths(settings)
    if mongo_exports_are_current(settings):
        print(f"Mongo exports are current: {subject_path}, {biospecimen_path}")
        return

    if not mongo_is_configured(settings):
        if use_cached_mongo_exports(settings, "Mongo is not configured in .env"):
            return
        raise SystemExit(
            "Mongo is not configured in .env and no cached subject/biospecimen CSVs "
            "were found under data/reference/mongo/."
        )

    print("On BTC VM and today's Mongo exports are missing; pulling Mongo reference files...")
    command = ["uv", "run", "--with", "pymongo", "btc", "curate", "mongo", "export"]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.returncode != 0:
        if use_cached_mongo_exports(settings, "Mongo export failed"):
            return
        raise SystemExit(
            "Mongo export failed. Check MONGODB_URI, MONGODB_DATABASE, and global-bundle.pem.\n"
            f"{result.stderr.strip()}"
        )
