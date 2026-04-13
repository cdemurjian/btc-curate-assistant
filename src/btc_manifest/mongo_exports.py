from __future__ import annotations

import os
import socket
import subprocess
from datetime import datetime
from pathlib import Path

from btc_manifest.config import Settings


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


def ensure_current_mongo_exports(settings: Settings) -> None:
    if not on_btc_vm():
        print("Not on BTC VM; using existing gitignored files under files/ if present.")
        return

    subject_path, biospecimen_path = mongo_export_paths(settings)
    if mongo_exports_are_current(settings):
        print(f"Mongo exports are current: {subject_path}, {biospecimen_path}")
        return

    print("On BTC VM and today's Mongo exports are missing; pulling Mongo reference files...")
    command = ["uv", "run", "--with", "pymongo", "pull-gbm-mongo", "export"]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.returncode != 0:
        raise SystemExit(
            "Mongo export failed. Check MONGODB_URI, MONGODB_DATABASE, and global-bundle.pem.\n"
            f"{result.stderr.strip()}"
        )
