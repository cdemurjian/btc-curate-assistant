from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

from btc_manifest.config import load_dotenv


SUBJECT_PATTERN = "subject"
BIOSPECIMEN_PATTERN = "biospecimen"


SUBJECT_FIELDS = ["study", "subject", "subject_key", "subject_trial_id"]
BIOSPECIMEN_FIELDS = ["biospecimen_key", "biospecimen_trial_id", "subject_key"]


def require_dependencies() -> Any:
    try:
        from pymongo import MongoClient
    except ImportError as error:
        raise SystemExit(
            "Missing dependency. Run with:\n"
            "  uv run --with pymongo pull-gbm-mongo <command>\n"
        ) from error
    return MongoClient


def mongo_uri(args: argparse.Namespace) -> str:
    load_dotenv()
    uri = args.uri or os.getenv("MONGODB_URI")
    if not uri:
        raise SystemExit("MONGODB_URI is not set in .env and --uri was not provided.")
    return uri


def mongo_settings(args: argparse.Namespace) -> tuple[str, str]:
    uri = mongo_uri(args)
    database = args.database or os.getenv("MONGODB_DATABASE")
    if not database:
        raise SystemExit("MONGODB_DATABASE is not set in .env and --database was not provided.")
    return uri, database


def mongo_client(args: argparse.Namespace) -> Any:
    MongoClient = require_dependencies()
    client = MongoClient(mongo_uri(args), serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    return client


def mongo_database(args: argparse.Namespace) -> Any:
    uri, database = mongo_settings(args)
    MongoClient = require_dependencies()
    client = MongoClient(uri, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    return client[database]


def stringify(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return json.dumps(value, default=str, sort_keys=True)


def flattened(document: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in document.items():
        name = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            output.update(flattened(value, name))
        else:
            output[name] = stringify(value)
    return output


def candidate_collections(collections: list[str], pattern: str) -> list[str]:
    return [name for name in collections if pattern.lower() in name.lower()]


def list_collections(args: argparse.Namespace) -> None:
    db = mongo_database(args)
    for name in sorted(db.list_collection_names()):
        count = db[name].estimated_document_count()
        print(f"{name}\t{count}")


def list_databases(args: argparse.Namespace) -> None:
    client = mongo_client(args)
    for name in sorted(client.list_database_names()):
        print(name)


def discover(args: argparse.Namespace) -> None:
    db = mongo_database(args)
    names = sorted(db.list_collection_names())

    print("Collections:")
    for name in names:
        count = db[name].estimated_document_count()
        sample = db[name].find_one()
        keys = ", ".join(sorted(sample.keys())) if sample else "<empty>"
        print(f"- {name} ({count} docs): {keys}")

    subject_candidates = candidate_collections(names, SUBJECT_PATTERN)
    biospecimen_candidates = candidate_collections(names, BIOSPECIMEN_PATTERN)

    print("\nSubject candidates:")
    for name in subject_candidates or ["<none>"]:
        print(f"- {name}")

    print("\nBiospecimen candidates:")
    for name in biospecimen_candidates or ["<none>"]:
        print(f"- {name}")

    print("\nExport once you choose collections:")
    print(
        "uv run --with pymongo pull-gbm-mongo export "
        "--subject-collection subject --biospecimen-collection biospecimen"
    )


def dated_csv_path(out_dir: Path, stem: str) -> Path:
    return out_dir / f"{stem}-{datetime.now().strftime('%y%m%d')}.csv"


def nested_value(document: dict[str, Any], key: str) -> Any:
    value: Any = document
    for part in key.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return stringify(value)


def export_collection(
    db: Any,
    collection_name: str,
    output_path: Path,
    fields: list[str],
    limit: int | None,
) -> int:
    cursor = db[collection_name].find({}, {field: 1 for field in fields})
    if limit:
        cursor = cursor.limit(limit)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(fields)
        for document in cursor:
            writer.writerow([nested_value(document, field) for field in fields])
            count += 1
    return count


def export(args: argparse.Namespace) -> None:
    db = mongo_database(args)
    out_dir = args.out_dir
    subject_path = dated_csv_path(out_dir, "subject")
    biospecimen_path = dated_csv_path(out_dir, "biospecimen")

    subject_count = export_collection(
        db,
        args.subject_collection,
        subject_path,
        SUBJECT_FIELDS,
        args.limit,
    )
    biospecimen_count = export_collection(
        db,
        args.biospecimen_collection,
        biospecimen_path,
        BIOSPECIMEN_FIELDS,
        args.limit,
    )

    print(f"Wrote {subject_count} subject rows: {subject_path}")
    print(f"Wrote {biospecimen_count} biospecimen rows: {biospecimen_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Discover and export GBM Mongo metadata.")
    parser.add_argument("--uri", default=None, help="MongoDB URI. Defaults to MONGODB_URI.")
    parser.add_argument(
        "--database",
        default=None,
        help="MongoDB database. Defaults to MONGODB_DATABASE.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List Mongo collections and estimated counts.")
    subparsers.add_parser("databases", help="List Mongo databases.")
    subparsers.add_parser("discover", help="List collections, sample keys, and likely candidates.")

    export_parser = subparsers.add_parser("export", help="Export selected collections to CSV.")
    export_parser.add_argument("--subject-collection", default="subject")
    export_parser.add_argument("--biospecimen-collection", default="biospecimen")
    export_parser.add_argument("--out-dir", type=Path, default=Path("files/mongo"))
    export_parser.add_argument("--limit", type=int, default=None)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "databases":
        list_databases(args)
    elif args.command == "list":
        list_collections(args)
    elif args.command == "discover":
        discover(args)
    elif args.command == "export":
        export(args)
    else:
        parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
