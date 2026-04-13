from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def is_fake_file_path(file_path: str) -> bool:
    key = file_path.rstrip("/")
    name = key.rsplit("/", 1)[-1]
    return (
        not key
        or name == "."
        or name == ".DS_Store"
        or name.startswith("._")
        or file_path.endswith("/")
    )


def normalize_inventory_file_path(file_path: str, plan_data: dict[str, Any]) -> str:
    file_path = file_path.strip()
    if file_path.startswith("s3://"):
        return file_path
    bucket = plan_data["s3"]["bucket"]
    return f"s3://{bucket}/{file_path.lstrip('/')}"


def read_inventory_rows(plan_data: dict[str, Any]) -> list[dict[str, Any]]:
    inventory_path = Path(plan_data["commands"]["s3_inventory_csv"])
    rows: list[dict[str, Any]] = []
    lines = [line for line in inventory_path.read_text().splitlines() if line.strip()]
    if not lines:
        return rows

    delimiter = "\t" if "\t" in lines[0] and "," not in lines[0] else ","
    reader = csv.reader(lines, delimiter=delimiter)
    first = next(reader, None)
    if first is None:
        return rows

    has_header = len(first) >= 2 and first[0] == "file_path" and first[1] == "size"
    source_rows = reader if has_header else iter([first, *reader])
    for row in source_rows:
        if len(row) < 2:
            continue
        file_path = normalize_inventory_file_path(row[0], plan_data)
        if is_fake_file_path(file_path):
            continue
        rows.append({"file_path": file_path, "size": row[1].strip()})
    return rows


def file_extension(file_path: str) -> str:
    name = file_path.rstrip("/").rsplit("/", 1)[-1].lower()
    for extension in ("fastq.gz", "fq.gz", "vcf.gz", "tar.gz"):
        if name.endswith(f".{extension}"):
            return extension
    if "." not in name:
        return ""
    return name.rsplit(".", 1)[-1]


def data_level_for_file(file_path: str) -> str:
    return "1" if file_extension(file_path) in {"fastq.gz", "fq.gz"} else ""
