from __future__ import annotations

import csv
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any


FUZZY_MATCH_THRESHOLD = 0.85


@dataclass(frozen=True)
class ReferenceMatch:
    query: str
    value: str
    score: float
    row: dict[str, str]


def latest_reference_csv(files_dir: Path, stem: str) -> Path | None:
    mongo_dir = files_dir / "mongo"
    candidates = sorted(mongo_dir.glob(f"{stem}-*.csv"), reverse=True)
    candidates.append(mongo_dir / f"{stem}.csv")
    return next((path for path in candidates if path.exists()), None)


def read_reference_csv(path: Path | None) -> list[dict[str, str]]:
    if path is None:
        return []
    with path.open(newline="") as input_file:
        return [dict(row) for row in csv.DictReader(input_file)]


def load_subject_references(files_dir: Path, study: str | None = None) -> list[dict[str, str]]:
    rows = read_reference_csv(latest_reference_csv(files_dir, "subject"))
    if study:
        return [row for row in rows if row.get("study") == study]
    return rows


def load_biospecimen_references(files_dir: Path) -> list[dict[str, str]]:
    return read_reference_csv(latest_reference_csv(files_dir, "biospecimen"))


def exact_reference_match(
    refs: list[dict[str, str]],
    field: str,
    value: str,
) -> dict[str, str] | None:
    if not value:
        return None
    return next((row for row in refs if row.get(field) == value), None)


def fuzzy_reference_matches(
    refs: list[dict[str, str]],
    field: str,
    value: str,
    threshold: float = FUZZY_MATCH_THRESHOLD,
    limit: int = 3,
) -> list[ReferenceMatch]:
    if not value:
        return []

    matches: list[ReferenceMatch] = []
    query = value.lower()
    for row in refs:
        candidate = row.get(field, "")
        if not candidate:
            continue
        score = SequenceMatcher(None, query, candidate.lower()).ratio()
        if score >= threshold:
            matches.append(ReferenceMatch(value, candidate, score, row))

    return sorted(matches, key=lambda match: match.score, reverse=True)[:limit]


def reviewed_id_map(plan_data: dict[str, Any]) -> dict[str, dict[str, str]]:
    return plan_data.get("biospecimenfile_id_map", {})
