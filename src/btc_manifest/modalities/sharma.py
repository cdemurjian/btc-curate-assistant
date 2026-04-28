from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

from .gbm_tracker import gbm_tracker_rows, normalize_patient, normalize_token


@dataclass(frozen=True)
class SharmaParsed:
    tail: str
    parent: str
    patient: str
    surgery: str
    location: str
    core: str
    assay: str


PATIENT_RE = re.compile(r"(?<![A-Z0-9])(DFCI|MSK|JHU)[_\-\s]*(\d+)(?![A-Z0-9])", re.IGNORECASE)
SURGERY_RE = re.compile(r"(?:^|[_\-.])S(\d+)(?:$|[_\-.])", re.IGNORECASE)
LOCATION_RE = re.compile(r"(?:^|[_\-.])L(\d+[A-Z]?)(?:$|[_\-.])", re.IGNORECASE)
CORE_RE = re.compile(r"(?:^|[_\-.])C(\d+)(?:$|[_\-.])", re.IGNORECASE)
LEGACY_BTC_GBM_RE = re.compile(r"^BTC[_\-]GBM[_\-]", re.IGNORECASE)

def _assay_from_path(file_path: str, tail: str) -> str:
    for text in (tail, file_path):
        normalized = text.upper()
        if "GEX" in normalized:
            return "scRNA"
        if "BCR" in normalized:
            return "BCR"
        if "TCR" in normalized:
            return "TCR"
    return ""


def _extract_tail(file_path: str) -> tuple[str, str]:
    parts = [part for part in file_path.split("/") if part]
    if len(parts) < 2:
        return file_path, file_path
    parent = parts[-2]
    filename = parts[-1]
    return f"{parent}/{filename}", parent


def should_skip_biospecimenfile_mapping(
    file_path: str,
    plan_data: dict[str, Any],
) -> bool:
    _, parent = _extract_tail(file_path)
    return bool(LEGACY_BTC_GBM_RE.match(parent))


def parse_file_path(file_path: str) -> SharmaParsed:
    tail, parent = _extract_tail(file_path)
    normalized_parent = parent.upper().replace("-", "_").replace(".", "_")

    if LEGACY_BTC_GBM_RE.match(parent):
        patient_match = None
    else:
        patient_match = PATIENT_RE.search(normalized_parent)

    surgery_match = SURGERY_RE.search(f"_{normalized_parent}_")
    location_match = LOCATION_RE.search(f"_{normalized_parent}_")

    # Core: explicit C# in parent dir only. Not encoded in old BTC-GBM-NNN-NNN format.
    core_match = CORE_RE.search(f"_{normalized_parent}_")
    core_str = f"C{core_match.group(1)}" if core_match else ""

    return SharmaParsed(
        tail=tail,
        parent=parent,
        patient=(normalize_patient(patient_match.group(0)) if patient_match else ""),
        surgery=(f"S{surgery_match.group(1)}" if surgery_match else ""),
        location=(f"L{location_match.group(1).upper()}" if location_match else ""),
        core=core_str,
        assay=_assay_from_path(file_path, tail),
    )


def review_group_key_for_file(file_path: str, plan_data: dict[str, Any]) -> str:
    parsed = parse_file_path(file_path)
    if should_skip_biospecimenfile_mapping(file_path, plan_data):
        return parsed.parent
    if parsed.patient or parsed.surgery or parsed.location or parsed.core:
        return "|".join(
            [
                parsed.patient or "<patient>",
                parsed.surgery or "<surgery>",
                parsed.location or "",  # location is optional — blank is fine
                parsed.core or "<core>",
            ]
        )
    return parsed.parent


def assay_for_file(file_path: str, plan_data: dict[str, Any]) -> str | None:
    parsed = parse_file_path(file_path)
    return parsed.assay or None

def _gbm_tracker_rows(plan_data: dict[str, Any]) -> list[dict[str, str]]:
    return gbm_tracker_rows(plan_data, "gbm_tracker_rows")


def _score_biospecimen(parsed: SharmaParsed, candidate: str) -> float:
    normalized_candidate = normalize_token(candidate)
    score = 0.0
    if parsed.patient and normalize_token(parsed.patient) in normalized_candidate:
        score += 3.0
    if parsed.surgery and normalize_token(parsed.surgery) in normalized_candidate:
        score += 3.0
    if parsed.location and normalize_token(parsed.location) in normalized_candidate:
        score += 2.0
    if parsed.core and normalize_token(parsed.core) in normalized_candidate:
        score += 3.0
    if parsed.assay and normalize_token(parsed.assay) in normalized_candidate:
        score += 1.0
    parent_norm = normalize_token(parsed.parent)
    if parent_norm:
        score += 2.0 * SequenceMatcher(None, parent_norm, normalized_candidate).ratio()
    return score


def _tracker_row_matches(
    row: dict[str, str],
    patient: str,
    surgery: str,
    location: str,
    core: str,
) -> bool:
    if patient and normalize_patient(row.get("Patient_ID", "")) != normalize_patient(patient):
        return False
    if surgery and row.get("Original_Timepoint", "").upper() != surgery.upper():
        return False
    if location and row.get("Original_Location", "").upper() != location.upper():
        return False
    if core:
        core_number = core.upper().removeprefix("C")
        if row.get("Original_Core_Number", "").upper() != core_number:
            return False
    return True


def _tracker_candidates(
    plan_data: dict[str, Any],
    patient: str,
    surgery: str,
    location: str,
    core: str,
) -> list[dict[str, str]]:
    return [
        row
        for row in _gbm_tracker_rows(plan_data)
        if _tracker_row_matches(row, patient, surgery, location, core)
    ]


def biospecimen_candidates_for_group(
    group_parts: list[str],
    plan_data: dict[str, Any],
) -> list[str]:
    patient, surgery, location, core = (group_parts + ["", "", "", ""])[:4]
    patient = "" if patient.startswith("<") else normalize_patient(patient)
    surgery = "" if surgery.startswith("<") else surgery
    location = "" if location.startswith("<") else location
    core = "" if core.startswith("<") else core
    return [
        row["Original_Vial_Label"]
        for row in _tracker_candidates(plan_data, patient, surgery, location, core)
        if row.get("Original_Vial_Label")
    ]


def propose_biospecimenfile_ids_for_row(
    file_path: str,
    plan_data: dict[str, Any],
) -> tuple[str, str]:
    if should_skip_biospecimenfile_mapping(file_path, plan_data):
        return "", ""

    parsed = parse_file_path(file_path)
    tracker_candidates = _tracker_candidates(
        plan_data,
        parsed.patient,
        parsed.surgery,
        parsed.location,
        parsed.core,
    )
    if len(tracker_candidates) == 1:
        return parsed.patient, tracker_candidates[0].get("Original_Vial_Label", "")

    best_biospecimen = ""
    best_score = 0.0
    for row in tracker_candidates or _gbm_tracker_rows(plan_data):
        biospecimen_trial_id = row.get("Original_Vial_Label", "")
        score = _score_biospecimen(parsed, biospecimen_trial_id)
        if score > best_score:
            best_score = score
            best_biospecimen = biospecimen_trial_id

    return parsed.patient, best_biospecimen
