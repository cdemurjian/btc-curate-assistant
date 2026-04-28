from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any

from .gbm_tracker import (
    closest_tracker_patients,
    normalize_patient,
    normalize_token,
    tracker_row_summary,
    tracker_rows_for_patient,
)

MANIFEST_DEFAULTS = {
    "assays": ["bulkMS"],
    "study": "GBM_Trial1",
    "lab": "White",
    "panel": "",
    "platform": "Exploris480",
    "vendor": "ThermoFisher",
}

PHOSPHO_TOKEN_RE = re.compile(r"(^|[^A-Z0-9])(PY|PHOS)([^A-Z0-9]|$)")


def _file_panel_bucket(file_path: str) -> str:
    normalized = file_path.upper()
    if "MHC" in normalized:
        return "mhc"
    if "GLOBAL" in normalized:
        return "global"
    if PHOSPHO_TOKEN_RE.search(normalized):
        return "phospho"
    return "unknown"


def panel_for_file(file_path: str, plan_data: dict[str, Any]) -> str | None:
    del plan_data
    bucket = _file_panel_bucket(file_path)
    if bucket == "mhc":
        return "immunoPep"
    if bucket == "global":
        return "globalProt"
    if bucket == "phospho":
        return "phosphoProt"
    return None


def assay_for_file(file_path: str, plan_data: dict[str, Any]) -> str | None:
    del file_path, plan_data
    return "bulkMS"


def review_group_key_for_file(file_path: str, plan_data: dict[str, Any]) -> str:
    del plan_data
    parts = [part for part in file_path.split("/") if part]
    if len(parts) < 2:
        return _file_panel_bucket(file_path)
    return f"{_file_panel_bucket(file_path)}|{parts[-2]}"


def _select_patient(plan_data: dict[str, Any]) -> str:
    while True:
        query = input("Which patient is this?: ").strip()
        if not query:
            print("Enter a patient ID.")
            continue

        matches = closest_tracker_patients(plan_data, query)
        if not matches:
            print("No GBM tracker patient matches found.")
            continue

        print("Closest patient matches:")
        for index, (patient, score) in enumerate(matches, start=1):
            print(f"  {index}. {patient} ({score:.0%})")

        answer = input("Select number or type the patient ID: ").strip()
        if answer.isdigit() and 1 <= int(answer) <= len(matches):
            return matches[int(answer) - 1][0]

        typed = normalize_patient(answer or query)
        if tracker_rows_for_patient(plan_data, typed):
            return typed

        print("Choose one of the listed patients or type a valid patient ID.")


def _choose_sample_rows(
    rows: list[dict[str, str]],
    prompt: str,
) -> list[dict[str, str]] | None:
    print(prompt)
    for index, row in enumerate(rows, start=1):
        print(f"  {index}. {tracker_row_summary(row)}")
    print("  Type `back` to return to the previous step.")

    while True:
        answer = input("> ").strip()
        if answer.lower() == "back":
            return None
        if not answer:
            return []

        selected: list[dict[str, str]] = []
        valid = True
        for part in answer.replace(",", " ").split():
            if not part.isdigit():
                valid = False
                break
            index = int(part)
            if not (1 <= index <= len(rows)):
                valid = False
                break
            selected.append(rows[index - 1])

        if valid:
            deduped: list[dict[str, str]] = []
            seen: set[str] = set()
            for row in selected:
                label = row.get("Original_Vial_Label", "")
                if label and label not in seen:
                    seen.add(label)
                    deduped.append(row)
            return deduped

        print("Enter space-separated sample numbers.")


def _score_selected_sample(file_path: str, row: dict[str, str]) -> float:
    parts = [part for part in file_path.split("/") if part]
    parent = parts[-2] if len(parts) >= 2 else file_path
    tokens = [
        parent,
        row.get("Original_Vial_Label", ""),
        row.get("Original_Timepoint", ""),
        row.get("Original_Location", ""),
        f"C{row.get('Original_Core_Number', '')}" if row.get("Original_Core_Number") else "",
    ]
    haystack = normalize_token(" ".join(tokens[1:]))
    needle = normalize_token(tokens[0])
    return SequenceMatcher(None, needle, haystack).ratio()


def _selected_labels(rows: list[dict[str, str]]) -> list[str]:
    return [row.get("Original_Vial_Label", "") for row in rows if row.get("Original_Vial_Label")]


def available_samples_for_patient(
    plan_data: dict[str, Any],
    patient: str,
) -> list[dict[str, str]]:
    return tracker_rows_for_patient(plan_data, patient)


def build_selection_state(
    patient: str,
    mass_spec_rows: list[dict[str, str]],
    mhc_rows: list[dict[str, str]],
) -> dict[str, Any]:
    return {
        "patient": normalize_patient(patient),
        "mass_spec_samples": _selected_labels(mass_spec_rows),
        "mhc_samples": _selected_labels(mhc_rows),
    }


def _rows_by_label(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {
        row.get("Original_Vial_Label", ""): row
        for row in rows
        if row.get("Original_Vial_Label")
    }


def build_proposed_pairs_from_selection(
    inventory_rows: list[dict[str, Any]],
    patient: str,
    patient_rows: list[dict[str, str]],
    mass_spec_labels: list[str],
    mhc_labels: list[str],
) -> dict[str, tuple[str, str]]:
    labels_to_rows = _rows_by_label(patient_rows)
    mass_spec_rows = [labels_to_rows[label] for label in mass_spec_labels if label in labels_to_rows]
    mhc_rows = [labels_to_rows[label] for label in mhc_labels if label in labels_to_rows]
    normalized_patient = normalize_patient(patient)

    selected = {
        "mhc": mhc_rows,
        "global": mass_spec_rows,
        "phospho": mass_spec_rows,
        "unknown": mass_spec_rows,
    }

    proposed_pairs: dict[str, tuple[str, str]] = {}
    for row in inventory_rows:
        file_path = row["file_path"]
        bucket = _file_panel_bucket(file_path)
        candidate_rows = selected.get(bucket, [])
        if not candidate_rows:
            proposed_pairs[file_path] = (normalized_patient, "")
            continue

        best_row = max(candidate_rows, key=lambda candidate: _score_selected_sample(file_path, candidate))
        proposed_pairs[file_path] = (normalized_patient, best_row.get("Original_Vial_Label", ""))
    return proposed_pairs


def build_proposed_pairs(
    plan_data: dict[str, Any],
    inventory_rows: list[dict[str, Any]],
) -> tuple[dict[str, tuple[str, str]], list[str]] | None:
    cached_selection = plan_data.get("_modality_cache", {}).get("white_proteomics_selection")
    if cached_selection:
        patient = normalize_patient(cached_selection.get("patient", ""))
        patient_rows = tracker_rows_for_patient(plan_data, patient)
        if patient_rows:
            return (
                build_proposed_pairs_from_selection(
                    inventory_rows,
                    patient,
                    patient_rows,
                    list(cached_selection.get("mass_spec_samples", [])),
                    list(cached_selection.get("mhc_samples", [])),
                ),
                [],
            )

    while True:
        patient = _select_patient(plan_data)
        patient_rows = tracker_rows_for_patient(plan_data, patient)
        if not patient_rows:
            print(f"No GBM tracker samples found for {patient}.")
            continue

        mass_spec_rows = _choose_sample_rows(
            patient_rows,
            "For pY/PHOS/global files, which samples should be associated? "
            "(space-separated numbers, blank for none)",
        )
        if mass_spec_rows is None:
            continue

        while True:
            mhc_rows = _choose_sample_rows(
                patient_rows,
                "For MHC files, which samples should be associated? "
                "(space-separated numbers, blank for none)",
            )
            if mhc_rows is None:
                mass_spec_rows = _choose_sample_rows(
                    patient_rows,
                    "For pY/PHOS/global files, which samples should be associated? "
                    "(space-separated numbers, blank for none)",
                )
                if mass_spec_rows is None:
                    break
                continue
            break

        if mass_spec_rows is None or mhc_rows is None:
            continue
        break

    plan_data.setdefault("_modality_cache", {})["white_proteomics_selection"] = build_selection_state(
        patient,
        mass_spec_rows,
        mhc_rows,
    )
    return (
        build_proposed_pairs_from_selection(
            inventory_rows,
            patient,
            patient_rows,
            _selected_labels(mass_spec_rows),
            _selected_labels(mhc_rows),
        ),
        [],
    )
