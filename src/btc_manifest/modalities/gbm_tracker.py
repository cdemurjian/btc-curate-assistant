from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from difflib import SequenceMatcher
from pathlib import Path
from zipfile import ZipFile

XLSX_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
PATIENT_RE = re.compile(r"(?<![A-Z0-9])(DFCI|MSK|JHU)[_\-\s]*(\d+)(?![A-Z0-9])", re.IGNORECASE)


def normalize_token(token: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", token.upper())


def normalize_patient(patient: str) -> str:
    match = PATIENT_RE.search(patient.upper())
    if not match:
        return patient.upper()
    return f"{match.group(1).upper()}{match.group(2)}"


def _cell_column(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha())
    column = 0
    for char in letters:
        column = column * 26 + (ord(char.upper()) - 64)
    return column - 1


def _shared_strings(zf: ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    values: list[str] = []
    for si in root.findall(f"{{{XLSX_MAIN_NS}}}si"):
        texts = [text.text or "" for text in si.findall(f".//{{{XLSX_MAIN_NS}}}t")]
        values.append("".join(texts))
    return values


def _worksheet_rows(path: Path) -> list[list[str]]:
    with ZipFile(path) as zf:
        shared = _shared_strings(zf)
        root = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
        rows: list[list[str]] = []
        for row in root.findall(f".//{{{XLSX_MAIN_NS}}}row"):
            values: list[str] = []
            for cell in row.findall(f"{{{XLSX_MAIN_NS}}}c"):
                column = _cell_column(cell.attrib.get("r", ""))
                while len(values) <= column:
                    values.append("")
                value = cell.find(f"{{{XLSX_MAIN_NS}}}v")
                text = "" if value is None else value.text or ""
                if cell.attrib.get("t") == "s" and text:
                    text = shared[int(text)]
                values[column] = text
            rows.append(values)
        return rows


def gbm_tracker_rows(plan_data: dict[str, object], cache_key: str) -> list[dict[str, str]]:
    cache = plan_data.setdefault("_modality_cache", {})
    if not isinstance(cache, dict):
        cache = {}
        plan_data["_modality_cache"] = cache
    if cache_key in cache:
        return cache[cache_key]

    files_dir = Path(str(plan_data["files_dir"]))
    tracker_path = files_dir / "gbm" / "GBM-master-list.xlsx"
    if not tracker_path.exists():
        cache[cache_key] = []
        return []

    raw_rows = _worksheet_rows(tracker_path)
    if not raw_rows:
        cache[cache_key] = []
        return []

    headers = raw_rows[0]
    tracker_rows: list[dict[str, str]] = []
    for raw_row in raw_rows[1:]:
        row = {
            header: raw_row[index] if index < len(raw_row) else ""
            for index, header in enumerate(headers)
            if header
        }
        if row.get("Original_Vial_Label"):
            tracker_rows.append(row)

    cache[cache_key] = tracker_rows
    return tracker_rows


def tracker_rows_for_patient(plan_data: dict[str, object], patient: str) -> list[dict[str, str]]:
    normalized_patient = normalize_patient(patient)
    return [
        row
        for row in gbm_tracker_rows(plan_data, "gbm_tracker_rows")
        if normalize_patient(row.get("Patient_ID", "")) == normalized_patient
    ]


def closest_tracker_patients(
    plan_data: dict[str, object],
    query: str,
    limit: int = 5,
) -> list[tuple[str, float]]:
    normalized_query = normalize_patient(query)
    patients = sorted(
        {
            normalize_patient(row.get("Patient_ID", ""))
            for row in gbm_tracker_rows(plan_data, "gbm_tracker_rows")
            if row.get("Patient_ID")
        }
    )
    matches = [
        (
            patient,
            SequenceMatcher(None, normalize_token(normalized_query), normalize_token(patient)).ratio(),
        )
        for patient in patients
    ]
    return sorted(matches, key=lambda item: item[1], reverse=True)[:limit]


def tracker_row_summary(row: dict[str, str]) -> str:
    details = [
        row.get("Original_Timepoint", ""),
        row.get("Original_Location", ""),
        f"C{row.get('Original_Core_Number', '')}" if row.get("Original_Core_Number") else "",
    ]
    detail_text = " / ".join(part for part in details if part)
    if detail_text:
        return f"{row.get('Original_Vial_Label', '')} ({detail_text})"
    return row.get("Original_Vial_Label", "")
