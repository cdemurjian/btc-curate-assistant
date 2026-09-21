"""Levine-lab modality (GBM7/8/9 bulk DNA + RNA).

Files for this modality are staged by ``stage_gbm789.sh`` under::

    s3://btc-gbm/staging/trial1a/levine_lab/<GBM7|GBM8|GBM9>/<bulkDNA|bulkRNA>/<file>

where ``<file>`` is named ``251107BTC_<D##-library>_...`` (paired fastqs plus
per-library QC reports). The biospecimen identity is *not* encoded in the file
name; it is carried by the ``D##`` library code. A per-run mapping CSV
(``levine_lab_library_map.csv``) joins each library code to its sample:

    library_code,analyte,sample_name,gbm_patient,surgery,core,location,subject_trial_id,flowcell,notes
    D26-148013,bulkDNA,GBM8.6.44,GBM8,S6,C4,L4,DFCI6,S,

Sample names decode as ``<gbm_patient>.<surgery>.<core[+location]>``: a one-digit
trailing token is the core (e.g. ``GBM7.1.4`` -> S1/C4); a two-digit token is
core + location (e.g. ``GBM8.6.44`` -> S6/C4/L4). That decoding is materialised
into the CSV columns so it is verified once, not re-parsed at runtime.

Identifiers emitted:

* ``subject_trial_id``       canonical BTC subject (GBM7->JHU1, GBM8->DFCI6, GBM9->DFCI7)
* ``biospecimen_trial_id``   constructed ``GBM1.<subject>.<surgery>[.<location>].<core>``
                             (location only when a surgery/core has multiple locations)
* ``parent_biospecimen``     official vial label from the GBM master tracker (fuzzy lookup)
* ``parent_biospecimen_raw`` the raw lab sample name (e.g. ``GBM8.6.44``)

Instrument is taken from the flowcell run code (``...E`` -> Element Aviti,
``...S`` -> Singular G4) via per-file ``platform_for_file`` / ``vendor_for_file``
hooks. Panel is intentionally left blank.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any

from .gbm_tracker import gbm_tracker_rows, normalize_patient

MANIFEST_DEFAULTS = {
    "assays": ["bulkDNA", "bulkRNA"],
    "study": "GBM_Trial1",
    "lab": "Levine",
    "panel": "",
    "platform": "",
    "vendor": "",
}

# Levine-lab patient label -> canonical BTC subject_trial_id. The mapping CSV's
# ``subject_trial_id`` column takes precedence; this is the fallback.
PATIENT_TO_SUBJECT = {
    "GBM7": "JHU1",
    "GBM8": "DFCI6",
    "GBM9": "DFCI7",
}

STUDY_PREFIX = "GBM1"

# Flowcell run-code suffix -> (platform, vendor).
FLOWCELL_INSTRUMENT = {
    "E": ("Aviti", "Element"),
    "S": ("G4", "Singular"),
}

LIBRARY_RE = re.compile(r"D\d{2}-\d+")
# Flowcell token as embedded in multi-run report names, e.g. "...sequence_fastqc.7441E.html".
FLOWCELL_TOKEN_RE = re.compile(r"\.\d{3,4}([A-Za-z])\.")
_MAP_CACHE_KEY = "levine_lab_library_map"
_EXTRAS_CACHE_KEY = "levine_lab_biospecimen_extras"
_MAP_PATH_OVERRIDE_KEY = "levine_lab_library_map_path"
_DEFAULT_MAP_NAME = "levine_lab_library_map.csv"


def _library_code(file_path: str) -> str:
    match = LIBRARY_RE.search(file_path)
    return match.group(0) if match else ""


def _analyte_from_path(file_path: str) -> str:
    lowered = file_path.lower()
    if "/bulkdna/" in lowered:
        return "bulkDNA"
    if "/bulkrna/" in lowered:
        return "bulkRNA"
    code = _library_code(file_path)
    if code.startswith("D26-148"):
        return "bulkDNA"
    return "bulkRNA" if code else ""


def _map_search_paths(plan_data: dict[str, Any]):
    override = plan_data.get(_MAP_PATH_OVERRIDE_KEY)
    if override:
        yield Path(str(override))
    output_dir = plan_data.get("output_dir")
    if output_dir:
        yield Path(str(output_dir)) / "library_map.csv"
    files_dir = plan_data.get("files_dir")
    if files_dir:
        yield Path(str(files_dir)) / "levine_lab" / "library_map.csv"
    # Repo-tracked default written alongside stage_gbm789.sh.
    yield Path(__file__).resolve().parents[4] / _DEFAULT_MAP_NAME


def _modality_cache(plan_data: dict[str, Any]) -> dict[str, Any]:
    cache = plan_data.setdefault("_modality_cache", {})
    if not isinstance(cache, dict):
        cache = {}
        plan_data["_modality_cache"] = cache
    return cache


def _load_library_map(plan_data: dict[str, Any]) -> dict[str, dict[str, str]]:
    cache = _modality_cache(plan_data)
    if _MAP_CACHE_KEY in cache:
        return cache[_MAP_CACHE_KEY]

    mapping: dict[str, dict[str, str]] = {}
    for path in _map_search_paths(plan_data):
        if not path.exists():
            continue
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                code = (row.get("library_code") or "").strip()
                if not code:
                    continue
                mapping[code] = {key: (value or "").strip() for key, value in row.items()}
        break

    cache[_MAP_CACHE_KEY] = mapping
    return mapping


def _row_for_file(file_path: str, plan_data: dict[str, Any]) -> dict[str, str] | None:
    code = _library_code(file_path)
    if not code:
        return None
    return _load_library_map(plan_data).get(code)


def _subject_for_row(row: dict[str, str]) -> str:
    return row.get("subject_trial_id", "") or PATIENT_TO_SUBJECT.get(
        row.get("gbm_patient", "").upper(), ""
    )


def _prefixed(value: str, prefix: str) -> str:
    value = value.strip().upper()
    if not value:
        return ""
    return value if value.startswith(prefix) else f"{prefix}{value}"


def _biospecimen_trial_id(subject: str, surgery: str, core: str, location: str) -> str:
    parts = [STUDY_PREFIX, subject, surgery, location, core]
    return ".".join(part for part in parts if part)


def _tracker_vial_label(
    plan_data: dict[str, Any],
    subject: str,
    surgery: str,
    core: str,
    location: str,
) -> str:
    """Official vial label from the GBM master tracker when exactly one row matches."""
    if not subject:
        return ""
    norm_subject = normalize_patient(subject)
    core_number = core.removeprefix("C")
    candidates: list[str] = []
    for row in gbm_tracker_rows(plan_data, "gbm_tracker_rows"):
        if normalize_patient(row.get("Patient_ID", "")) != norm_subject:
            continue
        if surgery and row.get("Original_Timepoint", "").upper() != surgery.upper():
            continue
        if location and row.get("Original_Location", "").upper() != location.upper():
            continue
        if core_number and row.get("Original_Core_Number", "").upper() != core_number.upper():
            continue
        label = row.get("Original_Vial_Label", "")
        if label and label not in candidates:
            candidates.append(label)
    return candidates[0] if len(candidates) == 1 else ""


def _row_identifiers(row: dict[str, str]) -> tuple[str, str, str, str, str]:
    subject = _subject_for_row(row)
    surgery = _prefixed(row.get("surgery", ""), "S")
    core = _prefixed(row.get("core", ""), "C")
    location = _prefixed(row.get("location", ""), "L")
    biospecimen = _biospecimen_trial_id(subject, surgery, core, location)
    return subject, biospecimen, surgery, core, location


def propose_biospecimenfile_ids_for_row(
    file_path: str,
    plan_data: dict[str, Any],
) -> tuple[str, str]:
    row = _row_for_file(file_path, plan_data)
    if not row:
        return "", ""
    subject, biospecimen, *_ = _row_identifiers(row)
    return subject, biospecimen


def _biospecimen_extras_index(plan_data: dict[str, Any]) -> dict[str, dict[str, str]]:
    cache = _modality_cache(plan_data)
    if _EXTRAS_CACHE_KEY in cache:
        return cache[_EXTRAS_CACHE_KEY]

    index: dict[str, dict[str, str]] = {}
    for row in _load_library_map(plan_data).values():
        subject, biospecimen, surgery, core, location = _row_identifiers(row)
        if not biospecimen:
            continue
        index[biospecimen] = {
            "parent_biospecimen": _tracker_vial_label(
                plan_data, subject, surgery, core, location
            ),
            "parent_biospecimen_raw": row.get("sample_name", ""),
        }
    cache[_EXTRAS_CACHE_KEY] = index
    return index


def biospecimen_row_extras(biospecimen_trial_id: str, plan_data: dict[str, Any]) -> dict[str, str]:
    """parent_biospecimen / parent_biospecimen_raw for a constructed biospecimen_trial_id."""
    return _biospecimen_extras_index(plan_data).get(biospecimen_trial_id, {})


def review_group_key_for_file(file_path: str, plan_data: dict[str, Any]) -> str:
    """Group by sample name so a library's fastqs + reports (DNA and RNA) review together."""
    row = _row_for_file(file_path, plan_data)
    if row and row.get("sample_name"):
        return row["sample_name"]
    parts = [part for part in file_path.split("/") if part]
    return parts[-2] if len(parts) >= 2 else file_path


def assay_for_file(file_path: str, plan_data: dict[str, Any]) -> str | None:
    del plan_data
    return _analyte_from_path(file_path) or None


def _flowcell_for_file(file_path: str, plan_data: dict[str, Any]) -> str:
    # A flowcell token embedded in a (multi-run) report name is the most specific signal.
    match = FLOWCELL_TOKEN_RE.search(file_path)
    if match:
        return match.group(1).upper()
    row = _row_for_file(file_path, plan_data)
    if row and row.get("flowcell"):
        return row["flowcell"].strip().upper()
    # Fall back to the batch convention: DNA on Singular (S), RNA on Element (E).
    analyte = _analyte_from_path(file_path)
    if analyte == "bulkDNA":
        return "S"
    return "E" if analyte == "bulkRNA" else ""


def platform_for_file(file_path: str, plan_data: dict[str, Any]) -> str | None:
    return FLOWCELL_INSTRUMENT.get(_flowcell_for_file(file_path, plan_data), (None, None))[0]


def vendor_for_file(file_path: str, plan_data: dict[str, Any]) -> str | None:
    return FLOWCELL_INSTRUMENT.get(_flowcell_for_file(file_path, plan_data), (None, None))[1]
