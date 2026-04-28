from __future__ import annotations

from pathlib import Path
from typing import Any

from btc_manifest.inventory import data_level_for_file, file_extension, read_inventory_rows
from btc_manifest.modalities import assay_for_file, panel_for_file, propose_biospecimenfile_ids_for_row
from btc_manifest.references import reviewed_id_map
from btc_manifest.templates import replace_xlsx_sheet_rows, strip_template_hints_from_paths


ASSAY_OPTIONS = [
    "scRNA",
    "spatialTrans",
    "spatialProt",
    "bulkDNA",
    "bulkRNA",
    "scWGS",
    "radiology",
    "metabolome",
    "cfDNA",
    "histoPath",
    "mIF",
    "BCR",
    "TCR",
    "cytokine",
    "clinical",
    "bulkMS",
    "scDNA",
    "globalProt",
    "documents",
    "digitalPath",
    "phosphProt",
    "immunoPep",
    "CITEseq",
    "DNAmethylation",
    "bulkDNA_LR",
    "CyTOF",
    "10xMultiome",
]


def manifest_assay_value(manifest_values: dict[str, Any]) -> str:
    return ",".join(manifest_values["assays"])


def render_file_manifest(plan_data: dict[str, Any]) -> str:
    copied_templates = plan_data.get("copied_templates", {})
    file_template = Path(copied_templates["file"])
    teamlab = plan_data["variables"]["teamlab"]
    manifest_values = plan_data["modality_variables"][plan_data["variables"]["custom_modality"]]
    inventory_rows = read_inventory_rows(plan_data)

    file_headers = [
        "Component",
        "assay",
        "teamlab",
        "data_level",
        "lab",
        "lab_contact",
        "lab_firstname",
        "lab_contact_firstname",
        "file_source",
        "file_extension",
        "file_size",
        "additional_data",
        "panel",
        "platform",
        "vendor",
    ]
    file_rows: list[list[Any]] = [file_headers]
    for row in inventory_rows:
        file_path = row["file_path"]
        assay_value = assay_for_file(file_path, plan_data) or manifest_assay_value(manifest_values)
        panel_value = panel_for_file(file_path, plan_data) or manifest_values["panel"]
        file_rows.append(
            [
                "file",
                assay_value,
                teamlab,
                data_level_for_file(file_path),
                manifest_values["lab"],
                manifest_values["lab_contact"],
                manifest_values["lab_firstname"],
                manifest_values["lab_contact_firstname"],
                file_path,
                file_extension(file_path),
                int(row["size"]) if row["size"].isdigit() else row["size"],
                "",
                panel_value,
                manifest_values["platform"],
                manifest_values["vendor"],
            ]
        )

    replace_xlsx_sheet_rows(file_template, "Sheet1", file_rows)
    strip_template_hints_from_paths([str(file_template)])
    return str(file_template)


def render_biospecimenfile_manifest(plan_data: dict[str, Any]) -> str:
    copied_templates = plan_data.get("copied_templates", {})
    biospecimenfile_template = Path(copied_templates["biospecimenfile"])
    teamlab = plan_data["variables"]["teamlab"]
    manifest_values = plan_data["modality_variables"][plan_data["variables"]["custom_modality"]]
    inventory_rows = read_inventory_rows(plan_data)

    biospecimenfile_headers = [
        "Component",
        "teamlab",
        "study",
        "subject_trial_id",
        "biospecimen_trial_id",
        "file_source",
    ]
    biospecimenfile_rows: list[list[Any]] = [biospecimenfile_headers]
    biospecimenfile_id_rows = plan_data.get("biospecimenfile_id_rows", [])
    if biospecimenfile_id_rows:
        for row in biospecimenfile_id_rows:
            biospecimenfile_rows.append(
                [
                    "biospecimenfile",
                    teamlab,
                    manifest_values["study"],
                    row.get("subject_trial_id", ""),
                    row.get("biospecimen_trial_id", ""),
                    row.get("file_path", ""),
                ]
            )
        replace_xlsx_sheet_rows(biospecimenfile_template, "Sheet1", biospecimenfile_rows)
        strip_template_hints_from_paths([str(biospecimenfile_template)])
        return str(biospecimenfile_template)

    id_map = reviewed_id_map(plan_data)
    for row in inventory_rows:
        reviewed_ids = id_map.get(row["file_path"], {})
        if reviewed_ids:
            subject_trial_id = reviewed_ids.get("subject_trial_id", "")
            biospecimen_trial_id = reviewed_ids.get("biospecimen_trial_id", "")
        else:
            subject_trial_id, biospecimen_trial_id = propose_biospecimenfile_ids_for_row(
                row["file_path"],
                plan_data,
            )
        biospecimenfile_rows.append(
            [
                "biospecimenfile",
                teamlab,
                manifest_values["study"],
                subject_trial_id,
                biospecimen_trial_id,
                row["file_path"],
            ]
        )

    replace_xlsx_sheet_rows(biospecimenfile_template, "Sheet1", biospecimenfile_rows)
    strip_template_hints_from_paths([str(biospecimenfile_template)])
    return str(biospecimenfile_template)


def render_biospecimen_manifest(plan_data: dict[str, Any]) -> str | None:
    copied_templates = plan_data.get("copied_templates", {})
    if "biospecimen" not in copied_templates:
        return None

    biospecimen_template = Path(copied_templates["biospecimen"])
    teamlab = plan_data["variables"]["teamlab"]
    manifest_values = plan_data["modality_variables"][plan_data["variables"]["custom_modality"]]
    id_map = reviewed_id_map(plan_data)

    biospecimen_headers = [
        "Component",
        "teamlab",
        "study",
        "subject_trial_id",
        "biospecimen_trial_id",
        "sample_type",
        "timepoint",
        "additional_data",
        "nominal_timepoint",
        "parent_biospecimen",
        "parent_biospecimen_raw",
    ]
    biospecimen_rows: list[list[Any]] = [biospecimen_headers]
    biospecimenfile_id_rows = plan_data.get("biospecimenfile_id_rows", [])
    if biospecimenfile_id_rows:
        seen: set[tuple[str, str]] = set()
        for ids in biospecimenfile_id_rows:
            if ids.get("biospecimen_status") == "exact":
                continue
            subject_trial_id = ids.get("subject_trial_id", "")
            biospecimen_trial_id = ids.get("biospecimen_trial_id", "")
            key = (subject_trial_id, biospecimen_trial_id)
            if not any(key) or key in seen:
                continue
            seen.add(key)
            biospecimen_rows.append(
                [
                    "biospecimen",
                    teamlab,
                    manifest_values["study"],
                    subject_trial_id,
                    biospecimen_trial_id,
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                ]
            )
        replace_xlsx_sheet_rows(biospecimen_template, "Sheet1", biospecimen_rows)
        strip_template_hints_from_paths([str(biospecimen_template)])
        return str(biospecimen_template)

    seen: set[tuple[str, str]] = set()
    for ids in id_map.values():
        if ids.get("biospecimen_status") == "exact":
            continue
        subject_trial_id = ids.get("subject_trial_id", "")
        biospecimen_trial_id = ids.get("biospecimen_trial_id", "")
        key = (subject_trial_id, biospecimen_trial_id)
        if not any(key) or key in seen:
            continue
        seen.add(key)
        biospecimen_rows.append(
            [
                "biospecimen",
                teamlab,
                manifest_values["study"],
                subject_trial_id,
                biospecimen_trial_id,
                "",
                "",
                "",
                "",
                "",
                "",
            ]
        )

    replace_xlsx_sheet_rows(biospecimen_template, "Sheet1", biospecimen_rows)
    strip_template_hints_from_paths([str(biospecimen_template)])
    return str(biospecimen_template)


def render_manifest_files(plan_data: dict[str, Any]) -> dict[str, str]:
    rendered = {
        "file": render_file_manifest(plan_data),
        "biospecimenfile": render_biospecimenfile_manifest(plan_data),
    }
    biospecimen_manifest = render_biospecimen_manifest(plan_data)
    if biospecimen_manifest is not None:
        rendered["biospecimen"] = biospecimen_manifest
    return rendered
