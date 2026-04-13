from __future__ import annotations

from pathlib import Path
from typing import Any

from btc_manifest.inventory import data_level_for_file, file_extension, read_inventory_rows
from btc_manifest.modalities import biospecimenfile_ids_for_row
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
        file_rows.append(
            [
                "file",
                manifest_assay_value(manifest_values),
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
                manifest_values["panel"],
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
    for row in inventory_rows:
        subject_trial_id, biospecimen_trial_id = biospecimenfile_ids_for_row(
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


def render_manifest_files(plan_data: dict[str, Any]) -> dict[str, str]:
    return {
        "file": render_file_manifest(plan_data),
        "biospecimenfile": render_biospecimenfile_manifest(plan_data),
    }
