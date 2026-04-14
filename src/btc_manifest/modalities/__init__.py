from __future__ import annotations

from typing import Any

from . import sharma


def propose_biospecimenfile_ids_for_row(
    file_path: str,
    plan_data: dict[str, Any],
) -> tuple[str, str]:
    modality = plan_data["variables"]["custom_modality"]
    if modality == "sharma":
        return sharma.propose_biospecimenfile_ids_for_row(file_path, plan_data)
    return "", ""


def should_skip_biospecimenfile_mapping(
    file_path: str,
    plan_data: dict[str, Any],
) -> bool:
    modality = plan_data["variables"]["custom_modality"]
    if modality == "sharma":
        return sharma.should_skip_biospecimenfile_mapping(file_path, plan_data)
    return False


def review_group_key_for_file(
    file_path: str,
    plan_data: dict[str, Any],
) -> str:
    modality = plan_data["variables"]["custom_modality"]
    if modality == "sharma":
        return sharma.review_group_key_for_file(file_path, plan_data)

    parts = [part for part in file_path.split("/") if part]
    if len(parts) < 2:
        return file_path
    return parts[-2]


def assay_for_file(
    file_path: str,
    plan_data: dict[str, Any],
) -> str | None:
    modality = plan_data["variables"]["custom_modality"]
    if modality == "sharma":
        return sharma.assay_for_file(file_path, plan_data)
    return None


def biospecimen_candidates_for_group(
    group_parts: list[str],
    plan_data: dict[str, Any],
) -> list[str]:
    modality = plan_data["variables"]["custom_modality"]
    if modality == "sharma":
        return sharma.biospecimen_candidates_for_group(group_parts, plan_data)
    return []
