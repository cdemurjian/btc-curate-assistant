from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from . import sharma, white_proteomics


def _default_review_group_key(file_path: str, plan_data: dict[str, Any]) -> str:
    del plan_data
    parts = [part for part in file_path.split("/") if part]
    if len(parts) < 2:
        return file_path
    return parts[-2]


@dataclass(frozen=True)
class ModalitySpec:
    name: str
    manifest_defaults: dict[str, Any] = field(default_factory=dict)
    propose_biospecimenfile_ids_for_row: Callable[[str, dict[str, Any]], tuple[str, str]] = (
        lambda file_path, plan_data: ("", "")
    )
    should_skip_biospecimenfile_mapping: Callable[[str, dict[str, Any]], bool] = (
        lambda file_path, plan_data: False
    )
    review_group_key_for_file: Callable[[str, dict[str, Any]], str] = _default_review_group_key
    assay_for_file: Callable[[str, dict[str, Any]], str | None] = lambda file_path, plan_data: None
    panel_for_file: Callable[[str, dict[str, Any]], str | None] = lambda file_path, plan_data: None
    biospecimen_candidates_for_group: Callable[[list[str], dict[str, Any]], list[str]] = (
        lambda group_parts, plan_data: []
    )
    custom_proposed_pairs: Callable[
        [dict[str, Any], list[dict[str, Any]]],
        tuple[dict[str, tuple[str, str]], list[str]] | None,
    ] | None = None


_MODALITIES: dict[str, ModalitySpec] = {
    "no_modality-default": ModalitySpec(name="no_modality-default"),
    "sharma": ModalitySpec(
        name="sharma",
        propose_biospecimenfile_ids_for_row=sharma.propose_biospecimenfile_ids_for_row,
        should_skip_biospecimenfile_mapping=sharma.should_skip_biospecimenfile_mapping,
        review_group_key_for_file=sharma.review_group_key_for_file,
        assay_for_file=sharma.assay_for_file,
        biospecimen_candidates_for_group=sharma.biospecimen_candidates_for_group,
    ),
    "white-proteomics": ModalitySpec(
        name="white-proteomics",
        manifest_defaults=white_proteomics.MANIFEST_DEFAULTS,
        review_group_key_for_file=white_proteomics.review_group_key_for_file,
        assay_for_file=white_proteomics.assay_for_file,
        panel_for_file=white_proteomics.panel_for_file,
        custom_proposed_pairs=white_proteomics.build_proposed_pairs,
    ),
}


def available_modalities() -> list[str]:
    return list(_MODALITIES)


def modality_spec(plan_data: dict[str, Any]) -> ModalitySpec:
    modality = plan_data["variables"]["custom_modality"]
    return _MODALITIES.get(modality, _MODALITIES["no_modality-default"])


def manifest_defaults_for_modality(modality: str) -> dict[str, Any]:
    return dict(_MODALITIES.get(modality, _MODALITIES["no_modality-default"]).manifest_defaults)


def custom_proposed_pairs(
    plan_data: dict[str, Any],
    inventory_rows: list[dict[str, Any]],
) -> tuple[dict[str, tuple[str, str]], list[str]] | None:
    builder = modality_spec(plan_data).custom_proposed_pairs
    if builder is None:
        return None
    return builder(plan_data, inventory_rows)


def propose_biospecimenfile_ids_for_row(
    file_path: str,
    plan_data: dict[str, Any],
) -> tuple[str, str]:
    return modality_spec(plan_data).propose_biospecimenfile_ids_for_row(file_path, plan_data)


def should_skip_biospecimenfile_mapping(
    file_path: str,
    plan_data: dict[str, Any],
) -> bool:
    return modality_spec(plan_data).should_skip_biospecimenfile_mapping(file_path, plan_data)


def review_group_key_for_file(
    file_path: str,
    plan_data: dict[str, Any],
) -> str:
    return modality_spec(plan_data).review_group_key_for_file(file_path, plan_data)


def assay_for_file(
    file_path: str,
    plan_data: dict[str, Any],
) -> str | None:
    return modality_spec(plan_data).assay_for_file(file_path, plan_data)


def panel_for_file(
    file_path: str,
    plan_data: dict[str, Any],
) -> str | None:
    return modality_spec(plan_data).panel_for_file(file_path, plan_data)


def biospecimen_candidates_for_group(
    group_parts: list[str],
    plan_data: dict[str, Any],
) -> list[str]:
    return modality_spec(plan_data).biospecimen_candidates_for_group(group_parts, plan_data)
