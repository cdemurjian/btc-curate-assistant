from __future__ import annotations

from typing import Any

from . import sharma


def biospecimenfile_ids_for_row(
    file_path: str,
    plan_data: dict[str, Any],
) -> tuple[str, str]:
    modality = plan_data["variables"]["custom_modality"]
    if modality == "sharma":
        return sharma.biospecimenfile_ids_for_row(file_path, plan_data)
    return "", ""
