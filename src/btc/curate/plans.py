from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from btc.common.aws import S3Location, build_aws_s3_inventory_command, normalize_s3_uri, parse_s3_uri
from btc.common.config import Settings


@dataclass(frozen=True)
class CurationPlan:
    s3: S3Location
    run_id: str
    output_dir: Path
    templates_dir: Path
    files_dir: Path
    checks: dict[str, Any] = field(default_factory=dict)
    steps: list[str] = field(default_factory=list)
    commands: dict[str, str] = field(default_factory=dict)
    variables: dict[str, Any] = field(default_factory=dict)
    copied_templates: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["s3"]["uri"] = self.s3.to_uri()
        data["output_dir"] = str(self.output_dir)
        data["templates_dir"] = str(self.templates_dir)
        data["files_dir"] = str(self.files_dir)
        return data


def write_plan(plan: CurationPlan) -> None:
    (plan.output_dir / "plan.json").write_text(
        json.dumps(plan.to_dict(), indent=2, sort_keys=True) + "\n"
    )


def load_plan(run_dir: Path) -> dict[str, Any]:
    plan_path = run_dir / "plan.json"
    if not plan_path.exists():
        raise ValueError(f"No plan.json found in {run_dir}")
    return json.loads(plan_path.read_text())


def save_plan_data(plan_data: dict[str, Any]) -> None:
    output_dir = Path(plan_data["output_dir"])
    (output_dir / "plan.json").write_text(json.dumps(plan_data, indent=2, sort_keys=True) + "\n")


def new_timestamp() -> str:
    return datetime.now().strftime("%y%m%d_%H%M%S")


def build_plan(s3_uri: str, settings: Settings) -> CurationPlan:
    s3_uri = normalize_s3_uri(s3_uri)
    run_id = new_timestamp()
    run_dir = settings.output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    template_workbooks = (
        sorted(settings.templates_dir.glob("*.xlsx")) if settings.templates_dir.exists() else []
    )
    plan = CurationPlan(
        s3=parse_s3_uri(s3_uri),
        run_id=run_id,
        output_dir=run_dir,
        templates_dir=settings.templates_dir,
        files_dir=settings.files_dir,
        checks={
            "aws_profile_configured": bool(settings.aws_profile),
            "aws_region_configured": bool(settings.aws_region),
            "mongodb_uri_configured": bool(settings.mongodb_uri),
            "mongodb_database_configured": bool(settings.mongodb_database),
            "templates_dir_exists": settings.templates_dir.exists(),
            "template_workbook_count": len(template_workbooks),
            "files_dir_exists": settings.files_dir.exists(),
            "files_gbm_dir_exists": (settings.files_dir / "gbm").exists(),
            "files_kras_dir_exists": (settings.files_dir / "kras").exists(),
            "files_other_dir_exists": (settings.files_dir / "other").exists(),
        },
        commands={
            "s3_inventory": build_aws_s3_inventory_command(s3_uri, settings),
            "s3_inventory_csv": str(run_dir / f"{run_id}.csv"),
        },
        steps=[
            "parse_s3_uri",
            "load_runtime_config",
            "check_aws_sso",
            "discover_s3_objects",
            "pull_mongodb_metadata",
            "resolve_templates",
            "resolve_project_files",
            "render_curated_workbooks",
        ],
    )
    write_plan(plan)
    return plan
