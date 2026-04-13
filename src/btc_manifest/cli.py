from __future__ import annotations

import argparse
import json
from pathlib import Path
from shlex import quote
from typing import Any

from btc_manifest.aws import check_aws_sso, parse_s3_uri, run_s3_inventory
from btc_manifest.config import Settings
from btc_manifest.manifests import ASSAY_OPTIONS, render_manifest_files
from btc_manifest.mongo_exports import ensure_current_mongo_exports
from btc_manifest.plans import build_plan, load_plan, save_plan_data
from btc_manifest.templates import copy_required_templates, strip_template_hints_from_paths


def choose_one(
    prompt: str,
    options: list[str],
    default: str | None = None,
    aliases: dict[str, str] | None = None,
) -> str:
    if not options:
        raise ValueError(f"No options available for: {prompt}")

    normalized_aliases = {key.lower(): value for key, value in (aliases or {}).items()}
    print(prompt)
    for index, option in enumerate(options, start=1):
        default_marker = " [default]" if option == default else ""
        print(f"  {index}. {option}{default_marker}")

    while True:
        answer = input("> ").strip()
        if not answer and default is not None:
            return default
        if answer.isdigit() and 1 <= int(answer) <= len(options):
            return options[int(answer) - 1]
        if answer.lower() in normalized_aliases:
            return normalized_aliases[answer.lower()]
        matching = [option for option in options if option.lower() == answer.lower()]
        if matching:
            return matching[0]
        print("Choose one of the listed options.")


def choose_many(prompt: str, options: list[str], default: list[str] | None = None) -> list[str]:
    default = default or []
    default_label = f" [default: {', '.join(default)}]" if default else ""
    print(f"{prompt}{default_label}")
    for index, option in enumerate(options, start=1):
        print(f"  {index}. {option}")

    while True:
        answer = input("> ").strip()
        if not answer and default:
            return default
        selected: list[str] = []
        for part in [item.strip() for item in answer.split(",") if item.strip()]:
            if part.isdigit() and 1 <= int(part) <= len(options):
                selected.append(options[int(part) - 1])
                continue
            matches = [option for option in options if option.lower() == part.lower()]
            if matches:
                selected.append(matches[0])
                continue
            selected = []
            break
        if selected:
            return list(dict.fromkeys(selected))
        print("Choose one or more listed options, separated by commas.")


def ask_text(prompt: str, default: str | None = None) -> str:
    default_label = f" [{default}]" if default else ""
    answer = input(f"{prompt}{default_label}: ").strip()
    if not answer and default is not None:
        return default
    return answer


def ask_curation_questions(existing: dict[str, Any] | None = None) -> dict[str, str]:
    existing = existing or {}
    register_subjects = choose_one(
        "Do you need to register subjects?",
        ["Yes", "No"],
        existing.get("register_subjects"),
        {"y": "Yes", "n": "No"},
    )
    register_biospecimens = (
        "Yes"
        if register_subjects == "Yes"
        else choose_one(
            "Do you need to register biospecimens?",
            ["Yes", "No", "Maybe"],
            existing.get("register_biospecimens"),
            {"y": "Yes", "n": "No", "m": "Maybe"},
        )
    )
    if register_subjects == "Yes":
        print("Biospecimen registration set to Yes because subject registration is Yes.")

    return {
        "register_subjects": register_subjects,
        "register_biospecimens": register_biospecimens,
        "teamlab": choose_one(
            "What teamlab is this for?",
            [
                "GBM",
                "AMLCH",
                "OVMRD",
                "DPT",
                "OVIOC",
                "KRAS",
                "AMLMRD",
                "DST",
                "LUNGRD",
                "KRAS-REVMED",
                "OSTEO",
            ],
            existing.get("teamlab") or "GBM",
        ),
        "custom_modality": choose_one(
            "Which custom modality is this?",
            ["no_modality-default", "sharma"],
            existing.get("custom_modality") or "no_modality-default",
        ),
    }


def default_study_for_teamlab(teamlab: str) -> str | None:
    if teamlab == "GBM":
        return "GBM_Trial1"
    return None


def default_assays_for_modality(modality: str) -> list[str]:
    return []


def ask_manifest_questions(
    plan_data: dict[str, Any],
    existing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    existing = existing or {}
    teamlab = plan_data["variables"]["teamlab"]
    modality = plan_data["variables"]["custom_modality"]
    return {
        "assays": choose_many(
            "What assay(s) should be used for the file manifest?",
            ASSAY_OPTIONS,
            existing.get("assays") or default_assays_for_modality(modality),
        ),
        "study": ask_text(
            "Study",
            existing.get("study") or default_study_for_teamlab(teamlab),
        ),
        "lab": ask_text("Lab", existing.get("lab")),
        "lab_contact": ask_text("Lab contact", existing.get("lab_contact")),
        "lab_firstname": ask_text("Lab firstname", existing.get("lab_firstname")),
        "lab_contact_firstname": ask_text(
            "Lab contact firstname",
            existing.get("lab_contact_firstname"),
        ),
        "panel": ask_text("Panel", existing.get("panel")),
        "platform": ask_text("Platform", existing.get("platform")),
        "vendor": ask_text("Vendor", existing.get("vendor")),
    }


def render_modality_manifests(plan_data: dict[str, Any]) -> dict[str, str]:
    modality = plan_data["variables"]["custom_modality"]
    existing = plan_data.get("modality_variables", {}).get(modality, {})
    plan_data.setdefault("modality_variables", {})[modality] = ask_manifest_questions(
        plan_data,
        existing,
    )
    rendered = render_manifest_files(plan_data)
    plan_data["rendered_manifests"] = rendered
    save_plan_data(plan_data)
    return rendered


def update_run_plan(plan_data: dict[str, Any], variables: dict[str, str]) -> dict[str, Any]:
    output_dir = Path(plan_data["output_dir"])
    templates_dir = Path(plan_data["templates_dir"])
    plan_data["variables"] = {
        **plan_data.get("variables", {}),
        **variables,
        "needs_subject_template": variables["register_subjects"] == "Yes",
        "needs_biospecimen_templates": variables["register_biospecimens"] in {"Yes", "Maybe"},
    }
    plan_data["copied_templates"] = copy_required_templates(
        templates_dir,
        output_dir,
        variables,
    )
    strip_template_hints_from_paths(list(plan_data["copied_templates"].values()))
    save_plan_data(plan_data)
    return plan_data


def available_run_dirs(output_dir: Path) -> list[Path]:
    if not output_dir.exists():
        return []
    return sorted(
        [path for path in output_dir.iterdir() if path.is_dir() and (path / "plan.json").exists()],
        reverse=True,
    )


def choose_resume_run(settings: Settings) -> dict[str, Any] | None:
    run_dirs = available_run_dirs(settings.output_dir)
    if not run_dirs:
        return None

    choice = choose_one(
        "Start a new run or resume an existing run?",
        ["New run", *[path.name for path in run_dirs]],
        "New run",
    )
    if choice == "New run":
        return None
    return load_plan(settings.output_dir / choice)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="curate",
        description="BTC metadata curation assistant workflow tools.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for generated run artifacts. Defaults to OUTPUT_DIR or runs/.",
    )
    subparsers = parser.add_subparsers(dest="command")

    plan = subparsers.add_parser("plan", help="Create a local curation run plan.")
    plan.add_argument("s3_uri", help="S3 project root, for example s3://bucket/prefix/")

    return parser


def run_interactive(settings: Settings) -> None:
    print("Checking AWS SSO...", flush=True)
    aws_ok, aws_message = check_aws_sso(settings)
    if not aws_ok:
        login_hint = (
            f"\nRun `aws sso login --profile {settings.aws_profile}` and then rerun `uv run curate`."
            if settings.aws_profile
            else ""
        )
        raise SystemExit(f"AWS is not ready: {aws_message}{login_hint}")
    print(f"AWS SSO OK: {aws_message}")
    ensure_current_mongo_exports(settings)

    resumed_plan = choose_resume_run(settings)
    if resumed_plan is not None:
        print(f"Resuming run: {resumed_plan['output_dir']}")
        variables = ask_curation_questions(resumed_plan.get("variables"))
        updated_plan = update_run_plan(resumed_plan, variables)
        print(f"Wrote plan: {Path(updated_plan['output_dir']) / 'plan.json'}")
        print("Copied templates:")
        for path in updated_plan["copied_templates"].values():
            print(f"  {path}")
        rendered = render_modality_manifests(updated_plan)
        print("Rendered manifests:")
        for path in rendered.values():
            print(f"  {path}")
        return

    s3_uri = input("Paste AWS S3 path: ").strip()
    s3_uri = parse_s3_uri(s3_uri).to_uri()

    plan = build_plan(s3_uri, settings)
    inventory_path = Path(plan.commands["s3_inventory_csv"])
    print(f"Running: {plan.commands['s3_inventory']} > {quote(str(inventory_path))}")
    result = run_s3_inventory(plan.commands["s3_inventory"], inventory_path)
    if result.returncode != 0:
        raise SystemExit(f"S3 inventory failed: {result.stderr.strip()}")

    print(f"Wrote S3 inventory: {inventory_path}")
    variables = ask_curation_questions()
    plan_data = update_run_plan(plan.to_dict(), variables)
    print(f"Wrote plan: {Path(plan_data['output_dir']) / 'plan.json'}")
    print("Copied templates:")
    for path in plan_data["copied_templates"].values():
        print(f"  {path}")
    rendered = render_modality_manifests(plan_data)
    print("Rendered manifests:")
    for path in rendered.values():
        print(f"  {path}")


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = Settings.from_env(output_dir=args.output_dir)

    if args.command == "plan":
        plan = build_plan(args.s3_uri, settings)
        print(json.dumps(plan.to_dict(), indent=2, sort_keys=True))
        return

    if args.command is None:
        run_interactive(settings)
        return

    parser.error(f"unknown command: {args.command}")


if __name__ == "__main__":
    main()
