from __future__ import annotations

import argparse
import json
from pathlib import Path
from shlex import quote
from typing import Any

from btc_manifest.aws import check_aws_sso, parse_s3_uri, run_s3_inventory
from btc_manifest.config import Settings
from btc_manifest.inventory import read_inventory_rows
from btc_manifest.manifests import ASSAY_OPTIONS, render_manifest_files
from btc_manifest.modalities import (
    biospecimen_candidates_for_group,
    propose_biospecimenfile_ids_for_row,
    review_group_key_for_file,
    should_skip_biospecimenfile_mapping,
)
from btc_manifest.mongo_exports import ensure_current_mongo_exports
from btc_manifest.plans import build_plan, load_plan, save_plan_data
from btc_manifest.references import (
    exact_reference_match,
    fuzzy_reference_matches,
    load_biospecimen_references,
    load_subject_references,
)
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


def review_id_candidate(
    label: str,
    candidate: str,
    exact_match: dict[str, str] | None,
    fuzzy_matches: list[Any],
    field: str,
) -> tuple[str, str]:
    if not candidate:
        return "", "missing"
    if exact_match is not None:
        return candidate, "exact"
    if not fuzzy_matches:
        return candidate, "new"

    print(f"Mongo check: {label} not found exactly: {candidate}")
    options = [f"Keep new value: {candidate}", *[match.value for match in fuzzy_matches], "Blank"]
    print(f"  1. Keep new value: {candidate} [default]")
    for index, match in enumerate(fuzzy_matches, start=2):
        print(f"  {index}. {match.value} ({match.score:.0%})")
    print(f"  {len(fuzzy_matches) + 2}. Blank")

    while True:
        answer = input("> ").strip()
        if not answer:
            return candidate, "new"
        if answer.isdigit() and 1 <= int(answer) <= len(options):
            selected = options[int(answer) - 1]
            if selected == "Blank":
                return "", "blank"
            if selected.startswith("Keep new value: "):
                return candidate, "new"
            return selected, "fuzzy"
        if answer:
            direct_match = next(
                (match for match in fuzzy_matches if match.row.get(field) == answer),
                None,
            )
            if direct_match is not None:
                return direct_match.value, "fuzzy"
            if answer == candidate:
                return candidate, "new"
        print("Choose one of the listed options.")


# Parts order in group_key: patient|surgery|location|core
_SLUG_PROMPTS: dict[str, tuple[str, str]] = {
    "<patient>": ("Patient ID", "e.g. DFCI1"),
    "<surgery>": ("Surgery", "e.g. S1"),
    "<core>": ("Core", "e.g. C4"),
}
_REQUIRED_SLUG_MARKERS = set(_SLUG_PROMPTS)


def confirm_group_assignment(
    group_key: str,
    file_count: int,
    subject_trial_id: str,
    biospecimen_trial_id: str,
    plan_data: dict[str, Any],
    sample_paths: list[str] | None = None,
    default_core: str = "",
) -> tuple[str, str]:
    # group_key parts: patient|surgery|location|core
    parts = group_key.split("|")
    print(f"Group {group_key} ({file_count} files)")

    missing_required = [p for p in parts if p in _REQUIRED_SLUG_MARKERS]
    if missing_required:
        # Show a few representative file paths so the user has context.
        shown = (sample_paths or [])[:4]
        for p in shown:
            # Print just the last two path components (parent_dir/filename)
            segments = [s for s in p.split("/") if s]
            print(f"    {'/'.join(segments[-2:]) if len(segments) >= 2 else p}")
        if sample_paths and len(sample_paths) > 4:
            print(f"    ... and {len(sample_paths) - 4} more")

        # Ask for each missing required slug by name, then look up the biospecimen.
        resolved = list(parts)
        for i, part in enumerate(resolved):
            if part not in _REQUIRED_SLUG_MARKERS:
                continue
            if part == "<core>" and default_core:
                resolved[i] = default_core
                continue
            label, hint = _SLUG_PROMPTS[part]
            value = input(f"  {label} ({hint}): ").strip()
            if not value:
                print("  Skipping group.")
                return "", ""
            resolved[i] = value.upper()

        # resolved[0] = patient = subject_trial_id for this modality
        subject_trial_id = resolved[0] if not resolved[0].startswith("<") else subject_trial_id

        candidates = biospecimen_candidates_for_group(resolved, plan_data)

        if not candidates:
            known_slugs = [s for s in resolved if s and not s.startswith("<")]
            print(f"  No tracker biospecimen found matching {' + '.join(known_slugs)}.")
            biospecimen_trial_id = ask_text("Biospecimen trial ID", biospecimen_trial_id or None)
            subject_trial_id = ask_text("Subject trial ID", subject_trial_id or None)
        elif len(candidates) == 1:
            biospecimen_trial_id = candidates[0]
            print(f"  Tracker Candidate -> {biospecimen_trial_id}")
        else:
            print(f"  {len(candidates)} candidates:")
            for idx, cand in enumerate(candidates, 1):
                print(f"    {idx}. {cand}")
            while True:
                answer = input("  Select number or type value: ").strip()
                if answer.isdigit() and 1 <= int(answer) <= len(candidates):
                    biospecimen_trial_id = candidates[int(answer) - 1]
                    break
                if answer:
                    biospecimen_trial_id = answer
                    break
        # Fall through to Y/e/s confirmation with the resolved IDs.

    else:
        print(
            f"  candidate: subject_trial_id={subject_trial_id or '<blank>'}, "
            f"biospecimen_trial_id={biospecimen_trial_id or '<blank>'}"
        )

    while True:
        answer = input("Is this right? [Y/n/e/s]: ").strip().lower()
        if answer in {"", "y", "yes"}:
            return subject_trial_id, biospecimen_trial_id
        if answer in {"n", "no"}:
            follow_up = input("No selected. Edit or skip? [e/s]: ").strip().lower()
            if follow_up in {"e", "edit"}:
                return (
                    ask_text("Subject trial ID", subject_trial_id),
                    ask_text("Biospecimen trial ID", biospecimen_trial_id),
                )
            if follow_up in {"s", "skip", ""}:
                return "", ""
            print("Enter e or s.")
            continue
        if answer in {"e", "edit"}:
            return (
                ask_text("Subject trial ID", subject_trial_id),
                ask_text("Biospecimen trial ID", biospecimen_trial_id),
            )
        if answer in {"s", "skip"}:
            return "", ""
        print("Enter Y, e, or s.")


def review_biospecimenfile_ids(plan_data: dict[str, Any]) -> None:
    files_dir = Path(plan_data["files_dir"])
    modality = plan_data["variables"]["custom_modality"]
    manifest_values = plan_data["modality_variables"][modality]
    subject_refs = load_subject_references(files_dir, manifest_values.get("study"))
    biospecimen_refs = load_biospecimen_references(files_dir)
    proposed_pairs: dict[str, tuple[str, str]] = {}
    skipped_paths: list[str] = []
    for row in read_inventory_rows(plan_data):
        file_path = row["file_path"]
        if should_skip_biospecimenfile_mapping(file_path, plan_data):
            skipped_paths.append(file_path)
            continue
        proposed_pairs[file_path] = propose_biospecimenfile_ids_for_row(file_path, plan_data)

    if skipped_paths:
        plan_data["biospecimenfile_skipped_mapping"] = skipped_paths
        print(
            "Biospecimenfile ID review: "
            f"skipped {len(skipped_paths)} legacy/manual file rows."
        )

    if not any(any(pair) for pair in proposed_pairs.values()):
        plan_data["biospecimenfile_id_map"] = {}
        print("Biospecimenfile ID review: no modality ID candidates yet.")
        return

    group_to_paths: dict[str, list[str]] = {}
    for file_path in proposed_pairs:
        group_key = review_group_key_for_file(file_path, plan_data)
        group_to_paths.setdefault(group_key, []).append(file_path)

    # If any groups are missing core, ask once upfront — it's usually the same for the whole upload.
    default_core = ""
    if any("<core>" in key for key in group_to_paths):
        raw = input("What core should these files be? (e.g. C4, or blank to ask per group): ").strip()
        default_core = raw.upper() if raw else ""

    id_map: dict[str, dict[str, str]] = {}
    reviewed_pairs: dict[tuple[str, str], dict[str, str]] = {}
    fuzzy_count = 0
    new_biospecimen_count = 0
    for group_key, paths in sorted(group_to_paths.items()):
        pair_counts: dict[tuple[str, str], int] = {}
        for path in paths:
            pair = proposed_pairs[path]
            pair_counts[pair] = pair_counts.get(pair, 0) + 1
        (subject_trial_id, biospecimen_trial_id), _ = max(
            pair_counts.items(),
            key=lambda item: item[1],
        )
        subject_trial_id, biospecimen_trial_id = confirm_group_assignment(
            group_key,
            len(paths),
            subject_trial_id,
            biospecimen_trial_id,
            plan_data,
            sample_paths=paths,
            default_core=default_core,
        )
        pair_key = (subject_trial_id, biospecimen_trial_id)
        if pair_key in reviewed_pairs:
            for path in paths:
                id_map[path] = reviewed_pairs[pair_key]
            continue

        subject_exact = exact_reference_match(subject_refs, "subject_trial_id", subject_trial_id)
        subject_fuzzy = fuzzy_reference_matches(subject_refs, "subject_trial_id", subject_trial_id)
        biospecimen_exact = exact_reference_match(
            biospecimen_refs,
            "biospecimen_trial_id",
            biospecimen_trial_id,
        )
        biospecimen_fuzzy = fuzzy_reference_matches(
            biospecimen_refs,
            "biospecimen_trial_id",
            biospecimen_trial_id,
        )

        subject_value, subject_status = review_id_candidate(
            "Subject trial ID",
            subject_trial_id,
            subject_exact,
            subject_fuzzy,
            "subject_trial_id",
        )
        biospecimen_value, biospecimen_status = review_id_candidate(
            "Biospecimen trial ID",
            biospecimen_trial_id,
            biospecimen_exact,
            biospecimen_fuzzy,
            "biospecimen_trial_id",
        )

        fuzzy_count += int(subject_status == "fuzzy") + int(biospecimen_status == "fuzzy")
        new_biospecimen_count += int(biospecimen_status == "new")
        reviewed_pair = {
            "subject_trial_id": subject_value,
            "subject_status": subject_status,
            "biospecimen_trial_id": biospecimen_value,
            "biospecimen_status": biospecimen_status,
        }
        reviewed_pairs[pair_key] = reviewed_pair
        for path in paths:
            id_map[path] = reviewed_pair

    plan_data["biospecimenfile_id_map"] = id_map
    print(
        "Biospecimenfile ID review: "
        f"{len(id_map)} file rows, {fuzzy_count} fuzzy selections, "
        f"{new_biospecimen_count} new biospecimen IDs."
    )


def render_modality_manifests(plan_data: dict[str, Any]) -> dict[str, str]:
    modality = plan_data["variables"]["custom_modality"]
    existing = plan_data.get("modality_variables", {}).get(modality, {})
    plan_data.setdefault("modality_variables", {})[modality] = ask_manifest_questions(
        plan_data,
        existing,
    )
    review_biospecimenfile_ids(plan_data)
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
