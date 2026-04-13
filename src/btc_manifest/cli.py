from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import subprocess
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from shlex import quote
from typing import Any
from urllib.parse import urlparse
from zipfile import ZIP_DEFLATED, ZipFile

from btc_manifest.config import Settings


XLSX_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
XLSX_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
XLSX_PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

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


@dataclass(frozen=True)
class S3Location:
    bucket: str
    prefix: str

    def to_uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}" if self.prefix else f"s3://{self.bucket}"


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


def parse_s3_uri(uri: str) -> S3Location:
    uri = normalize_s3_uri(uri)
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"expected an s3:// URI, got {uri!r}")
    if not parsed.netloc:
        raise ValueError(f"S3 URI is missing a bucket: {uri!r}")
    return S3Location(bucket=parsed.netloc, prefix=parsed.path.lstrip("/"))


def normalize_s3_uri(uri: str) -> str:
    parsed = urlparse(uri.strip())
    if parsed.scheme == "s3":
        return uri.strip()
    if parsed.scheme in {"http", "https"}:
        host_parts = parsed.netloc.split(".")
        if len(host_parts) >= 5 and host_parts[1] == "s3" and host_parts[3] == "amazonaws":
            bucket = host_parts[0]
            prefix = parsed.path.lstrip("/")
            return f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"
    return uri.strip()


def build_aws_s3_inventory_command(s3_uri: str, settings: Settings) -> str:
    s3_uri = normalize_s3_uri(s3_uri)
    s3_location = parse_s3_uri(s3_uri)
    s3_base = f"s3://{s3_location.bucket}/"
    aws_parts = ["aws", "s3", "ls", quote(s3_uri), "--recursive"]
    if settings.aws_profile:
        aws_parts.extend(["--profile", quote(settings.aws_profile)])
    aws_command = " ".join(aws_parts)
    awk_command = (
        f"awk -v base={quote(s3_base)} "
        "'BEGIN {print \"file_path,size\"} "
        "{key=$4; for (i=5; i<=NF; i++) key=key \" \" $i} "
        "key == \"\" {next} "
        "key ~ /(^|\\/)\\.DS_Store$/ {next} "
        "key ~ /(^|\\/)\\._/ {next} "
        "key ~ /(^|\\/)\\.$/ {next} "
        "key ~ /\\/$/ {next} "
        "{print base key \",\" $3}'"
    )
    return f"{aws_command} | {awk_command}"


def check_aws_sso(settings: Settings) -> tuple[bool, str]:
    if shutil.which("aws") is None:
        return False, "aws CLI was not found on PATH. Install or activate aws-cli first."
    if not settings.aws_profile:
        return False, "AWS_PROFILE is not set in .env"

    profiles_result = subprocess.run(
        ["aws", "configure", "list-profiles"],
        capture_output=True,
        text=True,
        check=False,
    )
    if profiles_result.returncode != 0:
        message = (profiles_result.stderr or profiles_result.stdout).strip()
        return False, message or "Could not list AWS CLI profiles"

    profiles = {profile.strip() for profile in profiles_result.stdout.splitlines() if profile.strip()}
    if settings.aws_profile not in profiles:
        available = ", ".join(sorted(profiles)) or "none"
        return (
            False,
            f"AWS_PROFILE={settings.aws_profile!r} is not configured. "
            f"Available profiles: {available}.",
        )

    command = ["aws", "sts", "get-caller-identity", "--profile", settings.aws_profile]
    if settings.aws_region:
        command.extend(["--region", settings.aws_region])

    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode == 0:
        identity = json.loads(result.stdout)
        return True, f"authenticated as {identity.get('Arn', 'unknown AWS identity')}"

    message = (result.stderr or result.stdout).strip()
    return False, message or "AWS SSO check failed"


def run_s3_inventory(command: str, output_path: Path) -> subprocess.CompletedProcess[str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w") as output_file:
        return subprocess.run(
            command,
            shell=True,
            text=True,
            stdout=output_file,
            stderr=subprocess.PIPE,
            check=False,
        )


def write_plan(plan: CurationPlan) -> None:
    (plan.output_dir / "plan.json").write_text(
        json.dumps(plan.to_dict(), indent=2, sort_keys=True) + "\n"
    )


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


def load_plan(run_dir: Path) -> dict[str, Any]:
    plan_path = run_dir / "plan.json"
    if not plan_path.exists():
        raise ValueError(f"No plan.json found in {run_dir}")
    return json.loads(plan_path.read_text())


def save_plan_data(plan_data: dict[str, Any]) -> None:
    output_dir = Path(plan_data["output_dir"])
    (output_dir / "plan.json").write_text(json.dumps(plan_data, indent=2, sort_keys=True) + "\n")


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
            ["no_modality-default", "scRNA-sharma"],
            existing.get("custom_modality") or "no_modality-default",
        ),
    }


def template_date(path: Path) -> datetime:
    match = re.search(r"_(\d{8})\.xlsx$", path.name)
    if not match:
        return datetime.min
    return datetime.strptime(match.group(1), "%m%d%Y")


def latest_template(templates_dir: Path, template_kind: str) -> Path:
    candidates = sorted(
        templates_dir.glob(f"template_{template_kind}_*.xlsx"),
        key=template_date,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No template_{template_kind}_*.xlsx found in {templates_dir}")
    return candidates[0]


def required_template_kinds(variables: dict[str, str]) -> list[str]:
    kinds = ["file", "biospecimenfile"]
    if variables["register_subjects"] == "Yes":
        kinds.append("subject")
    if variables["register_biospecimens"] in {"Yes", "Maybe"}:
        kinds.append("biospecimen")
    return kinds


def copy_required_templates(
    templates_dir: Path, output_dir: Path, variables: dict[str, str]
) -> dict[str, str]:
    copied: dict[str, str] = {}
    for kind in required_template_kinds(variables):
        source = latest_template(templates_dir, kind)
        target = output_dir / source.name
        shutil.copy2(source, target)
        copied[kind] = str(target)
    return copied


def ask_text(prompt: str, default: str | None = None) -> str:
    default_label = f" [{default}]" if default else ""
    answer = input(f"{prompt}{default_label}: ").strip()
    if not answer and default is not None:
        return default
    return answer


def default_study_for_teamlab(teamlab: str) -> str | None:
    if teamlab == "GBM":
        return "GBM_Trial1"
    return None


def default_assays_for_modality(modality: str) -> list[str]:
    if modality == "scRNA-sharma":
        return ["scRNA"]
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


def is_fake_file_path(file_path: str) -> bool:
    key = file_path.rstrip("/")
    name = key.rsplit("/", 1)[-1]
    return (
        not key
        or name == "."
        or name == ".DS_Store"
        or name.startswith("._")
        or file_path.endswith("/")
    )


def normalize_inventory_file_path(file_path: str, plan_data: dict[str, Any]) -> str:
    file_path = file_path.strip()
    if file_path.startswith("s3://"):
        return file_path
    bucket = plan_data["s3"]["bucket"]
    return f"s3://{bucket}/{file_path.lstrip('/')}"


def read_inventory_rows(plan_data: dict[str, Any]) -> list[dict[str, Any]]:
    inventory_path = Path(plan_data["commands"]["s3_inventory_csv"])
    rows: list[dict[str, Any]] = []
    lines = [line for line in inventory_path.read_text().splitlines() if line.strip()]
    if not lines:
        return rows

    delimiter = "\t" if "\t" in lines[0] and "," not in lines[0] else ","
    reader = csv.reader(lines, delimiter=delimiter)
    first = next(reader, None)
    if first is None:
        return rows

    has_header = len(first) >= 2 and first[0] == "file_path" and first[1] == "size"
    source_rows = reader if has_header else iter([first, *reader])
    for row in source_rows:
        if len(row) < 2:
            continue
        file_path = normalize_inventory_file_path(row[0], plan_data)
        if is_fake_file_path(file_path):
            continue
        rows.append({"file_path": file_path, "size": row[1].strip()})
    return rows


def file_extension(file_path: str) -> str:
    name = file_path.rstrip("/").rsplit("/", 1)[-1].lower()
    for extension in ("fastq.gz", "fq.gz", "vcf.gz", "tar.gz"):
        if name.endswith(f".{extension}"):
            return extension
    if "." not in name:
        return ""
    return name.rsplit(".", 1)[-1]


def data_level_for_file(file_path: str) -> str:
    return "1" if file_extension(file_path) in {"fastq.gz", "fq.gz"} else ""


def column_name(index: int) -> str:
    name = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        name = chr(65 + remainder) + name
    return name


def xlsx_sheet_path(xlsx_path: Path, sheet_name: str) -> str:
    with ZipFile(xlsx_path) as zf:
        rel_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        rels = {
            rel.attrib["Id"]: rel.attrib["Target"].lstrip("/")
            for rel in rel_root.findall(f"{{{XLSX_PKG_REL_NS}}}Relationship")
        }
        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        for sheet in workbook.findall(f".//{{{XLSX_MAIN_NS}}}sheet"):
            if sheet.attrib["name"] != sheet_name:
                continue
            target = rels[sheet.attrib[f"{{{XLSX_REL_NS}}}id"]]
            return target if target.startswith("xl/") else f"xl/{target}"
    raise ValueError(f"Sheet {sheet_name!r} not found in {xlsx_path}")


def worksheet_xml(rows: list[list[Any]]) -> bytes:
    ET.register_namespace("", XLSX_MAIN_NS)
    worksheet = ET.Element(f"{{{XLSX_MAIN_NS}}}worksheet")
    sheet_data = ET.SubElement(worksheet, f"{{{XLSX_MAIN_NS}}}sheetData")
    for row_number, row_values in enumerate(rows, start=1):
        row = ET.SubElement(sheet_data, f"{{{XLSX_MAIN_NS}}}row", {"r": str(row_number)})
        for column_number, value in enumerate(row_values, start=1):
            if value in {None, ""}:
                continue
            cell_ref = f"{column_name(column_number)}{row_number}"
            if isinstance(value, int):
                cell = ET.SubElement(row, f"{{{XLSX_MAIN_NS}}}c", {"r": cell_ref})
                ET.SubElement(cell, f"{{{XLSX_MAIN_NS}}}v").text = str(value)
            else:
                cell = ET.SubElement(
                    row,
                    f"{{{XLSX_MAIN_NS}}}c",
                    {"r": cell_ref, "t": "inlineStr"},
                )
                inline = ET.SubElement(cell, f"{{{XLSX_MAIN_NS}}}is")
                ET.SubElement(inline, f"{{{XLSX_MAIN_NS}}}t").text = str(value)
    return ET.tostring(worksheet, encoding="utf-8", xml_declaration=True)


def replace_xlsx_sheet_rows(xlsx_path: Path, sheet_name: str, rows: list[list[Any]]) -> None:
    sheet_path = xlsx_sheet_path(xlsx_path, sheet_name)
    tmp_path = xlsx_path.with_name(f"{xlsx_path.stem}.tmp{xlsx_path.suffix}")
    with ZipFile(xlsx_path, "r") as source, ZipFile(tmp_path, "w", ZIP_DEFLATED) as target:
        for item in source.infolist():
            if item.filename == sheet_path:
                target.writestr(item, worksheet_xml(rows))
            else:
                target.writestr(item, source.read(item.filename))
    tmp_path.replace(xlsx_path)


def strip_template_hints(xlsx_path: Path) -> None:
    tmp_path = xlsx_path.with_name(f"{xlsx_path.stem}.tmp{xlsx_path.suffix}")
    with ZipFile(xlsx_path, "r") as source, ZipFile(tmp_path, "w", ZIP_DEFLATED) as target:
        for item in source.infolist():
            filename = item.filename
            if filename.startswith("xl/comments") or filename.startswith("xl/drawings/vmlDrawing"):
                continue

            data = source.read(filename)
            if filename.startswith("xl/worksheets/") and filename.endswith(".xml"):
                root = ET.fromstring(data)
                for tag in (
                    f"{{{XLSX_MAIN_NS}}}dataValidations",
                    f"{{{XLSX_MAIN_NS}}}legacyDrawing",
                ):
                    for element in root.findall(tag):
                        root.remove(element)
                data = ET.tostring(root, encoding="utf-8", xml_declaration=True)
            elif filename.startswith("xl/worksheets/_rels/") and filename.endswith(".rels"):
                root = ET.fromstring(data)
                for rel in list(root):
                    rel_type = rel.attrib.get("Type", "")
                    target_name = rel.attrib.get("Target", "")
                    if "comments" in rel_type or "vmlDrawing" in rel_type or "comments" in target_name:
                        root.remove(rel)
                data = ET.tostring(root, encoding="utf-8", xml_declaration=True)

            target.writestr(item, data)
    tmp_path.replace(xlsx_path)


def strip_template_hints_from_paths(paths: list[str]) -> None:
    for path in paths:
        strip_template_hints(Path(path))


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


def biospecimenfile_ids_for_row(
    file_path: str,
    plan_data: dict[str, Any],
) -> tuple[str, str]:
    modality = plan_data["variables"]["custom_modality"]
    if modality == "scRNA-sharma":
        return "", ""
    return "", ""


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
        prog="btc-curate-assistant",
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
            f"\nRun `aws sso login --profile {settings.aws_profile}` and then rerun `uv run cli.py`."
            if settings.aws_profile
            else ""
        )
        raise SystemExit(
            f"AWS is not ready: {aws_message}{login_hint}"
        )
    print(f"AWS SSO OK: {aws_message}")

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
