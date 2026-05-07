from __future__ import annotations

import re
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile


XLSX_MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
XLSX_REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
XLSX_PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


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


def _replace_text_nodes(root: ET.Element, replacements: dict[str, str]) -> int:
    count = 0
    for node in root.iter():
        if node.text in replacements:
            node.text = replacements[node.text]
            count += 1
    return count


def replace_strings_in_xlsx(xlsx_path: Path, replacements: dict[str, str]) -> int:
    if not replacements:
        return 0

    tmp_path = xlsx_path.with_name(f"{xlsx_path.stem}.tmp{xlsx_path.suffix}")
    changed_cells = 0
    with ZipFile(xlsx_path, "r") as source, ZipFile(tmp_path, "w", ZIP_DEFLATED) as target:
        for item in source.infolist():
            data = source.read(item.filename)
            if (
                item.filename == "xl/sharedStrings.xml"
                or item.filename.startswith("xl/worksheets/")
                and item.filename.endswith(".xml")
            ):
                root = ET.fromstring(data)
                changed_cells += _replace_text_nodes(root, replacements)
                data = ET.tostring(root, encoding="utf-8", xml_declaration=True)
            target.writestr(item, data)
    tmp_path.replace(xlsx_path)
    return changed_cells


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
