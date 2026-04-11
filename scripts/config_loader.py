"""
sources.yaml 配置加载器
将 YAML 配置解析为 Python 数据结构，供 ingest.py 消费
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path, PureWindowsPath
from typing import Literal

import yaml


@dataclass
class SheetRule:
    sheet: str | int
    header_row: int = 1
    header_rows: int = 1
    merge_strategy: str | None = None
    skip_rows: list[int] = field(default_factory=list)
    target_table: str = ""


@dataclass
class FileRule:
    match: str
    format: Literal["xlsx", "xlsx_multi_sheet", "csv", "html_as_xlsx", "xml_as_xlsx"]
    encoding: str = "utf-8"
    sheet: str | int | None = None
    header_row: int = 1
    header_rows: int = 1
    merge_strategy: str | None = None
    target_table: str = ""
    sheets: list[SheetRule] = field(default_factory=list)
    skip_sheets: list[str] = field(default_factory=list)


@dataclass
class SourceConfig:
    key: str
    display_name: str
    folder: str
    discovery_mode: Literal["explicit", "all"]
    files: list[dict] = field(default_factory=list)
    file_rules: list[FileRule] = field(default_factory=list)
    min_files: int | None = None
    account_type_extract: dict | None = None
    store_name_extract: dict | None = None
    skip_non_data_files: list[str] = field(default_factory=list)
    extra_columns: dict = field(default_factory=dict)


@dataclass
class SourcesManifest:
    smb_base: str
    sources: list[SourceConfig]

    def resolve_path(self, source: SourceConfig, dt: date) -> Path:
        date_str = dt.strftime("%Y-%m-%d")
        return Path(self.smb_base) / source.folder / date_str


def _parse_sheet_rule(raw: dict) -> SheetRule:
    return SheetRule(
        sheet=raw["sheet"],
        header_row=raw.get("header_row", 1),
        header_rows=raw.get("header_rows", 1),
        merge_strategy=raw.get("merge_strategy"),
        skip_rows=raw.get("skip_rows", []),
        target_table=raw.get("target_table", ""),
    )


def _parse_file_rule(raw: dict) -> FileRule:
    sheets = [_parse_sheet_rule(s) for s in raw.get("sheets", [])]
    return FileRule(
        match=raw["match"],
        format=raw["format"],
        encoding=raw.get("encoding", "utf-8"),
        sheet=raw.get("sheet"),
        header_row=raw.get("header_row", 1),
        header_rows=raw.get("header_rows", 1),
        merge_strategy=raw.get("merge_strategy"),
        target_table=raw.get("target_table", ""),
        sheets=sheets,
        skip_sheets=raw.get("skip_sheets", []),
    )


def load_sources(config_path: str | Path | None = None) -> SourcesManifest:
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config" / "sources.yaml"
    config_path = Path(config_path)

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    smb_base = raw["global"]["smb_base"]
    sources = []

    for key, src in raw["sources"].items():
        if not src.get("enabled", True):
            continue
        file_rules = [_parse_file_rule(r) for r in src.get("file_rules", [])]
        sources.append(
            SourceConfig(
                key=key,
                display_name=src["display_name"],
                folder=src["folder"],
                discovery_mode=src.get("discovery_mode", "all"),
                files=src.get("files", []),
                file_rules=file_rules,
                min_files=src.get("min_files"),
                account_type_extract=src.get("account_type_extract"),
                store_name_extract=src.get("store_name_extract"),
                skip_non_data_files=src.get("skip_non_data_files", []),
            )
        )

    return SourcesManifest(smb_base=smb_base, sources=sources)


def extract_tag_from_filename(filename: str, extract_config: dict | None) -> str:
    """从文件名中提取标签（账号类型、门店名等）"""
    if not extract_config:
        return ""
    pattern = extract_config.get("pattern", "")
    default = extract_config.get("default", "")
    m = re.search(pattern, filename)
    return m.group(1) if m else default


def match_file_rule(filename: str, rules: list[FileRule]) -> FileRule | None:
    """根据文件名匹配对应的 FileRule（支持 fnmatch 通配符）"""
    import fnmatch

    for rule in rules:
        if fnmatch.fnmatch(filename, rule.match):
            return rule
    return None
