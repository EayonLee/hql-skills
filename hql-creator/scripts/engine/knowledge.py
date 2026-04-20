"""知识加载、业务语义绑定与字段绑定。

这个文件集中承载三类稳定知识：
1. 查询源与 index / 默认时间字段映射。
2. 字段元数据与别名检索。
3. `biz_semantic_rules.json` 中的宏语义与语义目标目录。

本文件只负责确定性绑定，不再做中文词面解析：
- 先按 macro id 绑定快捷业务语义
- 再按 target id 绑定语义目标取值
- 最后绑定显式字段短语和最终输出字段
"""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, replace
from functools import lru_cache
from pathlib import Path
from typing import Any

from rapidfuzz import fuzz

from .internal_types import (
    BoundPredicate,
    BoundPredicateAtom,
    BoundPredicateGroup,
    CompiledIntent,
    DetailLimitSpec,
    FinalResultSpec,
    DerivationSpec,
    ParsedRequest,
    ResultSpec,
    SortSpec,
    StrictSchemaError,
)
from .operators import (
    field_types_compatible,
    load_operator_registry,
    normalize_field_type,
    normalize_regex_pattern,
    value_matches_field_type,
)

# ROOT: skill 根目录，用于定位 references、agents 等目录。
ROOT = Path(__file__).resolve().parents[2]
# REFERENCES_DIR: 所有结构化参考资料所在目录。
REFERENCES_DIR = ROOT / "references"


@dataclass(frozen=True)
class SourceSpec:
    """描述一个逻辑查询源的固定元数据。"""

    source: str
    index: str
    default_time_field: str


@dataclass(frozen=True)
class MacroPredicateSpec:
    """描述宏语义中的一个原子条件。"""

    field: str
    field_key: str
    operator: str
    value: Any


@dataclass(frozen=True)
class SemanticMacro:
    """表示一条快捷业务语义。"""

    rule_id: str
    description: str
    source_scopes: tuple[str, ...]
    semantic_tag: str
    resolution_mode: str
    priority: int
    any_of: tuple[MacroPredicateSpec, ...] = ()
    all_of: tuple[MacroPredicateSpec, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """导出调试包里使用的宏语义对象。"""
        return {
            "rule_id": self.rule_id,
            "description": self.description,
            "source_scopes": list(self.source_scopes),
            "semantic_tag": self.semantic_tag,
            "resolution_mode": self.resolution_mode,
            "priority": self.priority,
            "any_of": [spec.__dict__ for spec in self.any_of],
            "all_of": [spec.__dict__ for spec in self.all_of],
        }


@dataclass(frozen=True)
class SemanticTarget:
    """表示一个业务语义目标。"""

    target_id: str
    description: str
    source_scopes: tuple[str, ...]
    field: str
    field_key: str
    semantic_tag: str
    allowed_values: tuple[Any, ...]
    value_aliases: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        """导出调试包里使用的语义目标对象。"""
        return {
            "target_id": self.target_id,
            "description": self.description,
            "source_scopes": list(self.source_scopes),
            "field": self.field,
            "field_key": self.field_key,
            "semantic_tag": self.semantic_tag,
            "allowed_values": list(self.allowed_values),
            "value_aliases": dict(self.value_aliases),
        }


# SOURCE_SPECS: 三类受支持查询源的唯一真相源。
SOURCE_SPECS: dict[str, SourceSpec] = {
    "alarm_merge": SourceSpec("alarm_merge", "alarm_merge", "开始时间"),
    "alarm": SourceSpec("alarm", "alarm", "开始时间"),
    "event": SourceSpec("event", "event*", "发生时间"),
}
# INDEX_TO_SOURCE: 反向索引，供 reviewer 在解析 nested subquery 时把 index 还原成 source。
INDEX_TO_SOURCE = {spec.index: spec.source for spec in SOURCE_SPECS.values()}

# SOURCE_FILES: 每个查询源对应的字段元数据文件。
SOURCE_FILES = {
    "alarm_merge": "alarm_merge_fields.json",
    "alarm": "alarm_fields.json",
    "event": "event_fields.json",
}

# FIELD_ALIASES: 少量高频同义词和英文别名，用于提升字段检索命中率。
FIELD_ALIASES = {
    "HTTP请求体": {"请求体", "http请求体", "request body", "http request body"},
    "HTTP请求内容": {"请求内容", "http请求内容", "request content", "http request content"},
    "HTTP响应体": {"响应体", "http响应体", "response body", "http response body"},
    "HTTP响应内容": {"响应内容", "http响应内容", "response content", "http response content"},
    "进程命令行": {"进程命令", "process command", "process command line", "proc command"},
    "进程pid": {"进程PID", "PID", "pid", "process pid"},
    "HTTP响应码": {"HTTP状态码", "状态码", "响应码", "http status", "status code"},
    "URL": {"url", "请求URL", "链接", "uri"},
    "域名": {"domain", "host"},
    "源地址": {"源IP", "src ip", "source ip", "sourceip", "攻击地址"},
    "目的地址": {"目的IP", "dst ip", "destination ip", "dest ip"},
    "主机IP": {"host ip", "主机ip"},
    "威胁信息": {"threat info"},
    "攻击结果": {"attack result"},
    "数据源": {"datasource", "data source"},
    "告警名称": {"alarm name"},
    "事件名称": {"event name"},
    "威胁类型": {"threat type"},
    "处置状态": {"处置", "人工处置", "被人工处置"},
}

# GENERIC_RECORD_ID_ALIASES: “ID” 类通用查询词，优先映射到统一的 `ID` 字段。
GENERIC_RECORD_ID_ALIASES = {
    "id",
    "ID",
    "告警id",
    "威胁告警id",
    "原始告警id",
    "日志id",
    "原始日志id",
    "合并告警id",
}
# GENERIC_RECORD_ID_FIELDS: 审查与检索时需要被视为“泛化 ID 名称”的字段集合。
GENERIC_RECORD_ID_FIELDS = {
    "告警ID",
    "威胁告警ID",
    "原始告警ID",
    "日志ID",
    "原始日志ID",
    "合并告警ID",
}

# RULES_PATH: 业务语义目录配置文件。
RULES_PATH = REFERENCES_DIR / "biz_semantic_rules.json"


def normalize(text: str) -> str:
    """把文本归一成便于做松散匹配的形式。"""
    value = unicodedata.normalize("NFKC", text or "").lower()
    return re.sub(r"[\s_\-:/,;()'\"[\]{}]+", "", value)


def resolve_source(source: str) -> SourceSpec:
    """校验并返回逻辑查询源的固定定义。"""
    key = (source or "").strip()
    if key not in SOURCE_SPECS:
        choices = "、".join(sorted(SOURCE_SPECS))
        raise ValueError(f"未知查询源：{source}。可选值：{choices}")
    return SOURCE_SPECS[key]


def index_for_source(source: str) -> str:
    """根据逻辑查询源返回 HQL 中使用的 index 字面量。"""
    return resolve_source(source).index


def default_time_field(source: str) -> str:
    """返回某个查询源默认使用的时间字段。"""
    return resolve_source(source).default_time_field


def source_for_index(index: str) -> str:
    """把 index 字面量反向映射回逻辑查询源。"""
    literal = (index or "").strip()
    if literal not in INDEX_TO_SOURCE:
        raise ValueError(f"不支持的 index 字面量：{index}")
    return INDEX_TO_SOURCE[literal]


def split_terms(text: str) -> list[str]:
    """把检索词拆成去重后的多个匹配 token。"""
    raw_terms = re.split(r"[\s,，/|]+", text.strip())
    terms: list[str] = []
    seen: set[str] = set()
    for item in [text] + raw_terms:
        token = normalize(item)
        if not token or token in seen:
            continue
        seen.add(token)
        terms.append(token)
    return terms


@lru_cache(maxsize=None)
def load_fields(source: str) -> list[dict[str, Any]]:
    """加载某个查询源的字段元数据列表。"""
    file_name = SOURCE_FILES[resolve_source(source).source]
    path = REFERENCES_DIR / file_name
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=None)
def load_catalog(source: str) -> dict[str, Any]:
    """把字段列表转换成按名称和 key 双索引的目录结构。"""
    fields = [dict(field) for field in load_fields(source)]
    by_name = {field["name"]: field for field in fields}
    by_key = {field["key"]: field for field in fields}
    return {"fields": fields, "by_name": by_name, "by_key": by_key}


def alias_map_for(field_name: str) -> set[str]:
    """返回某个字段名的同义词集合。"""
    return FIELD_ALIASES.get(field_name, set())


def preferred_field_names_for_terms(terms: list[str]) -> set[str]:
    """根据通用检索词给出优先字段名。"""
    if any(term in GENERIC_RECORD_ID_ALIASES for term in terms):
        return {"ID"}
    return set()


def field_search_tokens(field: dict[str, Any]) -> list[str]:
    """返回用于字段绑定与模糊检索的候选 token。"""
    name = field.get("name", "")
    key = field.get("key", "")
    pinyin = field.get("pinyin", "")
    tokens = [normalize(name), normalize(key), normalize(pinyin)]
    for alias in alias_map_for(name):
        tokens.append(normalize(alias))
    return [token for token in tokens if token]


def score_field(field: dict[str, Any], terms: list[str], preferred_names: set[str]) -> int:
    """按“精确优先、RapidFuzz 兜底”的规则为字段打分。"""
    candidates = field_search_tokens(field)
    if not candidates:
        return 0
    score = 0
    for term in terms:
        best = max((int(fuzz.WRatio(term, candidate)) for candidate in candidates), default=0)
        if best < 60:
            continue
        score += best
    if score == 0:
        return 0
    if field.get("name") in preferred_names:
        score += 15
    if preferred_names and field.get("name") in GENERIC_RECORD_ID_FIELDS:
        score -= 10
    if field.get("options"):
        score += 1
    return score


def trim_options(field: dict[str, Any], limit: int = 6) -> list[str]:
    """裁剪字段的枚举选项预览长度。"""
    return (field.get("options") or [])[:limit]


def search_fields(source: str | None, query_text: str, *, use_all: bool = False, limit: int = 8) -> list[dict[str, Any]]:
    """按检索词搜索字段元数据。"""
    sources = list(SOURCE_SPECS) if use_all or not source else [resolve_source(source).source]
    terms = split_terms(query_text)
    preferred_names = preferred_field_names_for_terms(terms)
    results: list[dict[str, Any]] = []

    for item_source in sources:
        for field in load_fields(item_source):
            score = score_field(field, terms, preferred_names)
            if score <= 0:
                continue
            results.append({"source": item_source, "field": field, "score": score})

    results.sort(key=lambda item: (-item["score"], item["field"]["name"]))
    return results[:limit]


def exact_field_matches(source: str, phrase: str) -> list[dict[str, Any]]:
    """按字段名 / key / alias 做严格等值匹配。"""
    token = normalize(phrase)
    matches: list[dict[str, Any]] = []
    seen: set[str] = set()
    for field in load_fields(source):
        candidates = {normalize(str(field.get("name", ""))), normalize(str(field.get("key", "")))}
        candidates.update(normalize(alias) for alias in alias_map_for(str(field.get("name", ""))))
        if token not in candidates:
            continue
        key = str(field.get("key", ""))
        if key in seen:
            continue
        seen.add(key)
        matches.append(field)
    matches.sort(key=lambda item: str(item.get("name", "")))
    return matches


def format_candidate_summary(matches: list[dict[str, Any]]) -> str:
    """把候选字段压成简短可读文本。"""
    return "、".join(f"{field['name']}({field['key']})" for field in matches[:5])


def choose_field_match(source: str, phrase: str) -> dict[str, Any]:
    """按“精确优先、模糊兜底、歧义失败”策略选择字段。"""
    exact_matches = exact_field_matches(source, phrase)
    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(exact_matches) > 1:
        raise StrictSchemaError(
            f"字段短语 `{phrase}` 在查询源 `{source}` 中存在多个精确候选：{format_candidate_summary(exact_matches)}。"
            "请改用更精确短语，或在请求里直接填写 `field`。"
        )

    fuzzy_matches = search_fields(source, phrase, limit=5)
    if not fuzzy_matches:
        raise StrictSchemaError(f"字段短语 `{phrase}` 在查询源 `{source}` 中没有匹配结果。")
    if fuzzy_matches[0]["score"] < 90:
        raise StrictSchemaError(
            f"字段短语 `{phrase}` 在查询源 `{source}` 中没有足够可信的匹配结果。"
            f"候选：{format_candidate_summary([item['field'] for item in fuzzy_matches])}。"
        )
    if len(fuzzy_matches) > 1 and fuzzy_matches[0]["score"] - fuzzy_matches[1]["score"] < 5:
        candidates = [item["field"] for item in fuzzy_matches]
        raise StrictSchemaError(
            f"字段短语 `{phrase}` 在查询源 `{source}` 中存在歧义候选：{format_candidate_summary(candidates)}。"
            "请改用更精确短语，或在请求里直接填写 `field`。"
        )
    return fuzzy_matches[0]["field"]


def serialize_search_result(item: dict[str, Any], *, show_options: bool) -> dict[str, Any]:
    """把内部字段检索结果裁剪成可输出结构。"""
    field = item["field"]
    payload = {
        "source": item["source"],
        "field_name": field["name"],
        "key": field["key"],
        "type": field["type"],
        "pinyin": field.get("pinyin", ""),
        "array": field.get("array", False),
        "score": item["score"],
    }
    aliases = sorted(alias_map_for(field["name"]))
    if aliases:
        payload["aliases"] = aliases
    if show_options and field.get("options"):
        payload["options_preview"] = trim_options(field)
        payload["options_count"] = len(field["options"])
    return payload


def source_file_name(source: str) -> str:
    """返回某个查询源对应的字段 JSON 文件名。"""
    return SOURCE_FILES[resolve_source(source).source]


def parse_macro_predicates(raw_items: list[dict[str, Any]], *, owner: str, path: str) -> tuple[MacroPredicateSpec, ...]:
    """把宏语义中的原子条件列表解析成稳定对象。"""
    predicates: list[MacroPredicateSpec] = []
    allowed_ops = load_operator_registry().field_filter_operators
    for index, item in enumerate(raw_items):
        if not isinstance(item, dict):
            raise StrictSchemaError(f"{owner} 的 {path}[{index}] 必须是对象。")
        field = str(item.get("field", "")).strip()
        operator = str(item.get("operator", "==")).strip()
        if not field:
            raise StrictSchemaError(f"{owner} 的 {path}[{index}] 必须包含 `field`。")
        if operator not in allowed_ops:
            allowed = "、".join(sorted(allowed_ops))
            raise StrictSchemaError(f"{owner} 的 {path}[{index}].operator 只能是：{allowed}。")
        predicates.append(
            MacroPredicateSpec(
                field=field,
                field_key=str(item.get("field_key", "")).strip(),
                operator=operator,
                value=item.get("value"),
            )
        )
    return tuple(predicates)


@lru_cache(maxsize=1)
def load_semantic_catalog() -> dict[str, Any]:
    """加载宏语义与语义目标目录。"""
    if not RULES_PATH.exists():
        return {"macros": [], "targets": []}
    raw_catalog = json.loads(RULES_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw_catalog, dict):
        raise StrictSchemaError("biz_semantic_rules.json 顶层必须是对象，并包含 `macros` 与 `targets`。")
    macros = raw_catalog.get("macros", [])
    targets = raw_catalog.get("targets", [])
    if not isinstance(macros, list) or not isinstance(targets, list):
        raise StrictSchemaError("biz_semantic_rules.json 中的 `macros` 与 `targets` 都必须是数组。")
    return {"macros": macros, "targets": targets}


@lru_cache(maxsize=1)
def load_semantic_macros() -> list[SemanticMacro]:
    """加载宏语义目录。"""
    macros: list[SemanticMacro] = []
    for item in load_semantic_catalog()["macros"]:
        rule_id = str(item.get("rule_id", "")).strip()
        if not rule_id:
            raise StrictSchemaError("宏语义规则必须包含 `rule_id`。")
        any_of = parse_macro_predicates(item.get("any_of", []), owner=f"宏语义 `{rule_id}`", path="any_of")
        all_of = parse_macro_predicates(item.get("all_of", []), owner=f"宏语义 `{rule_id}`", path="all_of")
        if bool(any_of) == bool(all_of):
            raise StrictSchemaError(f"宏语义 `{rule_id}` 必须且只能提供 `any_of` 或 `all_of`。")
        resolution_mode = str(item.get("resolution_mode", "suggestive")).strip()
        if resolution_mode not in {"authoritative", "suggestive"}:
            raise StrictSchemaError(f"宏语义 `{rule_id}` 的 resolution_mode 只能是 authoritative 或 suggestive。")
        macros.append(
            SemanticMacro(
                rule_id=rule_id,
                description=str(item.get("description", "")),
                source_scopes=tuple(item.get("source_scopes", [])),
                semantic_tag=str(item.get("semantic_tag", "")),
                resolution_mode=resolution_mode,
                priority=int(item.get("priority", 0)),
                any_of=any_of,
                all_of=all_of,
            )
        )
    macros.sort(key=lambda item: (-item.priority, item.rule_id))
    return macros


@lru_cache(maxsize=1)
def load_semantic_targets() -> list[SemanticTarget]:
    """加载语义目标目录。"""
    targets: list[SemanticTarget] = []
    for item in load_semantic_catalog()["targets"]:
        target_id = str(item.get("target_id", "")).strip()
        field = str(item.get("field", "")).strip()
        if not target_id or not field:
            raise StrictSchemaError("语义目标必须包含 `target_id` 与 `field`。")
        targets.append(
            SemanticTarget(
                target_id=target_id,
                description=str(item.get("description", "")),
                source_scopes=tuple(item.get("source_scopes", [])),
                field=field,
                field_key=str(item.get("field_key", "")).strip(),
                semantic_tag=str(item.get("semantic_tag", "")),
                allowed_values=tuple(item.get("allowed_values", [])),
                value_aliases={str(key): value for key, value in dict(item.get("value_aliases", {})).items()},
            )
        )
    return targets


@lru_cache(maxsize=1)
def semantic_macro_map() -> dict[str, SemanticMacro]:
    """把宏语义目录转换成 rule_id 索引。"""
    return {rule.rule_id: rule for rule in load_semantic_macros()}


@lru_cache(maxsize=1)
def semantic_target_map() -> dict[str, SemanticTarget]:
    """把语义目标目录转换成 target_id 索引。"""
    return {target.target_id: target for target in load_semantic_targets()}


def resolve_field_key(source: str, field_name: str) -> str:
    """根据字段名解析字段 key。"""
    field = choose_field_match(source, field_name)
    return str(field.get("key", ""))


def resolve_field_name(source: str, field_ref: str) -> str:
    """把字段名 / key / alias / 稳定短语解析成真实字段名。"""
    field = choose_field_match(source, field_ref)
    return str(field.get("name", ""))


def field_metadata(source: str, field_ref: str) -> dict[str, Any]:
    """把字段引用解析成字段元数据。"""
    return choose_field_match(source, field_ref)


def field_type(source: str, field_name: str) -> str:
    """返回字段的基础类型。"""
    return normalize_field_type(field_metadata(source, field_name).get("type", ""))


def field_is_array(source: str, field_name: str) -> bool:
    """判断字段是否是数组字段。"""
    return bool(field_metadata(source, field_name).get("array", False))


def normalize_predicate_atom(source: str, atom: BoundPredicateAtom) -> BoundPredicateAtom:
    """按 operator capability 把已绑定原子约束归一成稳定结构。"""
    registry = load_operator_registry()
    capability = registry.predicate_capability(atom.operator)
    if atom.operator not in registry.field_filter_operators:
        allowed = "、".join(sorted(registry.field_filter_operators))
        raise StrictSchemaError(f"`operator` 只能是：{allowed}。")

    lhs_meta = field_metadata(source, atom.field)
    lhs_type = normalize_field_type(lhs_meta.get("type", ""))
    lhs_name = str(lhs_meta.get("name", atom.field))
    lhs_key = str(lhs_meta.get("key", atom.field_key))
    lhs_is_belong_rhs = bool(lhs_meta.get("belong_rhs", False))

    if capability["lhs_allowed_types"] and lhs_type not in capability["lhs_allowed_types"]:
        allowed_types = "、".join(capability["lhs_allowed_types"])
        raise StrictSchemaError(f"字段 `{lhs_name}` 的类型 `{lhs_type}` 不支持操作符 `{atom.operator}`；允许类型：{allowed_types}。")
    if capability["lhs_requires_array"] and not bool(lhs_meta.get("array", False)):
        raise StrictSchemaError(f"`{atom.operator}` 只支持 `array=true` 字段；`{lhs_name}` 不是数组字段。")
    if capability["lhs_forbids_belong_rhs"] and lhs_is_belong_rhs:
        raise StrictSchemaError(f"`{atom.operator}` 的左侧字段 `{lhs_name}` 不能是业务归属字段。")

    if capability["rhs_kind"] == "field":
        rhs_ref = str(atom.value or "").strip()
        if not rhs_ref:
            raise StrictSchemaError(f"`{atom.operator}` 的右侧必须是单个字段短语字符串。")
        rhs_meta = field_metadata(source, rhs_ref)
        rhs_name = str(rhs_meta.get("name", rhs_ref))
        rhs_key = str(rhs_meta.get("key", ""))
        rhs_type = normalize_field_type(rhs_meta.get("type", ""))
        if bool(rhs_meta.get("array", False)):
            raise StrictSchemaError(f"`{atom.operator}` 的右侧字段 `{rhs_name}` 不能是数组字段。")
        if capability["rhs_requires_belong_rhs"] and not bool(rhs_meta.get("belong_rhs", False)):
            raise StrictSchemaError(f"`{atom.operator}` 的右侧字段 `{rhs_name}` 必须是业务归属字段。")
        if not field_types_compatible(lhs_type, rhs_type, allowed_types=capability["rhs_allowed_types"]):
            raise StrictSchemaError(f"`{atom.operator}` 要求左右字段类型兼容；`{lhs_name}` 是 `{lhs_type}`，`{rhs_name}` 是 `{rhs_type}`。")
        return replace(
            atom,
            field=lhs_name,
            field_key=lhs_key,
            value=None,
            values=[],
            rhs_field=rhs_name,
            rhs_field_key=rhs_key,
            render_kind=capability["render_kind"],
            literal_format=capability["rhs_literal_format"],
            operator_category=capability["category"],
        )

    if capability["rhs_kind"] == "literal_list":
        raw_values = list(atom.values) if atom.values else (list(atom.value) if isinstance(atom.value, list) else [atom.value])
        if not raw_values:
            raise StrictSchemaError(f"`{atom.operator}` 的 `value` 必须是非空单值或非空数组。")
        for item in raw_values:
            if isinstance(item, (list, dict)):
                raise StrictSchemaError(f"`{atom.operator}` 的列表元素必须是标量值。")
            if not value_matches_field_type(lhs_type, item):
                raise StrictSchemaError(f"`{atom.operator}` 的值 `{item}` 与字段 `{lhs_name}` 的类型 `{lhs_type}` 不兼容。")
        return replace(
            atom,
            field=lhs_name,
            field_key=lhs_key,
            value=None,
            values=list(raw_values),
            rhs_field="",
            rhs_field_key="",
            render_kind=capability["render_kind"],
            literal_format=capability["rhs_literal_format"],
            operator_category=capability["category"],
        )

    literal_value = atom.value
    if capability["rhs_literal_format"] == "regex_literal":
        try:
            literal_value = normalize_regex_pattern(atom.value)
        except ValueError as exc:
            raise StrictSchemaError(f"`{atom.operator}` 的 `value` 必须是非空正则模式；可写 `/正则/`，也可写纯正则文本。") from exc
    if isinstance(atom.value, (list, dict)):
        raise StrictSchemaError(f"操作符 `{atom.operator}` 的 `value` 必须是单个字面量。")
    if literal_value is not None and capability["rhs_literal_format"] != "regex_literal" and not value_matches_field_type(lhs_type, literal_value):
        raise StrictSchemaError(f"操作符 `{atom.operator}` 的值 `{literal_value}` 与字段 `{lhs_name}` 的类型 `{lhs_type}` 不兼容。")
    return replace(
        atom,
        field=lhs_name,
        field_key=lhs_key,
        value=literal_value,
        rhs_field="",
        rhs_field_key="",
        values=[],
        render_kind=capability["render_kind"],
        literal_format=capability["rhs_literal_format"],
        operator_category=capability["category"],
    )


def resolve_semantic_macros(source: str, macro_ids: list[str]) -> list[SemanticMacro]:
    """根据 macro id 列表解析宏语义。"""
    macros_by_id = semantic_macro_map()
    resolved: list[SemanticMacro] = []
    for macro_id in macro_ids:
        if macro_id not in macros_by_id:
            raise StrictSchemaError(f"未知宏语义：`{macro_id}`。")
        macro = macros_by_id[macro_id]
        if macro.source_scopes and source not in macro.source_scopes:
            scopes = "、".join(macro.source_scopes)
            raise StrictSchemaError(f"宏语义 `{macro_id}` 不适用于查询源 `{source}`；可用 source：{scopes}。")
        resolved.append(macro)
    return resolved


def resolve_semantic_target(source: str, target_id: str) -> SemanticTarget:
    """根据 target id 解析语义目标。"""
    targets_by_id = semantic_target_map()
    if target_id not in targets_by_id:
        raise StrictSchemaError(f"未知语义目标：`{target_id}`。")
    target = targets_by_id[target_id]
    if target.source_scopes and source not in target.source_scopes:
        scopes = "、".join(target.source_scopes)
        raise StrictSchemaError(f"语义目标 `{target_id}` 不适用于查询源 `{source}`；可用 source：{scopes}。")
    return target


def normalize_target_value(target: SemanticTarget, value: Any) -> Any:
    """根据语义目标的 alias 和枚举值归一化 value。"""
    if isinstance(value, str):
        normalized = target.value_aliases.get(value, value)
    else:
        normalized = value
    if target.allowed_values and normalized not in target.allowed_values:
        allowed = "、".join(str(item) for item in target.allowed_values)
        raise StrictSchemaError(f"语义目标 `{target.target_id}` 不支持取值 `{value}`；允许值：{allowed}。")
    return normalized


def make_time_predicates(source: str, time_range: dict[str, Any]) -> list[BoundPredicateAtom]:
    """把时间范围对象转换成显式时间字段约束。"""
    if not time_range:
        return []
    field_name = default_time_field(source)
    field_key = resolve_field_key(source, field_name)
    predicates: list[BoundPredicateAtom] = []
    if time_range.get("from"):
        predicates.append(
            BoundPredicateAtom(
                field=field_name,
                field_key=field_key,
                operator=">=",
                value=time_range["from"],
                origin="time_scope",
                authoritative=True,
                phrase=field_name,
                raw_text="time_range.from",
            )
        )
    if time_range.get("to"):
        predicates.append(
            BoundPredicateAtom(
                field=field_name,
                field_key=field_key,
                operator="<=",
                value=time_range["to"],
                origin="time_scope",
                authoritative=True,
                phrase=field_name,
                raw_text="time_range.to",
            )
        )
    return predicates


def macro_predicate_to_atom(source: str, macro: SemanticMacro, predicate: MacroPredicateSpec) -> BoundPredicateAtom:
    """把宏语义里的原子条件转换成已绑定原子约束。"""
    return BoundPredicateAtom(
        field=predicate.field,
        field_key=predicate.field_key or resolve_field_key(source, predicate.field),
        operator=predicate.operator,
        value=predicate.value,
        semantic_tag=macro.semantic_tag,
        origin="semantic_macro",
        authoritative=(macro.resolution_mode == "authoritative"),
        source_macro_id=macro.rule_id,
        phrase=macro.rule_id,
        raw_text=macro.description,
    )


def build_predicate_bindings(source: str, request: ParsedRequest) -> list[BoundPredicate]:
    """把宏语义、语义目标、字段约束和时间范围编译成谓词列表。"""
    predicates: list[BoundPredicate] = []

    for macro in resolve_semantic_macros(source, request.semantic_macro_ids):
        macro_atoms = [macro_predicate_to_atom(source, macro, item) for item in (macro.any_of or macro.all_of)]
        if macro.any_of:
            predicates.append(
                BoundPredicateGroup(
                    mode="any_of",
                    predicates=macro_atoms,
                    semantic_tag=macro.semantic_tag,
                    origin="semantic_macro",
                    authoritative=(macro.resolution_mode == "authoritative"),
                    source_macro_id=macro.rule_id,
                    phrase=macro.rule_id,
                    raw_text=macro.description,
                )
            )
        else:
            predicates.extend(macro_atoms)

    for constraint in request.semantic_constraints:
        target = resolve_semantic_target(source, constraint.target_id)
        predicates.append(
            BoundPredicateAtom(
                field=target.field,
                field_key=target.field_key or resolve_field_key(source, target.field),
                operator=constraint.operator,
                value=normalize_target_value(target, constraint.value),
                semantic_tag=target.semantic_tag,
                origin="semantic_target",
                authoritative=True,
                source_target_id=target.target_id,
                phrase=target.target_id,
                raw_text=target.description,
            )
        )

    predicates.extend(make_time_predicates(source, request.time_range))

    for constraint in request.field_constraints:
        if constraint.field:
            field_name = resolve_field_name(source, constraint.field)
            predicates.append(
                BoundPredicateAtom(
                    field=field_name,
                    field_key=resolve_field_key(source, field_name),
                    operator=constraint.operator,
                    value=constraint.value,
                    origin="explicit_field",
                    authoritative=True,
                    phrase=constraint.field,
                    raw_text=constraint.field,
                )
            )
            continue
        predicates.append(
            BoundPredicateAtom(
                field="",
                field_key="",
                operator=constraint.operator,
                value=constraint.value,
                origin="unresolved",
                authoritative=True,
                phrase=constraint.field_phrase,
                raw_text=constraint.field_phrase,
            )
        )
    return predicates


def canonical_value(value: Any) -> str:
    """把值归一成可比较、可哈希的稳定字符串。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def atom_signature(atom: BoundPredicateAtom) -> tuple[str, str, str, bool]:
    """生成原子约束的规范化签名。"""
    rhs_payload: dict[str, Any] = {}
    if atom.rhs_field or atom.rhs_field_key:
        rhs_payload["rhs_field"] = atom.rhs_field_key or atom.rhs_field
    elif atom.values:
        rhs_payload["rhs_values"] = list(atom.values)
    else:
        rhs_payload["rhs_value"] = atom.value
    return (atom.field_key or atom.field, atom.operator, canonical_value(rhs_payload), bool(atom.negated))


def group_signature(group: BoundPredicateGroup) -> tuple[str, frozenset[tuple[str, str, str, bool]]]:
    """生成原子组的规范化签名。"""
    return (group.mode, frozenset(atom_signature(atom) for atom in group.predicates if atom.field))


def predicate_priority(predicate: BoundPredicate) -> tuple[int, int, int, int]:
    """给重复谓词挑选更值得保留的版本。"""
    if isinstance(predicate, BoundPredicateGroup):
        return (
            1 if predicate.authoritative else 0,
            1 if predicate.source_macro_id else 0,
            1 if predicate.origin == "semantic_macro" else 0,
            len(predicate.predicates),
        )
    return (
        1 if predicate.authoritative else 0,
        1 if predicate.source_target_id else 0,
        1 if predicate.source_macro_id else 0,
        1 if predicate.origin != "unresolved" else 0,
    )


def dedupe_group_atoms(group: BoundPredicateGroup) -> BoundPredicateGroup:
    """去掉组内重复原子，保留信息量更高的那个。"""
    seen: dict[tuple[str, str, str, bool], BoundPredicateAtom] = {}
    ordered: list[tuple[str, str, str, bool]] = []
    for atom in group.predicates:
        signature = atom_signature(atom)
        if signature not in seen:
            seen[signature] = atom
            ordered.append(signature)
            continue
        if predicate_priority(atom) > predicate_priority(seen[signature]):
            seen[signature] = atom
    return replace(group, predicates=[seen[signature] for signature in ordered if seen[signature].field])


def has_stronger_equality(atom: BoundPredicateAtom) -> bool:
    """判断某个原子条件是否足以覆盖同字段的“非空”判断。"""
    return atom.operator == "==" and not atom.negated and atom.value not in {"", None}


def is_non_empty_guard(atom: BoundPredicateAtom) -> bool:
    """判断原子条件是否只是字段非空保护。"""
    return atom.operator == "!=" and not atom.negated and atom.value == ""


def canonicalize_bound_predicates(predicates: list[BoundPredicate]) -> list[BoundPredicate]:
    """做轻量、安全的谓词归一：去重并删除显然冗余的非空判断。"""
    deduped: list[BoundPredicate] = []
    seen: dict[tuple[Any, ...], BoundPredicate] = {}
    order: list[tuple[Any, ...]] = []

    for predicate in predicates:
        normalized = dedupe_group_atoms(predicate) if isinstance(predicate, BoundPredicateGroup) else predicate
        signature: tuple[Any, ...]
        if isinstance(normalized, BoundPredicateGroup):
            if not normalized.predicates:
                continue
            signature = ("group",) + group_signature(normalized)
        else:
            if not normalized.field:
                continue
            signature = ("atom",) + atom_signature(normalized)
        if signature not in seen:
            seen[signature] = normalized
            order.append(signature)
            continue
        if predicate_priority(normalized) > predicate_priority(seen[signature]):
            seen[signature] = normalized

    deduped = [seen[signature] for signature in order]
    stronger_equalities = {
        atom.field_key or atom.field
        for atom in deduped
        if isinstance(atom, BoundPredicateAtom) and has_stronger_equality(atom)
    }

    reduced: list[BoundPredicate] = []
    for predicate in deduped:
        if isinstance(predicate, BoundPredicateAtom):
            if is_non_empty_guard(predicate) and (predicate.field_key or predicate.field) in stronger_equalities:
                continue
            reduced.append(predicate)
            continue
        reduced.append(predicate)
    return reduced


def bind_atom_field(source: str, atom: BoundPredicateAtom) -> BoundPredicateAtom:
    """对仍未绑定字段的原子约束执行字段绑定。"""
    if atom.field:
        if not atom.field_key:
            atom = replace(atom, field_key=resolve_field_key(source, atom.field))
        return normalize_predicate_atom(source, atom)
    query_text = atom.phrase or atom.raw_text
    if not query_text:
        raise StrictSchemaError("存在无法绑定字段的约束：缺少 `field_phrase`。")
    field = choose_field_match(source, query_text)
    return normalize_predicate_atom(
        source,
        replace(atom, field=str(field["name"]), field_key=str(field["key"]), origin="generic_lookup"),
    )


def bind_fields(intent: CompiledIntent) -> CompiledIntent:
    """对仍未绑定字段的谓词执行通用字段绑定。"""
    resolved: list[BoundPredicate] = []
    for predicate in intent.bound_predicates:
        if isinstance(predicate, BoundPredicateGroup):
            resolved.append(replace(predicate, predicates=[bind_atom_field(intent.source, atom) for atom in predicate.predicates]))
            continue
        resolved.append(bind_atom_field(intent.source, predicate))
    return replace(intent, bound_predicates=resolved)


def validate_predicates(intent: CompiledIntent) -> None:
    """校验所有已绑定谓词引用的字段是否合法。"""
    catalog = load_catalog(intent.source)
    available_names = set(catalog["by_name"]) | {"ID"}
    for predicate in intent.bound_predicates:
        atoms = predicate.predicates if isinstance(predicate, BoundPredicateGroup) else [predicate]
        for atom in atoms:
            if atom.field not in available_names:
                raise StrictSchemaError(f"约束字段 `{atom.field}` 在查询源 `{intent.source}` 中不存在。")
            if atom.field_key and atom.field_key != resolve_field_key(intent.source, atom.field):
                raise StrictSchemaError(f"字段 `{atom.field}` 的 field_key `{atom.field_key}` 与目录不一致。")
            if atom.rhs_field:
                if atom.rhs_field not in available_names:
                    raise StrictSchemaError(f"约束字段 `{atom.field}` 的右侧字段 `{atom.rhs_field}` 在查询源 `{intent.source}` 中不存在。")
                if atom.rhs_field_key and atom.rhs_field_key != resolve_field_key(intent.source, atom.rhs_field):
                    raise StrictSchemaError(f"字段 `{atom.rhs_field}` 的 field_key `{atom.rhs_field_key}` 与目录不一致。")


def validate_result_fields(intent: CompiledIntent) -> CompiledIntent:
    """校验最终输出列和推导字段是否合法。"""
    catalog = load_catalog(intent.source)
    available_names = set(catalog["by_name"]) | {"ID"}
    normalized_outputs: list[ResultSpec] = []

    for output in intent.final_outputs:
        if output.role in {"field", "dimension"} and output.field not in available_names:
            raise StrictSchemaError(f"最终输出字段 `{output.field}` 在查询源 `{intent.source}` 中不存在。")
        if output.role == "metric":
            source_field = output.source_field or "ID"
            if source_field not in available_names:
                raise StrictSchemaError(f"指标输出 `{output.field}` 的 source_field `{source_field}` 不存在。")
            normalized_outputs.append(replace(output, source_field=source_field))
            continue
        normalized_outputs.append(output)

    for derivation in intent.derivations:
        if derivation.field and derivation.field not in available_names:
            raise StrictSchemaError(f"推导字段 `{derivation.field}` 在查询源 `{intent.source}` 中不存在。")
        if derivation.source_field and derivation.source_field not in available_names:
            raise StrictSchemaError(f"推导步骤 `{derivation.kind}` 的 source_field `{derivation.source_field}` 不存在。")

    metric_aliases = {output.field for output in normalized_outputs if output.role == "metric"}
    output_fields = {output.field for output in normalized_outputs}
    for sort in intent.sorts:
        if sort.field in available_names or sort.field in metric_aliases or sort.field in output_fields:
            continue
        raise StrictSchemaError(
            f"排序字段 `{sort.field}` 既不是真实字段，也不是最终结果中的可排序输出。"
            "如果你想表达“先按某个维度选前 N 个”，请把排序放进 `final_result.top_k.direction`，而不是顶层 `sort`。"
        )
    return replace(intent, final_outputs=normalized_outputs)


def infer_result_spec(intent: CompiledIntent) -> CompiledIntent:
    """补全结果策略和意图标签。"""
    tags = set(intent.intent_tags)
    for derivation in intent.derivations:
        if derivation.kind in {"aggregate_total", "group_aggregate", "group_aggregate_top_k"}:
            tags.update({"aggregation"})
        if derivation.kind in {"top_n", "ranking", "group_aggregate_top_k"}:
            tags.update({"ranking", "top_n"})
        if derivation.kind in {"subquery_filter", "top_n_select", "derived_filter"}:
            tags.update({"derived_filter", "subquery_filter", "ranked_detail"})
        if derivation.kind == "top_n_select":
            tags.update({"top_n_select", "top_n"})
    if any(item.role == "metric" for item in intent.final_outputs):
        tags.update({"aggregation"})
    return replace(intent, intent_tags=sorted(tags))


def lookup_best_field(source: str, phrase: str) -> dict[str, Any]:
    """根据字段短语选出单个最可信的字段候选。"""
    return choose_field_match(source, phrase)


def bind_output_phrase(source: str, phrase: str) -> str:
    """把原始输出短语绑定成真实字段名。"""
    field = lookup_best_field(source, phrase)
    return str(field["name"])


def resolve_metric(metric_spec: Any, *, default_alias: str) -> tuple[str, str, str]:
    """把请求中的 metric 定义解析成函数、源字段和 alias。"""
    registry = load_operator_registry()
    function = str(metric_spec.function or "count").strip() or "count"
    if function not in registry.stats_functions:
        allowed = "、".join(sorted(registry.stats_functions))
        raise StrictSchemaError(f"`metric.function` 只能是：{allowed}。")
    capability = registry.metric_capability(function)
    source_field = str(metric_spec.field or capability["default_field"] or "").strip()
    if source_field == "*":
        raise StrictSchemaError("`metric.field` 不支持 `*`；请省略该字段，或提供明确字段名。")
    if capability["requires_field"] and not source_field:
        raise StrictSchemaError(f"`{function}` 必须提供 `field`。")
    source_field = source_field or str(capability["default_field"] or "ID")
    alias = str(metric_spec.alias or capability["default_alias"] or default_alias).strip() or default_alias
    return function, source_field, alias


def resolve_detail_limit(detail_limit: DetailLimitSpec, *, default_value: int | None) -> int | None:
    """把结构化明细限制策略转换成 planner 使用的 limit。"""
    if detail_limit.mode == "default":
        return default_value
    if detail_limit.mode == "explicit":
        return int(detail_limit.value or 0)
    return None


def compile_final_result(source: str, final_result: FinalResultSpec, request: ParsedRequest) -> tuple[list[ResultSpec], list[DerivationSpec], list[SortSpec], int | None]:
    """把最终结果定义编译成输出列、推导步骤、排序与 limit。"""
    final_outputs: list[ResultSpec] = []
    derivations: list[DerivationSpec] = []
    sorts: list[SortSpec] = []

    if final_result.type == "detail":
        for phrase in final_result.field_phrases:
            final_outputs.append(ResultSpec(field=bind_output_phrase(source, phrase), role="field"))
        if final_result.group_by_phrase and final_result.top_k is None:
            raise StrictSchemaError("`final_result.type=detail` 且提供 `group_by_phrase` 时，必须同时提供 `top_k`。")
        if final_result.metric.function and not final_result.group_by_phrase:
            raise StrictSchemaError("`final_result.type=detail` 只有在提供 `group_by_phrase` 时才允许 `metric`。")
        if final_result.group_by_phrase and final_result.top_k is not None:
            group_field = bind_output_phrase(source, final_result.group_by_phrase)
            function, source_field, alias = resolve_metric(final_result.metric, default_alias="数量")
            derivations.extend(
                [
                    DerivationSpec(kind="group_aggregate", field=group_field, aggregate_function=function, source_field=source_field),
                    DerivationSpec(
                        kind="top_n_select",
                        field=group_field,
                        aggregate_function=function,
                        source_field=source_field,
                        limit=final_result.top_k.limit,
                        details={
                            "alias": alias,
                            "direction": final_result.top_k.direction,
                        },
                    ),
                    DerivationSpec(
                        kind="subquery_filter",
                        field=group_field,
                        limit=final_result.top_k.limit,
                        details={
                            "alias": alias,
                            "direction": final_result.top_k.direction,
                        },
                    ),
                ]
            )
        if request.sort:
            sorts.append(request.sort)
        return final_outputs, derivations, sorts, resolve_detail_limit(request.detail_limit, default_value=None)

    if final_result.type == "aggregate":
        function, source_field, alias = resolve_metric(final_result.metric, default_alias="数量")
        final_outputs.append(ResultSpec(field=alias, role="metric", aggregate_function=function, source_field=source_field))
        if final_result.group_by_phrase:
            group_field = bind_output_phrase(source, final_result.group_by_phrase)
            final_outputs.insert(0, ResultSpec(field=group_field, role="dimension"))
            derivations.append(
                DerivationSpec(
                    kind="group_aggregate_top_k" if final_result.top_k is not None else "group_aggregate",
                    field=group_field,
                    aggregate_function=function,
                    source_field=source_field,
                    limit=final_result.top_k.limit if final_result.top_k else None,
                    expose=True,
                    details={"alias": alias, "direction": final_result.top_k.direction if final_result.top_k else "desc"},
                )
            )
            if request.sort:
                sorts.append(request.sort)
            elif final_result.top_k is not None:
                sorts.append(SortSpec(field=alias, direction=final_result.top_k.direction))
            else:
                sorts.append(SortSpec(field=alias, direction="desc"))
            return final_outputs, derivations, sorts, final_result.top_k.limit if final_result.top_k else None

        if final_result.top_k is not None:
            raise StrictSchemaError("`final_result.type=aggregate` 只有在提供 `group_by_phrase` 时才允许 `top_k`。")
        derivations.append(
            DerivationSpec(
                kind="aggregate_total",
                aggregate_function=function,
                source_field=source_field,
                expose=True,
                details={"alias": alias},
            )
        )
        if request.sort:
            sorts.append(request.sort)
        return final_outputs, derivations, sorts, None

    raise StrictSchemaError(f"不支持的 final_result.type：{final_result.type}")


def compile_intent(request: ParsedRequest) -> CompiledIntent:
    """执行编译链前半段：宏语义 -> 语义目标 -> 字段绑定 -> 结果编译。"""
    source = resolve_source(request.source).source
    result_policy = "explicit_multi_result" if request.show_intermediate else "single_result_preferred"
    if request.show_intermediate and not (request.final_result.type == "detail" and request.final_result.group_by_phrase and request.final_result.top_k):
        raise StrictSchemaError("`show_intermediate=true` 目前只支持 `final_result.type=detail` 且显式提供 `group_by_phrase + top_k`。")

    final_outputs, derivations, sorts, limit = compile_final_result(source, request.final_result, request)
    intent = CompiledIntent(
        source=source,
        time_scope=dict(request.time_range),
        bound_predicates=build_predicate_bindings(source, request),
        final_outputs=final_outputs,
        derivations=derivations,
        result_policy=result_policy,
        result_fallback="final_only",
        can_embed_metric=True,
        sorts=sorts,
        limit=limit,
        raw_query=request.raw_query,
    )
    compiled = bind_fields(intent)
    compiled = replace(compiled, bound_predicates=canonicalize_bound_predicates(compiled.bound_predicates))
    validate_predicates(compiled)
    compiled = validate_result_fields(compiled)
    compiled = infer_result_spec(compiled)
    return compiled


def resolved_semantic_dicts(intent: CompiledIntent) -> dict[str, list[dict[str, Any]]]:
    """导出当前编译结果命中的宏语义与语义目标详情。"""
    macros = semantic_macro_map()
    targets = semantic_target_map()
    return {
        "macros": [macros[item].to_dict() for item in intent.matched_macro_ids() if item in macros],
        "targets": [targets[item].to_dict() for item in intent.matched_target_ids() if item in targets],
    }


def build_field_bindings(request: ParsedRequest, intent: CompiledIntent) -> dict[str, Any]:
    """把字段绑定轨迹整理成调试包可读结构。"""
    constraint_bindings: list[dict[str, Any]] = []
    for predicate in intent.bound_predicates:
        if isinstance(predicate, BoundPredicateGroup):
            constraint_bindings.append(
                {
                    "origin": predicate.origin,
                    "mode": predicate.mode,
                    "phrase": predicate.phrase,
                    "source_macro_id": predicate.source_macro_id,
                    "authoritative": predicate.authoritative,
                    "predicates": [
                        {
                            "field": atom.field,
                            "field_key": atom.field_key,
                            "operator": atom.operator,
                            "value": atom.value,
                        }
                        for atom in predicate.predicates
                    ],
                }
            )
            continue
        constraint_bindings.append(
            {
                "origin": predicate.origin,
                "phrase": predicate.phrase,
                "field": predicate.field,
                "field_key": predicate.field_key,
                "operator": predicate.operator,
                "value": predicate.value,
                "values": predicate.values,
                "rhs_field": predicate.rhs_field,
                "rhs_field_key": predicate.rhs_field_key,
                "render_kind": predicate.render_kind,
                "operator_category": predicate.operator_category,
                "source_macro_id": predicate.source_macro_id,
                "source_target_id": predicate.source_target_id,
                "authoritative": predicate.authoritative,
            }
        )

    output_bindings: list[dict[str, Any]] = []
    field_outputs = [item for item in intent.final_outputs if item.role == "field"]
    dimension_outputs = [item for item in intent.final_outputs if item.role == "dimension"]
    metric_outputs = [item for item in intent.final_outputs if item.role == "metric"]

    for phrase, output in zip(request.final_result.field_phrases, field_outputs):
        output_bindings.append({"phrase": phrase, "role": "field", "field": output.field})
    if request.final_result.group_by_phrase and dimension_outputs:
        output_bindings.append({"phrase": request.final_result.group_by_phrase, "role": "group_by", "field": dimension_outputs[0].field})
    if request.final_result.type == "detail" and request.final_result.group_by_phrase and request.final_result.top_k is not None:
        selector_group = next((step.field for step in intent.derivations if step.kind == "top_n_select" and step.field), "")
        output_bindings.append(
            {
                "phrase": request.final_result.group_by_phrase,
                "role": "selector_group_by",
                "field": selector_group,
                "limit": request.final_result.top_k.limit,
                "direction": request.final_result.top_k.direction,
            }
        )
    for metric in metric_outputs:
        output_bindings.append(
            {
                "phrase": request.final_result.metric.alias or metric.field,
                "role": "metric",
                "field": metric.field,
                "aggregate_function": metric.aggregate_function,
                "source_field": metric.source_field,
            }
        )
    return {"constraints": constraint_bindings, "final_result": output_bindings}
