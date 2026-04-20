"""V1 公共契约。

这一层是面向模型的正交 public schema：
- source
- time
- semantic / field filters
- result algebra

它不泄漏 internal id，也不直接暴露 compiler-facing 结构。
"""

from __future__ import annotations

from copy import deepcopy
import csv
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .compile_request import (
    DetailLimitSpec,
    FieldConstraintSpec,
    FinalResultSpec,
    LoadedRequest,
    MetricSpec,
    ParsedRequest,
    RequestParseError,
    SemanticConstraintSpec,
    SortSpec,
    TopKSpec,
)
from .knowledge import choose_field_match
from .operators import load_operator_registry, normalize_regex_pattern
from .public_time import CANONICAL_PRESETS, CANONICAL_RELATIVE_UNITS, PublicRelativeTime, lower_between, lower_relative_time, lower_time_preset

ROOT = Path(__file__).resolve().parents[2]
RULES_PATH = ROOT / "references" / "biz_semantic_rules.json"
PUBLIC_REQUEST_VERSION = 1
PUBLIC_SOURCES = {"日志": "event", "告警": "alarm_merge", "原始告警": "alarm"}
SEMANTIC_OPERATORS = {"==", "!="}
SENTINEL_SLOT_TOKENS = {"_all", "_none", "all", "none"}


class StrictPublicModel(BaseModel):
    """V1 公共层统一使用的严格 Pydantic 基类。"""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, populate_by_name=True)


def normalize_singular_slot(
    value: Any,
    *,
    path: str,
    allow_empty: bool,
    empty_message: str,
    sentinel_message: str,
) -> Any:
    """把“单个字符串槽位”的输入归一成稳定形态。"""
    if value is None:
        return "" if allow_empty else value
    if isinstance(value, list):
        if not value:
            return "" if allow_empty else ""
        raise ValueError(f"`{path}` 必须是单个字符串，不支持数组。")
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return "" if allow_empty else token
        if token.lower() in SENTINEL_SLOT_TOKENS:
            raise ValueError(sentinel_message)
        return token
    return value


def normalize_literal_list_slot(value: Any) -> list[Any]:
    """把 literal-list 槽位归一成稳定的 Python 列表。

    支持：
    - 真正的 JSON 数组
    - JSON 数组字符串，例如 `"[\"a\", \"b\"]"`
    - 逗号/顿号分隔的单字符串，例如 `"a,b"`、`"a，b"`、`"a、b"`
    - 单个标量值
    """
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return []
        if token.startswith("[") and token.endswith("]"):
            try:
                parsed = json.loads(token)
            except json.JSONDecodeError:
                pass
            else:
                if isinstance(parsed, list):
                    return list(parsed)
        if any(delimiter in token for delimiter in [",", "，", "、"]):
            normalized = token.replace("，", ",").replace("、", ",")
            reader = csv.reader([normalized], skipinitialspace=True)
            try:
                items = next(reader)
            except StopIteration:
                items = []
            return [item.strip() for item in items if item.strip()]
    return [value]


class PublicRelativeTimeModel(StrictPublicModel):
    """V1 canonical 相对时间。"""

    unit: Literal["分钟", "小时", "天", "周", "月", "年"]
    value: int

    @field_validator("value")
    @classmethod
    def validate_value(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("`request.time.relative.value` 必须是正整数。")
        return value


class PublicBetweenTimeModel(StrictPublicModel):
    """V1 canonical 绝对时间范围。"""

    from_: str = Field(alias="from")
    to: str


class PublicTimeModel(StrictPublicModel):
    """V1 canonical 时间对象。"""

    preset: Optional[Literal["今天", "昨天", "本周", "本月", "今年"]] = None
    relative: Optional[PublicRelativeTimeModel] = None
    between: Optional[PublicBetweenTimeModel] = None

    @model_validator(mode="after")
    def validate_shape(self) -> "PublicTimeModel":
        provided = sum(item is not None for item in (self.preset, self.relative, self.between))
        if provided != 1:
            raise ValueError("`request.time` 必须且只能提供 `preset`、`relative`、`between` 其中之一。")
        return self


class PublicFieldFilterModel(StrictPublicModel):
    """V1 公共层字段过滤。"""

    field: str
    operator: str = "=="
    value: Any = None

    @field_validator("field", mode="before")
    @classmethod
    def normalize_field(cls, value: Any) -> Any:
        return normalize_singular_slot(
            value,
            path="request.field_filters.field",
            allow_empty=False,
            empty_message="`request.field_filters.field` 不能为空。",
            sentinel_message="`request.field_filters.field` 必须是单个字段短语字符串，不支持 `_all` / `_none` 这类占位值。",
        )

    @field_validator("field")
    @classmethod
    def validate_field(cls, value: str) -> str:
        if not value:
            raise ValueError("`request.field_filters.field` 不能为空。")
        return value

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, value: str) -> str:
        allowed_ops = load_operator_registry().field_filter_operators
        if value not in allowed_ops:
            allowed = "、".join(sorted(allowed_ops))
            raise ValueError(f"`operator` 只能是：{allowed}。")
        return value

    @model_validator(mode="after")
    def validate_shape(self) -> "PublicFieldFilterModel":
        capability = load_operator_registry().predicate_capability(self.operator)
        if capability["rhs_kind"] == "field":
            if isinstance(self.value, list):
                raise ValueError(f"`{self.operator}` 的 `value` 必须是单个业务归属字段字符串，不支持数组；多个候选值请改用 `any_match`。")
            normalized = normalize_singular_slot(
                self.value,
                path="request.field_filters.value",
                allow_empty=False,
                empty_message="`request.field_filters.value` 不能为空。",
                sentinel_message="`request.field_filters.value` 必须是单个字段短语字符串，不支持 `_all` / `_none` 这类占位值。",
            )
            if not isinstance(normalized, str) or not normalized:
                raise ValueError("`request.field_filters.value` 不能为空。")
            self.value = normalized
            return self
        if capability["rhs_kind"] == "literal_list":
            values = normalize_literal_list_slot(self.value)
            if not values or any(item is None for item in values):
                raise ValueError("`any_match` 的 `value` 必须是非空单值或非空数组。")
            self.value = values
            return self
        if capability["rhs_literal_format"] == "regex_literal":
            try:
                self.value = normalize_regex_pattern(self.value)
            except ValueError as exc:
                raise ValueError("`rlike` 的 `value` 必须是非空正则模式；可写 `/正则/`，也可写纯正则文本。") from exc
            return self
        if isinstance(self.value, (list, dict)):
            raise ValueError(f"`{self.operator}` 的 `value` 必须是单个字面量。")
        return self


class PublicSemanticFilterModel(StrictPublicModel):
    """V1 公共层业务语义目标过滤。"""

    target: str
    operator: Literal["==", "!="] = "=="
    value: Any = None

    @field_validator("target", mode="before")
    @classmethod
    def normalize_target(cls, value: Any) -> Any:
        return normalize_singular_slot(
            value,
            path="request.semantic_filters.target",
            allow_empty=False,
            empty_message="`request.semantic_filters.target` 不能为空。",
            sentinel_message="`request.semantic_filters.target` 必须是单个语义目标字符串，不支持 `_all` / `_none` 这类占位值。",
        )

    @field_validator("target")
    @classmethod
    def validate_target(cls, value: str) -> str:
        if not value:
            raise ValueError("`request.semantic_filters.target` 不能为空。")
        return value


class PublicMetricModel(StrictPublicModel):
    """V1 公共层指标定义。"""

    function: str = "count"
    field: str = ""
    alias: str = ""

    @field_validator("function")
    @classmethod
    def validate_function(cls, value: str) -> str:
        token = value.strip()
        allowed = load_operator_registry().stats_functions
        if token not in allowed:
            choices = "、".join(sorted(allowed))
            raise ValueError(f"`request.result.metric.function` 只能是：{choices}。")
        return token

    @field_validator("field", mode="before")
    @classmethod
    def normalize_field(cls, value: Any) -> Any:
        return normalize_singular_slot(
            value,
            path="request.result.metric.field",
            allow_empty=True,
            empty_message="`request.result.metric.field` 不能为空。",
            sentinel_message="`request.result.metric.field` 必须是单个字段短语字符串，不支持 `_all` / `_none` 这类占位值。",
        )

    @model_validator(mode="after")
    def validate_shape(self) -> "PublicMetricModel":
        registry = load_operator_registry()
        capability = registry.metric_capability(self.function)
        if self.field == "*":
            raise ValueError("`request.result.metric.field` 不支持 `*`；请省略该字段，或提供明确字段名。")
        if capability["requires_field"] and not self.field:
            raise ValueError(f"`{self.function}` 必须提供 `field`。")
        return self


class PublicTopKModel(StrictPublicModel):
    """V1 公共层 top-k 选择。"""

    limit: int
    direction: Literal["asc", "desc"] = "desc"

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("`request.result.top_k.limit` 必须是正整数。")
        return value


class PublicSortModel(StrictPublicModel):
    """V1 公共层最终结果排序。"""

    field: str
    direction: Literal["asc", "desc"] = "desc"

    @field_validator("field", mode="before")
    @classmethod
    def normalize_field(cls, value: Any) -> Any:
        return normalize_singular_slot(
            value,
            path="request.result.sort.field",
            allow_empty=False,
            empty_message="`request.result.sort.field` 不能为空。",
            sentinel_message="`request.result.sort.field` 必须是单个字段短语字符串，不支持 `_all` / `_none` 这类占位值。",
        )

    @field_validator("field")
    @classmethod
    def validate_field(cls, value: str) -> str:
        if not value:
            raise ValueError("`request.result.sort.field` 不能为空。")
        return value


class PublicResultModel(StrictPublicModel):
    """V1 公共层结果代数。"""

    type: Literal["detail", "aggregate"]
    projection: list[str] = Field(default_factory=list)
    group_by: str = ""
    metric: Optional[PublicMetricModel] = None
    top_k: Optional[PublicTopKModel] = None
    sort: Optional[PublicSortModel] = None

    @field_validator("group_by", mode="before")
    @classmethod
    def normalize_group_by(cls, value: Any) -> Any:
        return normalize_singular_slot(
            value,
            path="request.result.group_by",
            allow_empty=True,
            empty_message="如果你想表达总数统计，请省略 `group_by`。",
            sentinel_message="`request.result.group_by` 不支持 `_all` / `_none` 这类占位值；如果你想表达总数统计，请省略 `group_by`。",
        )

    @field_validator("projection")
    @classmethod
    def validate_projection(cls, values: list[str]) -> list[str]:
        cleaned = [item.strip() for item in values]
        if any(not item for item in cleaned):
            raise ValueError("`request.result.projection` 里的每一项都必须是非空字符串。")
        if "*" in cleaned:
            raise ValueError("如果你想返回原始记录，请把 `projection` 设为空数组，不要使用 `\"*\"`。")
        return cleaned

    @model_validator(mode="after")
    def validate_shape(self) -> "PublicResultModel":
        if self.top_k is not None and not self.group_by:
            raise ValueError("存在 `top_k` 时，`group_by` 不能为空。")
        if self.type == "aggregate":
            if self.projection:
                raise ValueError("`aggregate` 模式不允许 `projection`。")
            if self.top_k is not None and not self.group_by:
                raise ValueError("`aggregate` 模式只有在提供 `group_by` 时才允许 `top_k`。")
        if self.type == "detail" and self.group_by and self.top_k is None:
            raise ValueError("`detail` 模式只有在同时提供 `group_by + top_k` 时才表示“先 top-k，再回明细”。")
        if self.type == "detail" and self.metric is not None and not self.group_by:
            raise ValueError("`detail` 模式只有在提供 `group_by` 时才允许 `metric`。")
        if self.type == "aggregate" and self.top_k is not None and self.sort is not None:
            raise ValueError("`aggregate + top_k` 当前不允许再额外提供 `sort`；排行方向请使用 `top_k.direction`。")
        return self


class DraftRequestV1Model(StrictPublicModel):
    """V1 公共入口请求。"""

    request_version: Literal[PUBLIC_REQUEST_VERSION]
    source: Literal["日志", "告警", "原始告警"]
    time: Optional[PublicTimeModel] = None
    semantic_macros: list[str] = Field(default_factory=list)
    semantic_filters: list[PublicSemanticFilterModel] = Field(default_factory=list)
    field_filters: list[PublicFieldFilterModel] = Field(default_factory=list)
    result: PublicResultModel
    show_intermediate: bool = False
    raw_query: str = ""

    @field_validator("semantic_macros")
    @classmethod
    def validate_macros(cls, values: list[str]) -> list[str]:
        cleaned = [item.strip() for item in values]
        if any(not item for item in cleaned):
            raise ValueError("`semantic_macros` 里的每一项都必须是非空字符串。")
        return cleaned

    @model_validator(mode="after")
    def validate_cross_fields(self) -> "DraftRequestV1Model":
        if self.show_intermediate and not (
            self.result.type == "detail" and self.result.group_by and self.result.top_k is not None
        ):
            raise ValueError("`show_intermediate=true` 目前只支持 `detail + top_k` 场景。")
        return self


def validation_error_to_request_error(exc: ValidationError) -> RequestParseError:
    """把 V1 ValidationError 转成更可操作的 RequestParseError。"""
    first = exc.errors()[0]
    loc_parts = [str(item) for item in first.get("loc", ())]
    path = ".".join(["request", *loc_parts]) if loc_parts else "request"
    return RequestParseError(
        "invalid_request",
        f"`{path}` {first.get('msg', '请求格式不合法。')}",
        unsupported_spans=[path],
    )


@lru_cache(maxsize=1)
def public_semantic_catalog() -> dict[str, dict[str, Any]]:
    """读取 V1 公共语义目录。"""
    raw = json.loads(RULES_PATH.read_text(encoding="utf-8")) if RULES_PATH.exists() else {}
    macro_aliases: dict[str, set[str]] = {}
    target_aliases: dict[str, set[str]] = {}

    def register(bucket: dict[str, set[str]], alias: str, value: str) -> None:
        token = str(alias or "").strip()
        if token:
            bucket.setdefault(token, set()).add(value)

    for item in raw.get("macros", []):
        rule_id = str(item.get("rule_id", "")).strip()
        public_name = str(item.get("public_name", "")).strip()
        if not rule_id or not public_name:
            continue
        register(macro_aliases, public_name, rule_id)
        for alias in item.get("public_aliases", []):
            register(macro_aliases, str(alias), rule_id)

    for item in raw.get("targets", []):
        target_id = str(item.get("target_id", "")).strip()
        public_name = str(item.get("public_name", "")).strip()
        if not target_id or not public_name:
            continue
        register(target_aliases, public_name, target_id)
        for alias in item.get("public_aliases", []):
            register(target_aliases, str(alias), target_id)

    macro_map = {alias: next(iter(values)) for alias, values in macro_aliases.items() if len(values) == 1}
    macro_ambiguous = {alias: sorted(values) for alias, values in macro_aliases.items() if len(values) > 1}
    target_map = {alias: next(iter(values)) for alias, values in target_aliases.items() if len(values) == 1}
    target_ambiguous = {alias: sorted(values) for alias, values in target_aliases.items() if len(values) > 1}
    return {
        "macro_map": macro_map,
        "macro_ambiguous": macro_ambiguous,
        "target_map": target_map,
        "target_ambiguous": target_ambiguous,
    }


def resolve_public_macro(value: str) -> str:
    """把公共 macro 名称解析成 internal rule_id。"""
    catalog = public_semantic_catalog()
    token = str(value).strip()
    if token in catalog["macro_map"]:
        return catalog["macro_map"][token]
    if token in catalog["macro_ambiguous"]:
        candidates = "、".join(catalog["macro_ambiguous"][token])
        raise RequestParseError(
            "ambiguous_semantic_macro",
            f"`semantic_macros` 中的 `{value}` 存在歧义，可选宏语义：{candidates}。",
            unsupported_spans=["request.semantic_macros"],
        )
    raise RequestParseError(
        "unknown_semantic_macro",
        f"未知宏语义：`{value}`。",
        unsupported_spans=["request.semantic_macros"],
        suggestions=["请使用公共中文业务名，例如 `被模型研判过`、`被处置过`、`进程类告警`。"],
    )


def resolve_public_target(value: str) -> str:
    """把公共 target 名称解析成 internal target_id。"""
    catalog = public_semantic_catalog()
    token = str(value).strip()
    if token in catalog["target_map"]:
        return catalog["target_map"][token]
    if token in catalog["target_ambiguous"]:
        candidates = "、".join(catalog["target_ambiguous"][token])
        raise RequestParseError(
            "ambiguous_semantic_target",
            f"`semantic_filters.target` 中的 `{value}` 存在歧义，可选语义目标：{candidates}。",
            unsupported_spans=["request.semantic_filters"],
        )
    raise RequestParseError(
        "unknown_semantic_target",
        f"未知语义目标：`{value}`。",
        unsupported_spans=["request.semantic_filters"],
        suggestions=["请使用公共中文业务名，例如 `模型研判结果`、`人工研判结果`、`综合研判结果`。"],
    )


def normalize_public_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """只做 V1 入口允许的轻量 normalization。"""
    normalized = deepcopy(payload)
    warnings: list[str] = []
    if "request_version" not in normalized:
        normalized["request_version"] = PUBLIC_REQUEST_VERSION
        warnings.append("公共入口已自动补全 `request_version=1`。")
    elif isinstance(normalized.get("request_version"), str):
        token = str(normalized["request_version"]).strip().lower().removeprefix("v")
        if token == str(PUBLIC_REQUEST_VERSION):
            normalized["request_version"] = PUBLIC_REQUEST_VERSION
    return normalized, warnings


def lower_public_time(model: PublicTimeModel | None) -> dict[str, str]:
    """把 V1 公共时间对象 lower 成 internal time_range。"""
    if model is None:
        return {}
    if model.preset is not None:
        return lower_time_preset(model.preset)
    if model.relative is not None:
        return lower_relative_time(PublicRelativeTime(unit=model.relative.unit, value=model.relative.value))
    if model.between is not None:
        return lower_between(model.between.from_, model.between.to)
    return {}


def default_metric() -> MetricSpec:
    """返回公共层默认指标。"""
    capability = load_operator_registry().metric_capability("count")
    return MetricSpec(function="count", field=str(capability["default_field"] or "ID"), alias=str(capability["default_alias"] or "数量"))


def lower_metric(source: str, metric: PublicMetricModel | None) -> MetricSpec:
    """把公共 metric lower 成 internal metric。"""
    if metric is None:
        return default_metric()
    registry = load_operator_registry()
    function = metric.function or "count"
    capability = registry.metric_capability(function)
    field = metric.field or str(capability["default_field"] or "")
    if field == "*":
        raise RequestParseError(
            "invalid_metric_field",
            "`request.result.metric.field` 不支持 `*`；请省略该字段，或提供明确字段名。",
            unsupported_spans=["request.result.metric.field"],
        )
    if capability["requires_field"] and not field:
        raise RequestParseError(
            "invalid_metric_field",
            f"`{function}` 必须提供 `field`。",
            unsupported_spans=["request.result.metric.field"],
        )
    if field and field != "ID":
        field = str(choose_field_match(source, field)["name"])
    alias = metric.alias or str(capability["default_alias"] or "数量")
    return MetricSpec(function=function, field=field or str(capability["default_field"] or "ID"), alias=alias)


def lower_sort_field(source: str, field: str, metric_alias: str | None = None) -> str:
    """把公共排序字段 lower 成 internal 可排序字段。"""
    token = field.strip()
    if metric_alias and token == metric_alias:
        return metric_alias
    return str(choose_field_match(source, token)["name"])


def lower_result(source: str, result: PublicResultModel) -> tuple[FinalResultSpec, DetailLimitSpec, SortSpec | None]:
    """把 V1 公共结果代数 lower 成 internal final_result/detail_limit/sort。"""
    if result.metric is None and result.type == "detail" and not result.group_by:
        metric = MetricSpec()
    else:
        metric = lower_metric(source, result.metric)
    sort = (
        SortSpec(
            field=lower_sort_field(source, result.sort.field, metric_alias=metric.alias),
            direction=result.sort.direction,
        )
        if result.sort
        else None
    )
    top_k = TopKSpec(limit=result.top_k.limit, direction=result.top_k.direction) if result.top_k is not None else None
    return (
        FinalResultSpec(
            type=result.type,
            field_phrases=list(result.projection),
            group_by_phrase=result.group_by,
            metric=metric,
            top_k=top_k,
        ),
        DetailLimitSpec(mode="default", value=None),
        sort,
    )


def lower_public_request(model: DraftRequestV1Model) -> ParsedRequest:
    """把 V1 公共请求 lower 成 internal ParsedRequest。"""
    source = PUBLIC_SOURCES[model.source]
    field_constraints = [
        FieldConstraintSpec(field="", field_phrase=item.field, operator=item.operator, value=item.value)
        for item in model.field_filters
    ]
    semantic_constraints = [
        SemanticConstraintSpec(target_id=resolve_public_target(item.target), operator=item.operator, value=item.value)
        for item in model.semantic_filters
    ]
    final_result, detail_limit, sort = lower_result(source, model.result)
    return ParsedRequest(
        schema_version=1,
        source=source,
        time_range=lower_public_time(model.time),
        semantic_macro_ids=[resolve_public_macro(item) for item in model.semantic_macros],
        semantic_constraints=semantic_constraints,
        field_constraints=field_constraints,
        final_result=final_result,
        show_intermediate=bool(model.show_intermediate),
        detail_limit=detail_limit,
        sort=sort,
        raw_query=model.raw_query,
    )


def parse_public_request_payload(payload: dict[str, Any]) -> LoadedRequest:
    """解析并 lowering V1 公共请求。"""
    normalized_payload, warnings = normalize_public_payload(payload)
    try:
        model = DraftRequestV1Model.model_validate(normalized_payload)
    except ValidationError as exc:
        raise validation_error_to_request_error(exc) from exc
    return LoadedRequest(request=lower_public_request(model), warnings=warnings, contract="draft_v1")


def example_detail_request() -> dict[str, Any]:
    """V1 detail skeleton。"""
    return {
        "request_version": 1,
        "source": "日志",
        "time": {"preset": "今天"},
        "semantic_macros": [],
        "semantic_filters": [],
        "field_filters": [],
        "result": {"type": "detail", "projection": []},
        "show_intermediate": False,
        "raw_query": "原始中文问题",
    }


def example_aggregate_request() -> dict[str, Any]:
    """V1 aggregate-total skeleton。"""
    return {
        "request_version": 1,
        "source": "告警",
        "time": {"relative": {"unit": "天", "value": 30}},
        "semantic_macros": [],
        "semantic_filters": [],
        "field_filters": [],
        "result": {"type": "aggregate"},
        "show_intermediate": False,
        "raw_query": "原始中文问题",
    }


def example_aggregate_grouped_request() -> dict[str, Any]:
    """V1 aggregate-grouped skeleton。"""
    return {
        "request_version": 1,
        "source": "告警",
        "time": {"relative": {"unit": "天", "value": 30}},
        "semantic_macros": [],
        "semantic_filters": [],
        "field_filters": [],
        "result": {"type": "aggregate", "group_by": "攻击地址"},
        "show_intermediate": False,
        "raw_query": "原始中文问题",
    }


def example_aggregate_topk_request() -> dict[str, Any]:
    """V1 aggregate-topk skeleton。"""
    return {
        "request_version": 1,
        "source": "告警",
        "time": {"relative": {"unit": "天", "value": 30}},
        "semantic_macros": [],
        "semantic_filters": [],
        "field_filters": [],
        "result": {
            "type": "aggregate",
            "group_by": "威胁类型",
            "top_k": {"limit": 5, "direction": "desc"},
        },
        "show_intermediate": False,
        "raw_query": "原始中文问题",
    }


def example_detail_topk_request() -> dict[str, Any]:
    """V1 detail + top_k skeleton。"""
    return {
        "request_version": 1,
        "source": "告警",
        "time": {"relative": {"unit": "天", "value": 30}},
        "semantic_macros": ["被处置过"],
        "semantic_filters": [],
        "field_filters": [],
        "result": {
            "type": "detail",
            "projection": ["攻击地址"],
            "group_by": "威胁类型",
            "top_k": {"limit": 3, "direction": "desc"},
        },
        "show_intermediate": False,
        "raw_query": "原始中文问题",
    }


PUBLIC_SKELETONS = {
    "detail": example_detail_request,
    "aggregate-total": example_aggregate_request,
    "aggregate-grouped": example_aggregate_grouped_request,
    "aggregate-topk": example_aggregate_topk_request,
    "detail-topk": example_detail_topk_request,
}


def public_skeleton(name: str) -> dict[str, Any]:
    """返回 V1 官方 skeleton。"""
    if name not in PUBLIC_SKELETONS:
        choices = "、".join(sorted(PUBLIC_SKELETONS))
        raise RequestParseError(
            "unknown_skeleton",
            f"未知 skeleton：`{name}`。可选值：{choices}。",
            unsupported_spans=[name],
        )
    return PUBLIC_SKELETONS[name]()
