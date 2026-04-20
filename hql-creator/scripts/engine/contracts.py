"""Internal request 解析与历史结构定义。

注意：
- 当前公共主路径使用 `DraftRequestV1`
- 当前 internal 类型真相源在 `internal_types.py`
- 这个文件主要负责 `schema_version=1` 的 internal request 解析
"""

from __future__ import annotations

from copy import deepcopy
import calendar
from datetime import datetime, timedelta
from difflib import get_close_matches
from functools import lru_cache
import json
import re
from dataclasses import asdict, dataclass, field as dc_field
from pathlib import Path
from typing import Annotated, Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .internal_types import RequestParseError as SharedRequestParseError, StrictSchemaError as SharedStrictSchemaError
from .operators import load_operator_registry

# DRAFT_REQUEST_VERSION: DraftRequestV1 的版本标记。
DRAFT_REQUEST_VERSION = 1
# REQUEST_SCHEMA_VERSION: compiler-facing 请求协议版本。
REQUEST_SCHEMA_VERSION = 1
# REQUEST_SOURCES: compiler-facing `ParsedRequest.source` 的受支持取值。
REQUEST_SOURCES = {"alarm_merge", "alarm", "event"}
# REQUEST_OPERATORS: 显式字段约束支持的谓词操作符。
REQUEST_OPERATORS = load_operator_registry().field_filter_operators
# SEMANTIC_CONSTRAINT_OPERATORS: 语义目标约束支持的比较运算符。
SEMANTIC_CONSTRAINT_OPERATORS = {"==", "!="}
# FINAL_RESULT_KINDS: `final_result.kind` 的受支持取值。
FINAL_RESULT_KINDS = {"detail", "group_count", "ranking"}
# DETAIL_LIMIT_MODES: 明细 limit 策略的受支持取值。
DETAIL_LIMIT_MODES = {"default", "explicit", "unbounded"}
# SORT_DIRECTIONS: 排序方向的受支持取值。
SORT_DIRECTIONS = {"asc", "desc"}
# DRAFT_TIME_KINDS: DraftRequestV1.time.kind 的受支持取值。
DRAFT_TIME_KINDS = {"preset", "relative", "between"}
TIME_PRESET_VALUES = {"today", "yesterday", "this_week", "this_month", "this_year"}
RELATIVE_TIME_UNITS = {"minute", "hour", "day", "week", "month", "year"}

# SOURCE_ALIASES: DraftRequestV1.source 允许的稳定 alias。
SOURCE_ALIASES = {
    "alarm_merge": {"alarm_merge", "告警", "威胁告警", "合并告警"},
    "alarm": {"alarm", "原始告警"},
    "event": {"event", "日志", "原始日志"},
}
SOURCE_COMPAT_ALIASES = {
    "event": {"log", "logs"},
}
SEMANTIC_RULES_PATH = Path(__file__).resolve().parents[2] / "references" / "biz_semantic_rules.json"
# RESULT_POLICIES: 查询结果集策略的受支持取值。
RESULT_POLICIES = {"final_only", "single_result_preferred", "explicit_multi_result"}
# RESULT_FALLBACK_POLICIES: 单结果优先时，无法合并统计信息后的退化策略。
RESULT_FALLBACK_POLICIES = {"final_only", "error"}
# RESULT_OUTPUT_ROLES: 最终输出列的受支持角色。
RESULT_OUTPUT_ROLES = {"field", "dimension", "metric"}
# CONSTRAINT_ORIGINS: 约束字段绑定来源。
CONSTRAINT_ORIGINS = {
    "explicit_field",
    "semantic_macro",
    "semantic_target",
    "generic_lookup",
    "time_scope",
    "unresolved",
}
# PREDICATE_GROUP_MODES: 受支持的原子组模式。
PREDICATE_GROUP_MODES = {"all_of", "any_of"}


class StrictSchemaError(SharedStrictSchemaError):
    """表示编译阶段遇到了不合法的结构或不被支持的组合。"""


class RequestParseError(SharedRequestParseError):
    """表示外部结构化请求不符合公共或兼容契约。"""

    def __init__(
        self,
        error_code: str,
        message: str,
        *,
        unsupported_spans: list[str] | None = None,
        suggestions: list[str] | None = None,
        unknown_keys: list[str] | None = None,
        nearest_valid_keys: dict[str, list[str]] | None = None,
        suggested_shape: str = "",
        example_request: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            error_code,
            message,
            unsupported_spans=unsupported_spans,
            suggestions=suggestions,
            unknown_keys=unknown_keys,
            nearest_valid_keys=nearest_valid_keys,
            suggested_shape=suggested_shape,
            example_request=example_request,
        )
        self.error_code = error_code
        self.message = message
        self.unsupported_spans = unsupported_spans or []
        self.suggestions = suggestions or []
        self.unknown_keys = unknown_keys or []
        self.nearest_valid_keys = nearest_valid_keys or {}
        self.suggested_shape = suggested_shape
        self.example_request = example_request

    def to_dict(self) -> dict[str, Any]:
        """导出成 JSON 可序列化的错误对象。"""
        payload = {
            "error_code": self.error_code,
            "message": self.message,
            "unsupported_spans": list(self.unsupported_spans),
            "suggestions": list(self.suggestions),
        }
        if self.unknown_keys:
            payload["unknown_keys"] = list(self.unknown_keys)
        if self.nearest_valid_keys:
            payload["nearest_valid_keys"] = dict(self.nearest_valid_keys)
        if self.suggested_shape:
            payload["suggested_shape"] = self.suggested_shape
        if self.example_request is not None:
            payload["example_request"] = self.example_request
        return payload


def request_error(path: str, message: str, *, code: str = "invalid_request") -> RequestParseError:
    """构造带字段路径的结构化请求错误。"""
    return RequestParseError(code, message, unsupported_spans=[path] if path else [])


def ensure_dict(value: Any, path: str) -> dict[str, Any]:
    """确保某个值是对象。"""
    if not isinstance(value, dict):
        raise request_error(path, f"`{path}` 必须是对象。")
    return value


def ensure_list(value: Any, path: str) -> list[Any]:
    """确保某个值是数组。"""
    if not isinstance(value, list):
        raise request_error(path, f"`{path}` 必须是数组。")
    return value


def ensure_str(value: Any, path: str, *, allow_empty: bool = True) -> str:
    """确保某个值是字符串。"""
    if not isinstance(value, str):
        raise request_error(path, f"`{path}` 必须是字符串。")
    if not allow_empty and not value.strip():
        raise request_error(path, f"`{path}` 不能为空字符串。")
    return value


def ensure_bool(value: Any, path: str) -> bool:
    """确保某个值是布尔值。"""
    if not isinstance(value, bool):
        raise request_error(path, f"`{path}` 必须是布尔值。")
    return value


def ensure_int(value: Any, path: str) -> int:
    """确保某个值是整数。"""
    if isinstance(value, bool) or not isinstance(value, int):
        raise request_error(path, f"`{path}` 必须是整数。")
    return value


def reject_unknown_keys(payload: dict[str, Any], allowed: set[str], path: str) -> None:
    """拒绝对象中的未知字段。"""
    unknown = sorted(set(payload) - allowed)
    if unknown:
        unknown_text = "、".join(unknown)
        raise request_error(path, f"`{path}` 包含未支持字段：{unknown_text}。", code="unknown_keys")


def parse_optional_object(payload: dict[str, Any], key: str, path: str) -> dict[str, Any] | None:
    """读取一个可选对象字段。"""
    value = payload.get(key)
    if value is None:
        return None
    return ensure_dict(value, f"{path}.{key}")


def normalize_contract_token(value: str) -> str:
    """把 source alias 等简单 token 归一成便于查表的形式。"""
    return "".join(char for char in str(value).strip().lower() if char not in " _-:/,;()[]{}'\"")


def resolve_source_alias(raw_source: str) -> str:
    """把 DraftRequest 里的 source alias 归一成稳定查询源。"""
    token = normalize_contract_token(raw_source)
    for canonical, aliases in SOURCE_ALIASES.items():
        normalized_aliases = {normalize_contract_token(item) for item in aliases}
        if token in normalized_aliases:
            return canonical
    for canonical, aliases in SOURCE_COMPAT_ALIASES.items():
        normalized_aliases = {normalize_contract_token(item) for item in aliases}
        if token in normalized_aliases:
            return canonical
    raise RequestParseError(
        "unknown_source_alias",
        f"`request.source` 不支持 `{raw_source}`。请只使用三组稳定叫法：日志 / 原始日志、告警 / 威胁告警 / 合并告警、原始告警。",
        unsupported_spans=["request.source"],
        suggestions=[
            "日志查询请使用 `日志` 或 `原始日志`。",
            "告警查询请使用 `告警`、`威胁告警` 或 `合并告警`。",
            "原始告警请使用 `原始告警`。",
        ],
    )


class StrictRequestModel(BaseModel):
    """外部请求层统一使用的严格 Pydantic 基类。"""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, populate_by_name=True)


class DraftPresetTimeModel(StrictRequestModel):
    """DraftRequestV1 中的固定时间预设。"""

    kind: Literal["preset"]
    preset: Literal["today", "yesterday", "this_week", "this_month", "this_year"]


class DraftRelativeTimeModel(StrictRequestModel):
    """DraftRequestV1 中的相对时间范围。"""

    kind: Literal["relative"]
    unit: Literal["minute", "hour", "day", "week", "month", "year"]
    value: int

    @field_validator("value")
    @classmethod
    def validate_value(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("`time.value` 必须是正整数。")
        return value


class DraftBetweenTimeModel(StrictRequestModel):
    """DraftRequestV1 的绝对时间范围。"""

    kind: Literal["between"]
    from_: str = Field(alias="from")
    to: str


DraftTimeModel = Annotated[
    Union[DraftPresetTimeModel, DraftRelativeTimeModel, DraftBetweenTimeModel],
    Field(discriminator="kind"),
]


class DraftFieldFilterModel(StrictRequestModel):
    """DraftRequestV1 中的字段条件。"""

    field: str
    operator: str = "=="
    value: Any = None

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, value: str) -> str:
        if value not in REQUEST_OPERATORS:
            allowed = "、".join(sorted(REQUEST_OPERATORS))
            raise ValueError(f"`operator` 只能是：{allowed}。")
        return value


class DraftSemanticFilterModel(StrictRequestModel):
    """DraftRequestV1 中的语义目标约束。"""

    target: str
    operator: Literal["==", "!="] = "=="
    value: Any = None


class DraftMetricModel(StrictRequestModel):
    """DraftRequestV1 中可选的指标定义。"""

    function: str = ""
    field: str = ""
    alias: str = ""


class DraftSortModel(StrictRequestModel):
    """DraftRequestV1 中的最终结果排序。"""

    field: str
    direction: Literal["asc", "desc"] = "desc"


class DraftSelectorModel(StrictRequestModel):
    """DraftRequestV1 中的 top-k 选择器。"""

    group_by: str
    limit: int
    direction: Literal["asc", "desc"] = "desc"
    metric: DraftMetricModel = Field(default_factory=DraftMetricModel)

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("`selector.limit` 必须是正整数。")
        return value


class DraftDetailResultModel(StrictRequestModel):
    """DraftRequestV1 的 detail 结果。"""

    kind: Literal["detail"]
    projection: list[str] = Field(default_factory=list)
    selector: Optional[DraftSelectorModel] = None
    limit: Optional[Union[int, Literal["unbounded"]]] = None
    sort: Optional[DraftSortModel] = None

    @field_validator("projection")
    @classmethod
    def validate_projection(cls, values: list[str]) -> list[str]:
        cleaned = [item.strip() for item in values]
        if any(not item for item in cleaned):
            raise ValueError("`result.projection` 里的每一项都必须是非空字符串。")
        if "*" in cleaned:
            raise ValueError("如果你想返回原始记录，请把 `result.projection` 设为空数组，不要使用 `\"*\"`。")
        return cleaned


class DraftGroupCountResultModel(StrictRequestModel):
    """DraftRequestV1 的分组统计结果。"""

    kind: Literal["group_count"]
    group_by: str
    metric: DraftMetricModel = Field(default_factory=DraftMetricModel)
    direction: Literal["asc", "desc"] = "desc"


class DraftRankingResultModel(StrictRequestModel):
    """DraftRequestV1 的排行结果。"""

    kind: Literal["ranking"]
    group_by: str
    limit: int

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("`result.limit` 必须是正整数。")
        return value


DraftResultModel = Annotated[
    Union[DraftDetailResultModel, DraftGroupCountResultModel, DraftRankingResultModel],
    Field(discriminator="kind"),
]


class DraftRequestModel(StrictRequestModel):
    """模型侧更小、更低熵的公共请求契约。"""

    request_version: Literal[DRAFT_REQUEST_VERSION]
    source: str
    time: Optional[DraftTimeModel] = None
    semantic_macros: list[str] = Field(default_factory=list)
    semantic_filters: list[DraftSemanticFilterModel] = Field(default_factory=list)
    field_filters: list[DraftFieldFilterModel] = Field(default_factory=list)
    result: DraftResultModel
    show_intermediate: bool = False
    raw_query: str = ""

    @field_validator("semantic_macros")
    @classmethod
    def validate_macros(cls, values: list[str]) -> list[str]:
        cleaned = [item.strip() for item in values]
        if any(not item for item in cleaned):
            raise ValueError("`semantic_macros` 里的每一项都必须是非空字符串。")
        return cleaned


def normalize_request_operator(value: Any) -> Any:
    """把 DraftRequest 中常见的 operator 简写归一成 canonical 形式。"""
    if not isinstance(value, str):
        return value
    token = value.strip().lower()
    aliases = {
        "=": "==",
        "eq": "==",
        "neq": "!=",
        "<>": "!=",
    }
    return aliases.get(token, value)


@lru_cache(maxsize=1)
def public_semantic_alias_catalog() -> dict[str, Any]:
    """加载 DraftRequest 公共层使用的语义 alias 目录。"""
    if not SEMANTIC_RULES_PATH.exists():
        return {"macro_aliases": {}, "macro_ambiguous": {}, "target_aliases": {}, "target_ambiguous": {}}
    raw = json.loads(SEMANTIC_RULES_PATH.read_text(encoding="utf-8"))
    macro_aliases: dict[str, set[str]] = {}
    target_aliases: dict[str, set[str]] = {}

    for item in raw.get("macros", []):
        rule_id = str(item.get("rule_id", "")).strip()
        if not rule_id:
            continue
        candidates = {rule_id, str(item.get("semantic_tag", "")).strip()}
        candidates.update(str(alias).strip() for alias in item.get("match_any", []))
        for alias in candidates:
            if alias:
                macro_aliases.setdefault(normalize_contract_token(alias), set()).add(rule_id)

    for item in raw.get("targets", []):
        target_id = str(item.get("target_id", "")).strip()
        if not target_id:
            continue
        candidates = {
            target_id,
            str(item.get("semantic_tag", "")).strip(),
            str(item.get("field", "")).strip(),
            str(item.get("field_key", "")).strip(),
        }
        candidates.update(str(alias).strip() for alias in item.get("match_any", []))
        for alias in candidates:
            if alias:
                target_aliases.setdefault(normalize_contract_token(alias), set()).add(target_id)

    macro_unique = {alias: next(iter(values)) for alias, values in macro_aliases.items() if len(values) == 1}
    macro_ambiguous = {alias: sorted(values) for alias, values in macro_aliases.items() if len(values) > 1}
    target_unique = {alias: next(iter(values)) for alias, values in target_aliases.items() if len(values) == 1}
    target_ambiguous = {alias: sorted(values) for alias, values in target_aliases.items() if len(values) > 1}
    return {
        "macro_aliases": macro_unique,
        "macro_ambiguous": macro_ambiguous,
        "target_aliases": target_unique,
        "target_ambiguous": target_ambiguous,
    }


def resolve_public_macro_ref(raw_value: str) -> str:
    """把 DraftRequest 中的宏语义引用归一成 rule_id。"""
    token = normalize_contract_token(raw_value)
    catalog = public_semantic_alias_catalog()
    if token in catalog["macro_aliases"]:
        return catalog["macro_aliases"][token]
    if token in catalog["macro_ambiguous"]:
        candidates = "、".join(catalog["macro_ambiguous"][token])
        raise RequestParseError(
            "ambiguous_semantic_macro",
            f"`semantic_macros` 中的 `{raw_value}` 存在歧义，可选宏语义：{candidates}。",
            unsupported_spans=["request.semantic_macros"],
        )
    raise RequestParseError(
        "unknown_semantic_macro",
        f"未知宏语义：`{raw_value}`。",
        unsupported_spans=["request.semantic_macros"],
        suggestions=["优先使用稳定短语，如 `被模型研判过`、`被处置过`、`进程类告警`。", "也可以直接填写对应的 `rule_id`。"],
    )


def resolve_public_target_ref(raw_value: str) -> str:
    """把 DraftRequest 中的语义目标引用归一成 target_id。"""
    token = normalize_contract_token(raw_value)
    catalog = public_semantic_alias_catalog()
    if token in catalog["target_aliases"]:
        return catalog["target_aliases"][token]
    if token in catalog["target_ambiguous"]:
        candidates = "、".join(catalog["target_ambiguous"][token])
        raise RequestParseError(
            "ambiguous_semantic_target",
            f"`semantic_filters.target` 中的 `{raw_value}` 存在歧义，可选语义目标：{candidates}。",
            unsupported_spans=["request.semantic_filters"],
        )
    raise RequestParseError(
        "unknown_semantic_target",
        f"未知语义目标：`{raw_value}`。",
        unsupported_spans=["request.semantic_filters"],
        suggestions=["优先使用 `llm_judgement`、`manual_judgement`、`final_judgement` 之一。"],
    )


def normalize_draft_time_payload(payload: Any) -> Any:
    """归一化 DraftRequest 的公共时间写法。"""
    if isinstance(payload, str):
        compact = normalize_contract_token(payload)
        preset_aliases = {
            "today": "today",
            "今天": "today",
            "yesterday": "yesterday",
            "昨天": "yesterday",
            "thisweek": "this_week",
            "本周": "this_week",
            "thismonth": "this_month",
            "本月": "this_month",
            "thisyear": "this_year",
            "今年": "this_year",
        }
        if compact in preset_aliases:
            return {"kind": "preset", "preset": preset_aliases[compact]}
        for separator in (" - ", " ~ ", " 至 ", "~", "至"):
            if separator in payload:
                left, right = payload.split(separator, 1)
                return {"kind": "between", "from": left.strip(), "to": right.strip()}
        relative_match = re.fullmatch(
            r"(?:last|recent|past)(\d+)(minute|minutes|min|mins|m|hour|hours|h|day|days|d|week|weeks|w|month|months|mon|year|years|y)",
            compact,
        )
        if relative_match:
            value, unit_token = relative_match.groups()
            unit_map = {
                "minute": "minute",
                "minutes": "minute",
                "min": "minute",
                "mins": "minute",
                "m": "minute",
                "hour": "hour",
                "hours": "hour",
                "h": "hour",
                "day": "day",
                "days": "day",
                "d": "day",
                "week": "week",
                "weeks": "week",
                "w": "week",
                "month": "month",
                "months": "month",
                "mon": "month",
                "year": "year",
                "years": "year",
                "y": "year",
            }
            return {"kind": "relative", "unit": unit_map[unit_token], "value": int(value)}
        return payload
    if not isinstance(payload, dict):
        return payload
    normalized = deepcopy(payload)
    if "kind" not in normalized and ({"from", "to"} <= set(normalized) or "range" in normalized):
        normalized["kind"] = "between"
    if "kind" not in normalized:
        if "type" in normalized:
            normalized["kind"] = normalized.pop("type")
        elif "preset" in normalized:
            normalized["kind"] = normalized.pop("preset")
        elif "mode" in normalized:
            normalized["kind"] = normalized.pop("mode")

    kind = normalized.get("kind")
    if isinstance(kind, str):
        compact = normalize_contract_token(kind)
        preset_aliases = {
            "today": "today",
            "今天": "today",
            "yesterday": "yesterday",
            "昨天": "yesterday",
            "thisweek": "this_week",
            "本周": "this_week",
            "thismonth": "this_month",
            "本月": "this_month",
            "thisyear": "this_year",
            "今年": "this_year",
        }
        if compact in preset_aliases:
            normalized["kind"] = "preset"
            normalized["preset"] = preset_aliases[compact]
            normalized.pop("value", None)
            normalized.pop("unit", None)
            return normalized
        shorthand_match = re.fullmatch(r"(?:last|recent|past)(\d+)(day|days|d|hour|hours|h)", compact)
        if shorthand_match:
            value, unit_token = shorthand_match.groups()
            normalized["kind"] = "relative"
            normalized["unit"] = "day" if unit_token in {"day", "days", "d"} else "hour"
            normalized["value"] = int(value)
            kind = normalized["kind"]
        extended_match = re.fullmatch(
            r"(?:last|recent|past)(\d+)(minute|minutes|min|mins|m|hour|hours|h|day|days|d|week|weeks|w|month|months|mon|year|years|y)",
            compact,
        )
        if extended_match:
            value, unit_token = extended_match.groups()
            unit_map = {
                "minute": "minute",
                "minutes": "minute",
                "min": "minute",
                "mins": "minute",
                "m": "minute",
                "hour": "hour",
                "hours": "hour",
                "h": "hour",
                "day": "day",
                "days": "day",
                "d": "day",
                "week": "week",
                "weeks": "week",
                "w": "week",
                "month": "month",
                "months": "month",
                "mon": "month",
                "year": "year",
                "years": "year",
                "y": "year",
            }
            normalized["kind"] = "relative"
            normalized["unit"] = unit_map[unit_token]
            normalized["value"] = int(value)
            kind = normalized["kind"]
    if kind == "recent_days":
        normalized["kind"] = "relative"
        normalized.setdefault("unit", "day")
        if "value" not in normalized and "days" in normalized:
            normalized["value"] = normalized["days"]
        normalized.setdefault("value", payload.get("value"))
    elif kind == "recent_hours":
        normalized["kind"] = "relative"
        normalized.setdefault("unit", "hour")
        if "value" not in normalized and "hours" in normalized:
            normalized["value"] = normalized["hours"]
        normalized.setdefault("value", payload.get("value"))
    elif kind == "recent_minutes":
        normalized["kind"] = "relative"
        normalized.setdefault("unit", "minute")
        if "value" not in normalized and "minutes" in normalized:
            normalized["value"] = normalized["minutes"]
        normalized.setdefault("value", payload.get("value"))
    elif kind == "recent_weeks":
        normalized["kind"] = "relative"
        normalized.setdefault("unit", "week")
        if "value" not in normalized and "weeks" in normalized:
            normalized["value"] = normalized["weeks"]
        normalized.setdefault("value", payload.get("value"))
    elif kind == "recent_months":
        normalized["kind"] = "relative"
        normalized.setdefault("unit", "month")
        if "value" not in normalized and "months" in normalized:
            normalized["value"] = normalized["months"]
        normalized.setdefault("value", payload.get("value"))
    elif kind == "recent_years":
        normalized["kind"] = "relative"
        normalized.setdefault("unit", "year")
        if "value" not in normalized and "years" in normalized:
            normalized["value"] = normalized["years"]
        normalized.setdefault("value", payload.get("value"))

    if normalized.get("kind") == "relative":
        has_days = "days" in normalized
        has_hours = "hours" in normalized
        has_minutes = "minutes" in normalized
        has_weeks = "weeks" in normalized
        has_months = "months" in normalized
        has_years = "years" in normalized
        if sum(bool(item) for item in (has_minutes, has_hours, has_days, has_weeks, has_months, has_years)) > 1 and "unit" not in normalized:
            raise RequestParseError(
                "ambiguous_time_shape",
                "`request.time` 同时提供了多个相对时间单位，请明确指定一种。",
                unsupported_spans=["request.time"],
            )
        if "unit" not in normalized:
            if has_minutes:
                normalized["unit"] = "minute"
            elif has_days:
                normalized["unit"] = "day"
            elif has_hours:
                normalized["unit"] = "hour"
            elif has_weeks:
                normalized["unit"] = "week"
            elif has_months:
                normalized["unit"] = "month"
            elif has_years:
                normalized["unit"] = "year"
        if "value" not in normalized:
            if normalized.get("unit") == "minute" and has_minutes:
                normalized["value"] = normalized["minutes"]
            elif normalized.get("unit") == "day" and has_days:
                normalized["value"] = normalized["days"]
            elif normalized.get("unit") == "hour" and has_hours:
                normalized["value"] = normalized["hours"]
            elif normalized.get("unit") == "week" and has_weeks:
                normalized["value"] = normalized["weeks"]
            elif normalized.get("unit") == "month" and has_months:
                normalized["value"] = normalized["months"]
            elif normalized.get("unit") == "year" and has_years:
                normalized["value"] = normalized["years"]
        normalized.pop("minutes", None)
        normalized.pop("days", None)
        normalized.pop("hours", None)
        normalized.pop("weeks", None)
        normalized.pop("months", None)
        normalized.pop("years", None)
    if normalized.get("kind") == "between" and "range" in normalized and ("from" not in normalized or "to" not in normalized):
        range_value = str(normalized.get("range", "")).strip()
        for separator in (" - ", " ~ ", " 至 ", "~", "至"):
            if separator in range_value:
                left, right = range_value.split(separator, 1)
                normalized["from"] = left.strip()
                normalized["to"] = right.strip()
                break
        normalized.pop("range", None)
    return normalized


def normalize_draft_metric_payload(payload: Any) -> Any:
    """归一化 DraftRequest 中的 metric 简写。"""
    if not isinstance(payload, dict):
        return payload
    normalized = deepcopy(payload)
    if "count" in normalized:
        count_target = normalized.pop("count")
        field = "ID"
        if isinstance(count_target, str) and count_target.strip() and count_target.strip() != "*":
            field = count_target.strip()
        normalized.setdefault("function", "count")
        normalized.setdefault("field", field)
        normalized.setdefault("alias", "数量")
    if normalized.get("field") == "*":
        normalized["field"] = "ID"
    return normalized


def normalize_draft_result_payload(payload: Any) -> Any:
    """归一化 DraftRequest 的 result 写法。"""
    if not isinstance(payload, dict):
        return payload
    normalized = deepcopy(payload)
    raw_kind = normalized.get("kind")
    kind_token = normalize_contract_token(raw_kind) if isinstance(raw_kind, str) else ""

    if "return_fields" in normalized and "projection" not in normalized:
        normalized["projection"] = normalized.pop("return_fields")
    if "group_field" in normalized and "group_by" not in normalized:
        normalized["group_by"] = normalized.pop("group_field")

    if kind_token in {"detailselector", "detail_selector"}:
        normalized["kind"] = "detail"

    projection = normalized.get("projection")
    has_projection = isinstance(projection, list) and bool(projection)
    selector_payload = normalized.get("selector")
    if isinstance(selector_payload, dict):
        selector_payload = deepcopy(selector_payload)
        if "group_field" in selector_payload and "group_by" not in selector_payload:
            selector_payload["group_by"] = selector_payload.pop("group_field")
        if "selector_count" in selector_payload and "limit" not in selector_payload:
            selector_payload["limit"] = selector_payload.pop("selector_count")
        if "top" in selector_payload and "limit" not in selector_payload:
            selector_payload["limit"] = selector_payload.pop("top")
        selector_payload.pop("order_by", None)
        selector_payload.pop("selector_type", None)

    if has_projection and kind_token == "ranking":
        normalized["kind"] = "detail"
        selector_payload = selector_payload if isinstance(selector_payload, dict) else {}
        if "group_by" not in selector_payload and "group_by" in normalized:
            selector_payload["group_by"] = normalized["group_by"]
        if "limit" not in selector_payload and "limit" in normalized:
            selector_payload["limit"] = normalized["limit"]
        normalized["selector"] = selector_payload
        normalized.pop("limit", None)

    if normalized.get("kind") == "detail":
        selector = selector_payload if isinstance(selector_payload, dict) else {}
        if "group_by" not in selector and "group_by" in normalized:
            selector["group_by"] = normalized["group_by"]
        if "limit" not in selector and "selector_count" in normalized:
            selector["limit"] = normalized["selector_count"]
        if "limit" not in selector and "top" in normalized:
            selector["limit"] = normalized["top"]
        if selector:
            selector.pop("selector_type", None)
            normalized["selector"] = selector
        normalized.pop("group_by", None)
        normalized.pop("selector_count", None)
        normalized.pop("top", None)
        normalized.pop("selector_type", None)

    if normalized.get("kind") == "ranking":
        normalized.pop("projection", None)
        normalized.pop("selector", None)
        normalized.pop("selector_count", None)
        normalized.pop("top", None)
        normalized.pop("selector_type", None)

    if "metric" in normalized:
        normalized["metric"] = normalize_draft_metric_payload(normalized["metric"])
    if isinstance(normalized.get("selector"), dict) and "metric" in normalized["selector"]:
        normalized["selector"]["metric"] = normalize_draft_metric_payload(normalized["selector"]["metric"])
    if isinstance(normalized.get("sort"), dict) and "operator" in normalized["sort"]:
        normalized["sort"]["operator"] = normalize_request_operator(normalized["sort"]["operator"])
    return normalized


def normalize_draft_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """把常见 public request 简写归一到 canonical DraftRequest 形状。"""
    normalized = deepcopy(payload)
    warnings: list[str] = []
    looks_like_draft = any(
        key in normalized for key in {"source", "time", "result", "field_filters", "semantic_filters", "semantic_macros"}
    )

    if isinstance(normalized.get("request_version"), str):
        token = normalized["request_version"].strip().lower().removeprefix("v")
        if token == str(DRAFT_REQUEST_VERSION):
            normalized["request_version"] = DRAFT_REQUEST_VERSION

    if isinstance(normalized.get("schema_version"), str):
        token = normalized["schema_version"].strip().lower().removeprefix("v")
        if token == str(REQUEST_SCHEMA_VERSION):
            normalized["schema_version"] = REQUEST_SCHEMA_VERSION

    if looks_like_draft and "request_version" not in normalized and "schema_version" not in normalized:
        normalized["request_version"] = DRAFT_REQUEST_VERSION
        warnings.append("公共入口已自动补全 `request_version=1`。")

    if "time" in normalized:
        original_time = deepcopy(normalized["time"])
        normalized["time"] = normalize_draft_time_payload(normalized["time"])
        if normalized["time"] != original_time:
            warnings.append("公共入口已把兼容 `time` 写法归一为 canonical 结构。")

    field_filters = normalized.get("field_filters")
    if isinstance(field_filters, list):
        normalized_filters = []
        field_filters_changed = False
        for item in field_filters:
            if not isinstance(item, dict):
                normalized_filters.append(item)
                continue
            entry = deepcopy(item)
            normalized_operator = normalize_request_operator(entry.get("operator", "=="))
            if normalized_operator != entry.get("operator", "=="):
                field_filters_changed = True
            entry["operator"] = normalized_operator
            normalized_filters.append(entry)
        normalized["field_filters"] = normalized_filters
        if field_filters_changed:
            warnings.append("公共入口已把兼容 `field_filters` 运算符写法归一为 canonical 形式。")

    semantic_filters = normalized.get("semantic_filters")
    if isinstance(semantic_filters, list):
        normalized_filters = []
        semantic_filters_changed = False
        for item in semantic_filters:
            if not isinstance(item, dict):
                normalized_filters.append(item)
                continue
            entry = deepcopy(item)
            if "target" not in entry and "target_id" in entry:
                entry["target"] = entry.pop("target_id")
                semantic_filters_changed = True
            if isinstance(entry.get("target"), str):
                resolved_target = resolve_public_target_ref(entry["target"])
                if resolved_target != entry["target"]:
                    semantic_filters_changed = True
                entry["target"] = resolved_target
            normalized_operator = normalize_request_operator(entry.get("operator", "=="))
            if normalized_operator != entry.get("operator", "=="):
                semantic_filters_changed = True
            entry["operator"] = normalized_operator
            normalized_filters.append(entry)
        normalized["semantic_filters"] = normalized_filters
        if semantic_filters_changed:
            warnings.append("公共入口已把兼容 `semantic_filters` 写法归一为 canonical 结构。")

    semantic_macros = normalized.get("semantic_macros")
    if isinstance(semantic_macros, list):
        normalized_macros = [resolve_public_macro_ref(item) if isinstance(item, str) else item for item in semantic_macros]
        normalized["semantic_macros"] = normalized_macros
        if normalized_macros != semantic_macros:
            warnings.append("公共入口已把兼容 `semantic_macros` 写法归一为 canonical `rule_id`。")

    if isinstance(normalized.get("result"), dict):
        original_result = deepcopy(normalized["result"])
        normalized["result"] = normalize_draft_result_payload(normalized["result"])
        if normalized["result"] != original_result:
            warnings.append("公共入口已把兼容 `result` 写法归一为 canonical 结构。")
        original_kind = normalize_contract_token(original_result.get("kind", "")) if isinstance(original_result.get("kind"), str) else ""
        has_original_projection = isinstance(original_result.get("projection"), list)
        has_original_return_fields = isinstance(original_result.get("return_fields"), list)
        if (
            isinstance(normalized["result"], dict)
            and normalized["result"].get("kind") == "detail"
            and isinstance(normalized["result"].get("selector"), dict)
            and not has_original_projection
            and not has_original_return_fields
            and original_kind in {"detailselector", "detail_selector", "detail", "ranking"}
        ):
            raise RequestParseError(
                "ambiguous_detail_selector_projection",
                "`detail + selector` 需要明确最终返回什么字段；当前请求没有提供 `projection` 或 `return_fields`。",
                unsupported_spans=["request.result"],
                suggestions=[
                    "如果你只想返回攻击地址，请填写 `projection:[\"攻击地址\"]` 或 `return_fields:[\"攻击地址\"]`。",
                    "如果你想返回原始记录，请显式填写 `projection:[]`。",
                ],
            )

    return normalized, warnings


class TimeRangeModel(StrictRequestModel):
    """结构化请求中的时间范围。"""

    from_: Optional[str] = Field(default=None, alias="from")
    to: Optional[str] = None


class FieldConstraintModel(StrictRequestModel):
    """字段约束的外部请求模型。"""

    field: str = ""
    field_phrase: str = ""
    operator: str = "=="
    value: Any = None

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, value: str) -> str:
        if value not in REQUEST_OPERATORS:
            allowed = "、".join(sorted(REQUEST_OPERATORS))
            raise ValueError(f"`operator` 只能是：{allowed}。")
        return value

    @model_validator(mode="after")
    def validate_shape(self) -> "FieldConstraintModel":
        if bool(self.field) == bool(self.field_phrase):
            raise ValueError("必须且只能提供 `field` 或 `field_phrase` 其中之一。")
        return self


class SemanticConstraintModel(StrictRequestModel):
    """语义目标约束的外部请求模型。"""

    target_id: str
    operator: Literal["==", "!="] = "=="
    value: Any = None


class MetricModel(StrictRequestModel):
    """聚合指标的外部请求模型。"""

    function: str = ""
    field: str = ""
    alias: str = ""


class SelectorModel(StrictRequestModel):
    """detail + selector 的外部请求模型。"""

    group_by_phrase: str
    metric: MetricModel = Field(default_factory=MetricModel)
    direction: Literal["asc", "desc"] = "desc"
    limit: int

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("`limit` 必须是正整数。")
        return value


class FinalResultModel(StrictRequestModel):
    """最终结果定义的外部请求模型。"""

    kind: Literal["detail", "group_count", "ranking"]
    field_phrases: list[str] = Field(default_factory=list)
    group_by_phrase: str = ""
    metric: MetricModel = Field(default_factory=MetricModel)
    selector: Optional[SelectorModel] = None

    @field_validator("field_phrases")
    @classmethod
    def validate_field_phrases(cls, values: list[str]) -> list[str]:
        cleaned = [item.strip() for item in values]
        if any(not item for item in cleaned):
            raise ValueError("`field_phrases` 里的每一项都必须是非空字符串。")
        if "*" in cleaned:
            raise ValueError("如果你想返回原始记录，请把 `field_phrases` 设为空数组，不要使用 `\"*\"`。")
        return cleaned

    @model_validator(mode="after")
    def validate_shape(self) -> "FinalResultModel":
        if self.kind == "detail":
            if self.group_by_phrase and self.selector is None:
                raise ValueError("`detail` 场景不允许顶层 `group_by_phrase`；如果你想先选 top-k，请改用 `selector.group_by_phrase`。")
            if self.group_by_phrase and self.selector is not None:
                raise ValueError("`detail` 且使用 `selector` 时，不应再填写顶层 `group_by_phrase`。")
            return self
        if self.selector is not None:
            raise ValueError("只有 `final_result.kind=detail` 时才允许使用 `selector`。")
        if not self.group_by_phrase:
            raise ValueError(f"`final_result.kind={self.kind}` 时，`group_by_phrase` 不能为空。")
        return self


class DetailLimitModel(StrictRequestModel):
    """明细 limit 策略的外部请求模型。"""

    mode: Literal["default", "explicit", "unbounded"] = "default"
    value: Optional[int] = None

    @model_validator(mode="after")
    def validate_shape(self) -> "DetailLimitModel":
        if self.mode == "explicit":
            if self.value is None or self.value <= 0:
                raise ValueError("`detail_limit.mode=explicit` 时，`value` 必须是正整数。")
            return self
        if self.value is not None:
            raise ValueError("`detail_limit.mode` 不是 explicit 时，`value` 必须为 null。")
        return self


class SortModel(StrictRequestModel):
    """排序的外部请求模型。"""

    field: str
    direction: Literal["asc", "desc"] = "desc"


class ParsedRequestModel(StrictRequestModel):
    """`当前 ParsedRequest` 的 Pydantic 外部模型。"""

    schema_version: Literal[REQUEST_SCHEMA_VERSION]
    source: str
    time_range: TimeRangeModel = Field(default_factory=TimeRangeModel)
    semantic_macro_ids: list[str] = Field(default_factory=list)
    semantic_constraints: list[SemanticConstraintModel] = Field(default_factory=list)
    field_constraints: list[FieldConstraintModel] = Field(default_factory=list)
    final_result: FinalResultModel
    show_intermediate: bool
    detail_limit: DetailLimitModel
    sort: Optional[SortModel] = None
    raw_query: str = ""

    @field_validator("semantic_macro_ids")
    @classmethod
    def validate_macro_ids(cls, values: list[str]) -> list[str]:
        cleaned = [item.strip() for item in values]
        if any(not item for item in cleaned):
            raise ValueError("`semantic_macro_ids` 里的每一项都必须是非空字符串。")
        return cleaned

    @field_validator("source")
    @classmethod
    def validate_source(cls, value: str) -> str:
        try:
            return resolve_source_alias(value)
        except RequestParseError as exc:
            raise ValueError(exc.message) from exc

    @model_validator(mode="after")
    def validate_cross_fields(self) -> "ParsedRequestModel":
        if self.show_intermediate and not (
            self.final_result.kind == "detail" and self.final_result.selector is not None
        ):
            raise ValueError("`show_intermediate=true` 目前只支持 `detail + selector` 场景。")
        return self


def validation_error_to_request_error(exc: ValidationError) -> RequestParseError:
    """把 Pydantic ValidationError 转成现有的 RequestParseError。"""
    first = exc.errors()[0]
    loc_parts = [str(item) for item in first.get("loc", ())]
    path = ".".join(["request", *loc_parts]) if loc_parts else "request"
    message = first.get("msg", "请求格式不合法。")
    return RequestParseError(
        "invalid_request",
        f"`{path}` {message}",
        unsupported_spans=[path],
    )


@dataclass
class FieldConstraintSpec:
    """表示外部请求里的显式字段约束。"""

    field: str = ""
    field_phrase: str = ""
    operator: str = "=="
    value: Any = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any], *, path: str) -> "FieldConstraintSpec":
        """从 JSON 对象解析字段约束。"""
        reject_unknown_keys(payload, {"field", "field_phrase", "operator", "value"}, path)
        field = ensure_str(payload.get("field", ""), f"{path}.field")
        field_phrase = ensure_str(payload.get("field_phrase", ""), f"{path}.field_phrase")
        operator = ensure_str(payload.get("operator", ""), f"{path}.operator", allow_empty=False)
        if operator not in REQUEST_OPERATORS:
            allowed = "、".join(sorted(REQUEST_OPERATORS))
            raise request_error(f"{path}.operator", f"`{path}.operator` 只能是：{allowed}。", code="invalid_enum")
        if bool(field.strip()) == bool(field_phrase.strip()):
            raise request_error(
                path,
                f"`{path}` 必须且只能提供 `field` 或 `field_phrase` 其中之一。",
                code="invalid_constraint_shape",
            )
        return cls(field=field.strip(), field_phrase=field_phrase.strip(), operator=operator, value=payload.get("value"))


@dataclass
class SemanticConstraintSpec:
    """表示业务语义目标上的取值约束。"""

    target_id: str
    operator: str = "=="
    value: Any = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any], *, path: str) -> "SemanticConstraintSpec":
        """从 JSON 对象解析语义目标约束。"""
        reject_unknown_keys(payload, {"target_id", "operator", "value"}, path)
        target_id = ensure_str(payload.get("target_id", ""), f"{path}.target_id", allow_empty=False).strip()
        operator = ensure_str(payload.get("operator", ""), f"{path}.operator", allow_empty=False)
        if operator not in SEMANTIC_CONSTRAINT_OPERATORS:
            allowed = "、".join(sorted(SEMANTIC_CONSTRAINT_OPERATORS))
            raise request_error(f"{path}.operator", f"`{path}.operator` 只能是：{allowed}。", code="invalid_enum")
        return cls(target_id=target_id, operator=operator, value=payload.get("value"))


@dataclass
class MetricSpec:
    """表示最终结果中的聚合指标定义。"""

    function: str = ""
    field: str = ""
    alias: str = ""

    @classmethod
    def from_dict(cls, payload: dict[str, Any], *, path: str) -> "MetricSpec":
        """从 JSON 对象解析聚合指标。"""
        reject_unknown_keys(payload, {"function", "field", "alias"}, path)
        return cls(
            function=ensure_str(payload.get("function", ""), f"{path}.function").strip(),
            field=ensure_str(payload.get("field", ""), f"{path}.field").strip(),
            alias=ensure_str(payload.get("alias", ""), f"{path}.alias").strip(),
        )


@dataclass
class SelectorSpec:
    """表示“先按某个维度求 top-k，再返回明细”的选择器。"""

    group_by_phrase: str
    metric: MetricSpec = dc_field(default_factory=MetricSpec)
    direction: str = "desc"
    limit: int = 1

    @classmethod
    def from_dict(cls, payload: dict[str, Any], *, path: str) -> "SelectorSpec":
        """从 JSON 对象解析 selector。"""
        reject_unknown_keys(payload, {"group_by_phrase", "metric", "direction", "limit"}, path)
        group_by_phrase = ensure_str(payload.get("group_by_phrase", ""), f"{path}.group_by_phrase", allow_empty=False).strip()
        metric_payload = ensure_dict(payload.get("metric", {}), f"{path}.metric")
        metric = MetricSpec.from_dict(metric_payload, path=f"{path}.metric")
        direction = ensure_str(payload.get("direction", "desc"), f"{path}.direction", allow_empty=False).lower()
        if direction not in SORT_DIRECTIONS:
            allowed = "、".join(sorted(SORT_DIRECTIONS))
            raise request_error(f"{path}.direction", f"`{path}.direction` 只能是：{allowed}。", code="invalid_enum")
        limit = ensure_int(payload.get("limit"), f"{path}.limit")
        if limit <= 0:
            raise request_error(f"{path}.limit", f"`{path}.limit` 必须是正整数。", code="invalid_limit")
        return cls(group_by_phrase=group_by_phrase, metric=metric, direction=direction, limit=limit)


@dataclass
class FinalResultSpec:
    """表示用户真正想看到的最终结果形态。"""

    kind: str
    field_phrases: list[str] = dc_field(default_factory=list)
    group_by_phrase: str = ""
    metric: MetricSpec = dc_field(default_factory=MetricSpec)
    selector: SelectorSpec | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any], *, path: str) -> "FinalResultSpec":
        """从 JSON 对象解析最终结果定义。"""
        reject_unknown_keys(payload, {"kind", "field_phrases", "group_by_phrase", "metric", "selector"}, path)
        kind = ensure_str(payload.get("kind", ""), f"{path}.kind", allow_empty=False)
        if kind not in FINAL_RESULT_KINDS:
            allowed = "、".join(sorted(FINAL_RESULT_KINDS))
            raise request_error(f"{path}.kind", f"`{path}.kind` 只能是：{allowed}。", code="invalid_enum")

        field_phrases_raw = ensure_list(payload.get("field_phrases", []), f"{path}.field_phrases")
        field_phrases = [ensure_str(item, f"{path}.field_phrases[{index}]", allow_empty=False).strip() for index, item in enumerate(field_phrases_raw)]
        group_by_phrase = ensure_str(payload.get("group_by_phrase", ""), f"{path}.group_by_phrase").strip()
        metric_payload = ensure_dict(payload.get("metric", {}), f"{path}.metric")
        metric = MetricSpec.from_dict(metric_payload, path=f"{path}.metric")
        selector_payload = parse_optional_object(payload, "selector", path)
        selector = SelectorSpec.from_dict(selector_payload, path=f"{path}.selector") if selector_payload else None

        if "*" in field_phrases:
            raise request_error(
                f"{path}.field_phrases",
                "如果你想返回原始记录，请把 `field_phrases` 设为空数组，不要使用 `\"*\"`。",
                code="invalid_result_shape",
            )
        if kind == "detail" and group_by_phrase and selector is None:
            raise request_error(
                path,
                "`final_result.kind=detail` 时，顶层 `group_by_phrase` 必须为空；如果你想先选 top-k 再返回明细，请改用 `selector.group_by_phrase`。",
                code="invalid_result_shape",
            )
        if kind == "group_count" and not group_by_phrase:
            raise request_error(path, "`final_result.kind=group_count` 时，`group_by_phrase` 不能为空。", code="invalid_result_shape")
        if kind == "detail" and selector and group_by_phrase:
            raise request_error(
                path,
                "`final_result.kind=detail` 且使用 `selector` 时，不应再填写顶层 `group_by_phrase`。",
                code="invalid_result_shape",
            )
        if kind == "ranking" and not group_by_phrase:
            raise request_error(path, "`final_result.kind=ranking` 时，`group_by_phrase` 不能为空。", code="invalid_result_shape")
        if kind != "detail" and selector is not None:
            raise request_error(path, "只有 `final_result.kind=detail` 时才允许使用 `selector`。", code="invalid_result_shape")
        return cls(kind=kind, field_phrases=field_phrases, group_by_phrase=group_by_phrase, metric=metric, selector=selector)


@dataclass
class DetailLimitSpec:
    """表示明细查询的限制策略。"""

    mode: str = "default"
    value: int | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any], *, path: str) -> "DetailLimitSpec":
        """从 JSON 对象解析明细 limit 策略。"""
        reject_unknown_keys(payload, {"mode", "value"}, path)
        mode = ensure_str(payload.get("mode", ""), f"{path}.mode", allow_empty=False)
        if mode not in DETAIL_LIMIT_MODES:
            allowed = "、".join(sorted(DETAIL_LIMIT_MODES))
            raise request_error(f"{path}.mode", f"`{path}.mode` 只能是：{allowed}。", code="invalid_enum")
        value = payload.get("value")
        if mode == "explicit":
            parsed = ensure_int(value, f"{path}.value")
            if parsed <= 0:
                raise request_error(f"{path}.value", "`detail_limit.value` 必须是正整数。", code="invalid_limit")
            return cls(mode=mode, value=parsed)
        if value is not None:
            raise request_error(f"{path}.value", "`detail_limit.mode` 不是 explicit 时，`value` 必须为 null。", code="invalid_limit")
        return cls(mode=mode, value=None)


@dataclass
class SortSpec:
    """表示一个排序目标。"""

    field: str
    direction: str = "desc"

    @classmethod
    def from_dict(cls, payload: dict[str, Any], *, path: str) -> "SortSpec":
        """从 JSON 对象解析排序定义。"""
        reject_unknown_keys(payload, {"field", "direction"}, path)
        field = ensure_str(payload.get("field", ""), f"{path}.field", allow_empty=False).strip()
        direction = ensure_str(payload.get("direction", ""), f"{path}.direction", allow_empty=False).lower()
        if direction not in SORT_DIRECTIONS:
            allowed = "、".join(sorted(SORT_DIRECTIONS))
            raise request_error(f"{path}.direction", f"`{path}.direction` 只能是：{allowed}。", code="invalid_enum")
        return cls(field=field, direction=direction)


@dataclass
class ParsedRequest:
    """表示 `当前 ParsedRequest` 外部输入。"""

    schema_version: int
    source: str
    time_range: dict[str, str] = dc_field(default_factory=dict)
    semantic_macro_ids: list[str] = dc_field(default_factory=list)
    semantic_constraints: list[SemanticConstraintSpec] = dc_field(default_factory=list)
    field_constraints: list[FieldConstraintSpec] = dc_field(default_factory=list)
    final_result: FinalResultSpec = dc_field(default_factory=lambda: FinalResultSpec(kind="detail"))
    show_intermediate: bool = False
    detail_limit: DetailLimitSpec = dc_field(default_factory=DetailLimitSpec)
    sort: SortSpec | None = None
    raw_query: str = ""

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ParsedRequest":
        """从 JSON 对象解析 `当前 ParsedRequest`。"""
        path = "request"
        allowed_top_level_keys = {
            "schema_version",
            "source",
            "time_range",
            "semantic_macro_ids",
            "semantic_constraints",
            "field_constraints",
            "final_result",
            "show_intermediate",
            "detail_limit",
            "sort",
            "raw_query",
        }
        unknown_top_level_keys = sorted(set(payload) - allowed_top_level_keys)
        if unknown_top_level_keys:
            nearest_valid_keys = {
                key: get_close_matches(key, sorted(allowed_top_level_keys), n=3, cutoff=0.5) for key in unknown_top_level_keys
            }
            suggested_shape = "请先套用三种 canonical skeleton 之一：detail、group_count、detail+selector。"
            example_request = example_draft_group_count_request()
            if {"aggregation", "output_mode"} & set(unknown_top_level_keys):
                suggested_shape = "你看起来想表达聚合统计；请改用 `final_result.kind=group_count`，不要使用 `aggregation` 或 `output_mode`。"
            raise RequestParseError(
                "unknown_keys",
                f"`{path}` 包含未支持字段：{'、'.join(unknown_top_level_keys)}。",
                unsupported_spans=[path],
                suggestions=[
                    "先确定要用 `detail`、`group_count` 还是 `detail + selector` 骨架。",
                    "不要自创 `aggregation`、`output_mode` 一类字段。",
                ],
                unknown_keys=unknown_top_level_keys,
                nearest_valid_keys=nearest_valid_keys,
                suggested_shape=suggested_shape,
                example_request=example_request,
            )
        try:
            model = ParsedRequestModel.model_validate(payload)
        except ValidationError as exc:
            raise validation_error_to_request_error(exc) from exc

        return cls(
            schema_version=model.schema_version,
            source=model.source,
            time_range={key: value for key, value in {"from": model.time_range.from_, "to": model.time_range.to}.items() if value},
            semantic_macro_ids=list(model.semantic_macro_ids),
            semantic_constraints=[
                SemanticConstraintSpec(target_id=item.target_id, operator=item.operator, value=item.value)
                for item in model.semantic_constraints
            ],
            field_constraints=[
                FieldConstraintSpec(field=item.field, field_phrase=item.field_phrase, operator=item.operator, value=item.value)
                for item in model.field_constraints
            ],
            final_result=FinalResultSpec(
                kind=model.final_result.kind,
                field_phrases=list(model.final_result.field_phrases),
                group_by_phrase=model.final_result.group_by_phrase,
                metric=MetricSpec(
                    function=model.final_result.metric.function,
                    field=model.final_result.metric.field,
                    alias=model.final_result.metric.alias,
                ),
                selector=(
                    SelectorSpec(
                        group_by_phrase=model.final_result.selector.group_by_phrase,
                        metric=MetricSpec(
                            function=model.final_result.selector.metric.function,
                            field=model.final_result.selector.metric.field,
                            alias=model.final_result.selector.metric.alias,
                        ),
                        direction=model.final_result.selector.direction,
                        limit=model.final_result.selector.limit,
                    )
                    if model.final_result.selector
                    else None
                ),
            ),
            show_intermediate=model.show_intermediate,
            detail_limit=DetailLimitSpec(mode=model.detail_limit.mode, value=model.detail_limit.value),
            sort=SortSpec(field=model.sort.field, direction=model.sort.direction) if model.sort else None,
            raw_query=model.raw_query,
        )

    def to_dict(self) -> dict[str, Any]:
        """导出成 JSON 可序列化的字典。"""
        return asdict(self)


@dataclass
class LoadedRequest:
    """表示主入口加载到的请求及其附带告警。"""

    request: ParsedRequest
    warnings: list[str] = dc_field(default_factory=list)
    contract: str = "draft_v1"


def normalize_time_range(time: DraftTimeModel) -> dict[str, str]:
    """把 DraftRequest 的 typed time lower 成 compiler-facing `from/to`。"""
    if isinstance(time, DraftPresetTimeModel):
        now = datetime.now().astimezone()
        if time.preset == "today":
            return {"from": "now(d)", "to": "now()"}
        if time.preset == "yesterday":
            return {"from": "now(d-1d)", "to": "now(d)"}
        if time.preset == "this_week":
            offset = now.weekday()
            start = "now(d)" if offset == 0 else f"now(d-{offset}d)"
            return {"from": start, "to": "now()"}
        if time.preset == "this_month":
            offset = now.day - 1
            start = "now(d)" if offset == 0 else f"now(d-{offset}d)"
            return {"from": start, "to": "now()"}
        if time.preset == "this_year":
            offset = int(now.strftime("%j")) - 1
            start = "now(d)" if offset == 0 else f"now(d-{offset}d)"
            return {"from": start, "to": "now()"}
    if isinstance(time, DraftRelativeTimeModel):
        now = datetime.now().astimezone()
        if time.unit == "day":
            return {"from": f"now(d-{time.value}d)", "to": "now()"}
        if time.unit == "hour":
            return {"from": f"now(h-{time.value}h)", "to": "now()"}
        if time.unit == "week":
            return {"from": f"now(d-{time.value * 7}d)", "to": "now()"}
        if time.unit == "minute":
            start = (now - timedelta(minutes=time.value)).strftime("%Y-%m-%d %H:%M:%S")
            return {"from": start, "to": "now()"}
        if time.unit == "month":
            target_year = now.year
            target_month_index = now.month - time.value
            while target_month_index <= 0:
                target_month_index += 12
                target_year -= 1
            day = min(now.day, calendar.monthrange(target_year, target_month_index)[1])
            start = now.replace(year=target_year, month=target_month_index, day=day).strftime("%Y-%m-%d %H:%M:%S")
            return {"from": start, "to": "now()"}
        if time.unit == "year":
            target_year = now.year - time.value
            day = min(now.day, calendar.monthrange(target_year, now.month)[1])
            start = now.replace(year=target_year, day=day).strftime("%Y-%m-%d %H:%M:%S")
            return {"from": start, "to": "now()"}
    if isinstance(time, DraftBetweenTimeModel):
        return {"from": time.from_, "to": time.to}
    raise RequestParseError("invalid_request", "不支持的 `time.kind`。", unsupported_spans=["request.time.kind"])


def resolve_metric_model(metric: DraftMetricModel | MetricModel, *, default_alias: str) -> MetricSpec:
    """把 DraftRequestV1 的 metric 模型归一成内部 `MetricSpec`。"""
    function = str(metric.function or "count")
    field = str(metric.field or "ID")
    if field == "*":
        field = "ID"
    alias = str(metric.alias or default_alias)
    return MetricSpec(function=function, field=field, alias=alias)


def lower_draft_request(model: DraftRequestModel) -> ParsedRequest:
    """把 DraftRequestV1 lower 成 compiler-facing `当前 ParsedRequest`。"""
    source = resolve_source_alias(model.source)
    time_range = normalize_time_range(model.time) if model.time is not None else {}
    show_intermediate = bool(model.show_intermediate)
    field_constraints = [
        FieldConstraintSpec(field="", field_phrase=item.field, operator=item.operator, value=item.value)
        for item in model.field_filters
    ]
    semantic_constraints = [
        SemanticConstraintSpec(target_id=item.target, operator=item.operator, value=item.value)
        for item in model.semantic_filters
    ]

    result = model.result
    sort: SortSpec | None = None
    detail_limit = DetailLimitSpec()

    if isinstance(result, DraftDetailResultModel):
        selector = None
        if result.selector is not None:
            selector = SelectorSpec(
                group_by_phrase=result.selector.group_by,
                metric=resolve_metric_model(result.selector.metric, default_alias="数量"),
                direction=result.selector.direction,
                limit=result.selector.limit,
            )
        if show_intermediate and selector is None:
            raise RequestParseError(
                "invalid_request",
                "`show_intermediate=true` 目前只支持 `result.kind=detail` 且显式提供 `selector` 的场景。",
                unsupported_spans=["request.show_intermediate"],
            )
        if result.limit == "unbounded":
            detail_limit = DetailLimitSpec(mode="unbounded", value=None)
        elif isinstance(result.limit, int):
            detail_limit = DetailLimitSpec(mode="explicit", value=result.limit)
        if result.sort is not None:
            sort = SortSpec(field=result.sort.field, direction=result.sort.direction)
        final_result = FinalResultSpec(
            kind="detail",
            field_phrases=list(result.projection),
            group_by_phrase="",
            metric=MetricSpec(),
            selector=selector,
        )
    elif isinstance(result, DraftGroupCountResultModel):
        if show_intermediate:
            raise RequestParseError(
                "invalid_request",
                "`show_intermediate=true` 目前只支持 `detail + selector` 场景。",
                unsupported_spans=["request.show_intermediate"],
            )
        metric = resolve_metric_model(result.metric, default_alias="数量")
        sort = SortSpec(field=metric.alias, direction=result.direction)
        final_result = FinalResultSpec(
            kind="group_count",
            field_phrases=[],
            group_by_phrase=result.group_by,
            metric=metric,
            selector=None,
        )
    elif isinstance(result, DraftRankingResultModel):
        if show_intermediate:
            raise RequestParseError(
                "invalid_request",
                "`show_intermediate=true` 目前只支持 `detail + selector` 场景。",
                unsupported_spans=["request.show_intermediate"],
            )
        detail_limit = DetailLimitSpec(mode="explicit", value=result.limit)
        final_result = FinalResultSpec(
            kind="ranking",
            field_phrases=[],
            group_by_phrase=result.group_by,
            metric=MetricSpec(),
            selector=None,
        )
    else:
        raise RequestParseError("invalid_request", "不支持的 `result.kind`。", unsupported_spans=["request.result.kind"])

    return ParsedRequest(
        schema_version=REQUEST_SCHEMA_VERSION,
        source=source,
        time_range=time_range,
        semantic_macro_ids=list(model.semantic_macros),
        semantic_constraints=semantic_constraints,
        field_constraints=field_constraints,
        final_result=final_result,
        show_intermediate=show_intermediate,
        detail_limit=detail_limit,
        sort=sort,
        raw_query=model.raw_query,
    )


def missing_version_error(payload: dict[str, Any]) -> RequestParseError:
    """在缺少 version 字段时给出更可操作的建议。"""
    looks_like_draft = any(key in payload for key in {"source", "time", "result", "field_filters", "semantic_filters", "semantic_macros"})
    message = "请求缺少版本字段。公共请求请填写 `request_version=1`；compiler-facing internal request 使用 `schema_version=1`。"
    suggested_shape = "推荐先使用 `main.py skeleton detail|aggregate|aggregate-topk|detail-topk` 输出当前 v1 skeleton。"
    example_request = example_draft_detail_request()
    if looks_like_draft:
        return RequestParseError(
            "missing_request_version",
            message,
            unsupported_spans=["request"],
            suggestions=["如果你在写模型侧请求，请添加 `request_version=1`。", "不要优先手写 compiler-facing 的 `schema_version=1` internal request。"],
            suggested_shape=suggested_shape,
            example_request=example_request,
        )
    return RequestParseError(
        "missing_request_version",
        message,
        unsupported_spans=["request"],
        suggestions=["请先运行 `python3 scripts/main.py skeleton detail` 获取最小 skeleton。"],
        suggested_shape=suggested_shape,
        example_request=example_request,
    )


def parse_request_json(text: str) -> LoadedRequest:
    """把内联 JSON 文本解析成 DraftRequestV1 或 internal request。"""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RequestParseError(
            "invalid_json",
            "请求 JSON 解析失败。",
            unsupported_spans=[f"line {exc.lineno} column {exc.colno}"],
            suggestions=["请确认 `--request` 传入的是合法 JSON。"],
        ) from exc
    if not isinstance(payload, dict):
        raise request_error("request", "顶层请求必须是对象。")
    if isinstance(payload.get("request_version"), str):
        token = payload["request_version"].strip().lower().removeprefix("v")
        if token == str(DRAFT_REQUEST_VERSION):
            payload["request_version"] = DRAFT_REQUEST_VERSION
    if isinstance(payload.get("schema_version"), str):
        token = payload["schema_version"].strip().lower().removeprefix("v")
        if token == str(REQUEST_SCHEMA_VERSION):
            payload["schema_version"] = REQUEST_SCHEMA_VERSION
    if "request_version" in payload:
        normalized_payload, warnings = normalize_draft_payload(payload)
        try:
            model = DraftRequestModel.model_validate(normalized_payload)
        except ValidationError as exc:
            raise validation_error_to_request_error(exc) from exc
        return LoadedRequest(request=lower_draft_request(model), warnings=warnings, contract="draft_v1")
    if "schema_version" in payload:
        return LoadedRequest(
            request=ParsedRequest.from_dict(payload),
            warnings=[],
            contract="internal_v1",
        )
    normalized_payload, warnings = normalize_draft_payload(payload)
    if "request_version" in normalized_payload:
        try:
            model = DraftRequestModel.model_validate(normalized_payload)
        except ValidationError as exc:
            raise validation_error_to_request_error(exc) from exc
        return LoadedRequest(request=lower_draft_request(model), warnings=warnings, contract="draft_v1")
    raise missing_version_error(payload)


def parse_request_file(path: str | Path) -> LoadedRequest:
    """从文件中读取并解析 DraftRequestV1 或 internal request。"""
    request_path = Path(path)
    if not request_path.exists():
        raise RequestParseError(
            "request_file_not_found",
            f"请求文件不存在：{request_path}",
            unsupported_spans=[str(request_path)],
        )
    return parse_request_json(request_path.read_text(encoding="utf-8"))


@dataclass
class BoundPredicateAtom:
    """表示已经完成绑定的原子约束。"""

    field: str
    field_key: str = ""
    operator: str = "=="
    value: Any = None
    negated: bool = False
    semantic_tag: str = ""
    origin: str = "unresolved"
    authoritative: bool = False
    source_macro_id: str = ""
    source_target_id: str = ""
    phrase: str = ""
    raw_text: str = ""


@dataclass
class BoundPredicateGroup:
    """表示一组已经完成绑定的原子约束。"""

    mode: str
    predicates: list[BoundPredicateAtom] = dc_field(default_factory=list)
    semantic_tag: str = ""
    origin: str = "semantic_macro"
    authoritative: bool = False
    source_macro_id: str = ""
    phrase: str = ""
    raw_text: str = ""


BoundPredicate = Union[BoundPredicateAtom, BoundPredicateGroup]


@dataclass
class ResultSpec:
    """表示最终结果集中的一个输出列。"""

    field: str
    role: str = "field"
    aggregate_function: str = ""
    source_field: str = ""


@dataclass
class DerivationSpec:
    """描述最终结果生成前需要经历的中间推导步骤。"""

    kind: str
    field: str = ""
    aggregate_function: str = ""
    source_field: str = ""
    limit: int | None = None
    expose: bool = False
    details: dict[str, Any] = dc_field(default_factory=dict)


@dataclass
class CompiledIntent:
    """表示已经完成绑定、校验和结果形态推断的内部编译结果。"""

    source: str
    time_scope: dict[str, Any] = dc_field(default_factory=dict)
    bound_predicates: list[BoundPredicate] = dc_field(default_factory=list)
    final_outputs: list[ResultSpec] = dc_field(default_factory=list)
    derivations: list[DerivationSpec] = dc_field(default_factory=list)
    result_policy: str = "single_result_preferred"
    result_fallback: str = "final_only"
    can_embed_metric: bool = True
    sorts: list[SortSpec] = dc_field(default_factory=list)
    limit: int | None = None
    raw_query: str = ""
    intent_tags: list[str] = dc_field(default_factory=list)

    def authoritative_predicates(self) -> list[BoundPredicate]:
        """返回所有 authoritative 语义约束。"""
        return [item for item in self.bound_predicates if item.authoritative]

    def final_output_fields(self) -> list[str]:
        """返回最终结果集中要输出的列名。"""
        return [item.field for item in self.final_outputs]

    def matched_macro_ids(self) -> list[str]:
        """返回当前编译结果命中的宏语义 id。"""
        macro_ids: set[str] = set()
        for item in self.bound_predicates:
            if isinstance(item, BoundPredicateGroup):
                if item.source_macro_id:
                    macro_ids.add(item.source_macro_id)
                continue
            if item.source_macro_id:
                macro_ids.add(item.source_macro_id)
        return sorted(macro_ids)

    def matched_target_ids(self) -> list[str]:
        """返回当前编译结果命中的语义目标 id。"""
        return sorted(
            {
                item.source_target_id
                for item in self.bound_predicates
                if isinstance(item, BoundPredicateAtom) and item.source_target_id
            }
        )

    def to_dict(self) -> dict[str, Any]:
        """导出成 JSON 可序列化的字典。"""
        return asdict(self)


def example_draft_detail_request() -> dict[str, Any]:
    """返回 DraftRequestV1 的 detail 骨架示例。"""
    return {
        "request_version": DRAFT_REQUEST_VERSION,
        "source": "日志",
        "time": {"kind": "preset", "preset": "today"},
        "semantic_macros": [],
        "semantic_filters": [],
        "field_filters": [],
        "result": {"kind": "detail"},
        "show_intermediate": False,
        "raw_query": "原始中文问题",
    }


def example_draft_projected_detail_request() -> dict[str, Any]:
    """返回带显式字段投影的 DraftRequestV1 detail 示例。"""
    payload = example_draft_detail_request()
    payload["result"] = {"kind": "detail", "projection": ["源地址", "域名", "HTTP Cookie"]}
    return payload


def example_draft_group_count_request() -> dict[str, Any]:
    """返回 DraftRequestV1 的 group_count 骨架示例。"""
    return {
        "request_version": DRAFT_REQUEST_VERSION,
        "source": "告警",
        "time": {"kind": "relative", "unit": "day", "value": 30},
        "semantic_macros": [],
        "semantic_filters": [],
        "field_filters": [],
        "result": {"kind": "group_count", "group_by": "攻击地址"},
        "show_intermediate": False,
        "raw_query": "原始中文问题",
    }


def example_draft_detail_selector_request() -> dict[str, Any]:
    """返回 DraftRequestV1 的 detail + selector 骨架示例。"""
    return {
        "request_version": DRAFT_REQUEST_VERSION,
        "source": "告警",
        "time": {"kind": "relative", "unit": "day", "value": 30},
        "semantic_macros": ["manual_handled_alarm_merge"],
        "semantic_filters": [],
        "field_filters": [],
        "result": {
            "kind": "detail",
            "projection": ["攻击地址"],
            "selector": {
                "group_by": "威胁类型",
                "limit": 3,
            },
        },
        "show_intermediate": False,
        "raw_query": "原始中文问题",
    }


SKELETON_REQUESTS = {
    "detail": example_draft_detail_request,
    "detail-projection": example_draft_projected_detail_request,
    "group_count": example_draft_group_count_request,
    "detail-selector": example_draft_detail_selector_request,
}


def request_skeleton(name: str) -> dict[str, Any]:
    """按 skeleton 名称返回官方示例。"""
    if name not in SKELETON_REQUESTS:
        choices = "、".join(sorted(SKELETON_REQUESTS))
        raise RequestParseError(
            "unknown_skeleton",
            f"未知 skeleton：`{name}`。可选值：{choices}。",
            unsupported_spans=[name],
        )
    return SKELETON_REQUESTS[name]()


@dataclass
class PipelineCommand:
    """表示 HQL 管道中的一个命令段。"""

    command: str
    body: str = ""
    subqueries: list["PipelineAst"] = dc_field(default_factory=list)


@dataclass
class PipelineAst:
    """表示一条完整 HQL 管道的抽象语法树。"""

    index: str = ""
    segments: list[PipelineCommand] = dc_field(default_factory=list)
    raw_index_segment: str = ""

    def to_dict(self) -> dict[str, Any]:
        """导出成 JSON 可序列化的字典。"""
        return asdict(self)


@dataclass
class DerivationStep:
    """描述 planner 在最终查询前经历的关键推导步骤。"""

    kind: str
    field: str = ""
    limit: int | None = None
    details: dict[str, Any] = dc_field(default_factory=dict)


@dataclass
class PlanCandidate:
    """表示 planner 产出的一份候选执行计划。"""

    shape: str
    ast: list[PipelineAst]
    single_query: bool
    cost: int
    completeness: float
    fallback_reason: str = ""
    derivations: list[DerivationStep] = dc_field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """导出成 JSON 可序列化的字典。"""
        return asdict(self)


@dataclass
class ReviewReport:
    """表示 reviewer 对一条 HQL 的完整审查结果。"""

    source: str
    shape: str
    ok: bool
    canonical_issues: list[str] = dc_field(default_factory=list)
    unknown_fields: list[str] = dc_field(default_factory=list)
    unknown_commands: list[str] = dc_field(default_factory=list)
    unknown_operators: list[str] = dc_field(default_factory=list)
    unknown_functions: list[str] = dc_field(default_factory=list)
    unknown_chart_panels: list[str] = dc_field(default_factory=list)
    nested_reports: list["ReviewReport"] = dc_field(default_factory=list)
    strategy_warnings: list[str] = dc_field(default_factory=list)
    notes: list[str] = dc_field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """导出成 JSON 可序列化的字典。"""
        return asdict(self)


@dataclass
class CompiledQuery:
    """表示单一编译链输出的最终结果。"""

    request: ParsedRequest
    resolved_semantics: dict[str, list[dict[str, Any]]]
    field_bindings: dict[str, Any]
    intent: CompiledIntent
    operator_context: dict[str, Any]
    plan: PlanCandidate
    rendered_hql: str
    review: ReviewReport

    def to_dict(self) -> dict[str, Any]:
        """导出成 JSON 可序列化的字典。"""
        return {
            "request": self.request.to_dict(),
            "resolved_semantics": self.resolved_semantics,
            "field_bindings": self.field_bindings,
            "intent": self.intent.to_dict(),
            "operator_context": self.operator_context,
            "plan": self.plan.to_dict(),
            "rendered_hql": self.rendered_hql,
            "review": self.review.to_dict(),
        }
