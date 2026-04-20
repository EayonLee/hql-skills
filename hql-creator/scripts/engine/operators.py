"""算子 registry 与算子上下文选择。

这个文件把 `hql_operators.json` 视为唯一算子真相源，
并在运行时叠加少量与当前 skill 强相关的机器决策字段，
用于支持算子筛选、上下文裁剪和 reviewer 校验。
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

# OPERATORS_PATH: 算子知识库文件位置。
OPERATORS_PATH = Path(__file__).resolve().parents[2] / "references" / "hql_operators.json"

# DEFAULT_MACHINE_FIELDS: 每个算子在 skill 运行时至少要具备的机器字段默认值。
DEFAULT_MACHINE_FIELDS = {
    "intent_tags": [],
    "input_shape": "pipeline",
    "output_shape": "pipeline",
    "supports_subquery": False,
    "usable_in_where_subquery": False,
    "returns_filter_expression": False,
    "preferred_for": [],
    "avoid_when": [],
    "can_follow": [],
    "can_precede": [],
    "cost_rank": 50,
}

# COMMAND_METADATA_OVERRIDES: skill 级补丁，用于把机器决策信息与具体算子绑定。
COMMAND_METADATA_OVERRIDES = {
    "where": {
        "intent_tags": ["filter", "subquery_filter", "detail_filter"],
        "input_shape": "dataset",
        "output_shape": "dataset",
        "can_follow": ["index"],
        "can_precede": ["fields", "sort", "stats", "top", "join", "append", "head", "format"],
        "cost_rank": 5,
    },
    "fields": {
        "intent_tags": ["projection", "display_fields", "subquery_projection"],
        "input_shape": "dataset",
        "output_shape": "dataset",
        "can_follow": ["where", "top", "stats", "sort", "join", "append"],
        "cost_rank": 4,
    },
    "top": {
        "intent_tags": ["ranking", "top_n_select", "derived_filter"],
        "input_shape": "dataset",
        "output_shape": "ranking_top_n",
        "supports_subquery": True,
        "usable_in_where_subquery": True,
        "preferred_for": ["top_n", "top_n_select", "ranked_detail"],
        "can_follow": ["where", "sort"],
        "can_precede": ["fields", "format"],
        "cost_rank": 15,
    },
    "stats": {
        "intent_tags": ["aggregation", "aggregate_total", "aggregate_grouped", "aggregate_top_k"],
        "input_shape": "dataset",
        "output_shape": "aggregate_result",
        "supports_subquery": True,
        "usable_in_where_subquery": True,
        "preferred_for": ["aggregate_total", "aggregate_grouped", "aggregate_top_k", "aggregation"],
        "avoid_when": ["detail_only"],
        "can_follow": ["where", "sort"],
        "can_precede": ["sort", "fields", "chart", "append"],
        "cost_rank": 20,
    },
    "format": {
        "intent_tags": ["subquery_filter", "derived_filter", "ranked_detail"],
        "input_shape": "ranking_top_n",
        "output_shape": "filter_expression",
        "supports_subquery": True,
        "usable_in_where_subquery": True,
        "returns_filter_expression": True,
        "preferred_for": ["derived_filter", "top_n_select", "ranked_detail", "subquery_filter"],
        "avoid_when": ["explicit_multi_result", "needs_joined_result", "join_required"],
        "can_follow": ["fields", "top", "stats", "head"],
        "cost_rank": 10,
    },
    "join": {
        "intent_tags": ["join", "merge_result"],
        "input_shape": "dataset",
        "output_shape": "dataset",
        "supports_subquery": True,
        "preferred_for": ["join_result", "enrich_result"],
        "avoid_when": ["derived_filter_only"],
        "can_follow": ["where", "sort"],
        "cost_rank": 40,
    },
    "append": {
        "intent_tags": ["append", "multi_result"],
        "input_shape": "dataset",
        "output_shape": "dataset",
        "supports_subquery": True,
        "preferred_for": ["multi_result", "explicit_intermediate_output"],
        "cost_rank": 45,
    },
    "head": {
        "intent_tags": ["limit", "detail_limit"],
        "input_shape": "dataset",
        "output_shape": "dataset",
        "can_follow": ["where", "fields", "sort", "join", "append"],
        "cost_rank": 2,
    },
}


class OperatorRegistry:
    """封装算子知识库，并提供筛选与紧凑投影能力。"""

    def __init__(self, payload: dict[str, Any]):
        """基于 JSON 原始载荷初始化 registry。"""
        self.payload = payload
        # 初始化阶段就把命令级补丁和默认值合并好，后续调用直接消费统一结构。
        self.commands = {entry["name"]: self._normalize_command(entry) for entry in payload.get("commands", [])}

    def _normalize_command(self, entry: dict[str, Any]) -> dict[str, Any]:
        """为单个算子合并默认值、skill 补丁和原始定义。"""
        merged = dict(DEFAULT_MACHINE_FIELDS)
        merged.update(COMMAND_METADATA_OVERRIDES.get(entry["name"], {}))
        for key, value in entry.items():
            merged[key] = value
        for key, value in DEFAULT_MACHINE_FIELDS.items():
            merged.setdefault(key, value if not isinstance(value, list) else list(value))
        return merged

    @property
    def command_names(self) -> set[str]:
        """返回所有受支持命令名。"""
        return set(self.commands)

    @property
    def allowed_expression_operators(self) -> set[str]:
        """返回允许出现在 where/search 表达式中的运算符集合。"""
        symbolic_and_text = {
            variant
            for operator in self.payload.get("expression_operators", [])
            for variant in operator.get("variants", [operator["symbol"]])
        }
        return symbolic_and_text | {
            name
            for name, capability in self.predicate_operator_capabilities.items()
            if capability.get("render_kind", "").startswith("infix_")
        }

    @property
    def predicate_operator_capabilities(self) -> dict[str, dict[str, Any]]:
        """返回字段谓词操作能力表。"""
        capabilities = self.payload.get("predicate_operators", {})
        return {str(name): dict(config) for name, config in capabilities.items()}

    @property
    def field_filter_operators(self) -> set[str]:
        """返回公共/内部字段过滤允许使用的操作符集合。"""
        return set(self.predicate_operator_capabilities)

    @property
    def text_predicate_operators(self) -> set[str]:
        """返回在表达式里按文本运算符出现的谓词操作符。"""
        return {
            name
            for name, capability in self.predicate_operator_capabilities.items()
            if capability.get("render_kind", "").startswith("infix_") and name.isalpha()
        }

    @property
    def deprecated_predicate_functions(self) -> dict[str, str]:
        """返回不再建议生成的函数形式谓词。"""
        payload = self.payload.get("deprecated_predicate_functions", {})
        return {str(name): str(target) for name, target in payload.items()}

    def predicate_capability(self, operator_name: str) -> dict[str, Any]:
        """读取某个谓词操作符的能力配置。"""
        name = str(operator_name or "").strip()
        capability = self.predicate_operator_capabilities.get(name, {})
        return {
            "category": str(capability.get("category", "comparison")),
            "render_kind": str(capability.get("render_kind", "infix_field_value")),
            "rhs_kind": str(capability.get("rhs_kind", "literal")),
            "rhs_literal_format": str(capability.get("rhs_literal_format", "default")),
            "lhs_requires_array": bool(capability.get("lhs_requires_array", False)),
            "rhs_must_resolve_field": bool(capability.get("rhs_must_resolve_field", False)),
            "rhs_requires_belong_rhs": bool(capability.get("rhs_requires_belong_rhs", False)),
            "lhs_forbids_belong_rhs": bool(capability.get("lhs_forbids_belong_rhs", False)),
            "lhs_allowed_types": [str(item) for item in capability.get("lhs_allowed_types", [])],
            "rhs_allowed_types": [str(item) for item in capability.get("rhs_allowed_types", [])],
            "supports_scalar_shorthand": bool(capability.get("supports_scalar_shorthand", False)),
        }

    @property
    def stats_functions(self) -> set[str]:
        """返回 `stats` 等命令允许使用的聚合函数集合。"""
        return set(self.payload.get("stats_functions", []))

    @property
    def stats_function_capabilities(self) -> dict[str, dict[str, Any]]:
        """返回聚合函数能力表。"""
        capabilities = self.payload.get("stats_function_capabilities", {})
        return {str(name): dict(config) for name, config in capabilities.items()}

    def metric_capability(self, function_name: str) -> dict[str, Any]:
        """读取某个聚合函数的能力配置。"""
        name = str(function_name or "").strip()
        capability = self.stats_function_capabilities.get(name, {})
        return {
            "requires_field": bool(capability.get("requires_field", True)),
            "default_field": capability.get("default_field"),
            "allow_star": bool(capability.get("allow_star", False)),
            "default_alias": str(capability.get("default_alias", "")),
        }

    @property
    def chart_panel_types(self) -> set[str]:
        """返回允许使用的 chart panel 类型集合。"""
        return set(self.payload.get("chart_panel_types", []))

    def get(self, name: str) -> dict[str, Any]:
        """按名称读取某个完整算子定义。"""
        return self.commands[name]

    def select_context(
        self,
        *,
        intent_tags: list[str] | None = None,
        stage: str = "",
        in_subquery: bool = False,
        names: list[str] | None = None,
        limit: int = 6,
    ) -> list[dict[str, Any]]:
        """按当前规划语境挑选最相关的一小组算子卡片。"""
        requested_names = set(names or [])
        tag_set = set(intent_tags or [])
        selected: list[tuple[int, dict[str, Any]]] = []

        for card in self.commands.values():
            score = 0
            if requested_names:
                if card["name"] not in requested_names:
                    continue
                # 显式点名的算子必须优先保留。
                score += 1000

            overlap_preferred = tag_set.intersection(card.get("preferred_for", []))
            overlap_intent = tag_set.intersection(card.get("intent_tags", []))
            overlap_avoid = tag_set.intersection(card.get("avoid_when", []))
            score += len(overlap_preferred) * 80
            score += len(overlap_intent) * 40
            score -= len(overlap_avoid) * 120

            if in_subquery:
                # 子查询场景下，对可嵌套和 where 子查询可用的算子额外加权。
                if card.get("supports_subquery"):
                    score += 20
                if card.get("usable_in_where_subquery"):
                    score += 30
            if stage:
                if stage in card.get("preferred_for", []):
                    score += 25
                if stage in card.get("intent_tags", []):
                    score += 10

            # 低成本算子更适合出现在有限上下文中，因此反向减去 cost_rank。
            score -= int(card.get("cost_rank", 50))
            if score <= 0:
                continue
            selected.append((score, card))

        selected.sort(key=lambda item: (-item[0], item[1]["name"]))
        return [self._compact_card(card) for _, card in selected[:limit]]

    def _compact_card(self, card: dict[str, Any]) -> dict[str, Any]:
        """把完整算子定义裁剪成适合上下文注入的紧凑卡片。"""
        return {
            "name": card["name"],
            "summary": card.get("summary", ""),
            "syntax": card.get("syntax", ""),
            "preferred_for": card.get("preferred_for", []),
            "avoid_when": card.get("avoid_when", []),
            "supports_subquery": card.get("supports_subquery", False),
            "usable_in_where_subquery": card.get("usable_in_where_subquery", False),
            "returns_filter_expression": card.get("returns_filter_expression", False),
            "related_commands": card.get("related_commands", []),
            "canonical_examples": card.get("canonical_examples", [])[:2],
            "notes": card.get("notes", [])[:2],
        }


def normalize_field_type(field_type: object) -> str:
    """把字段类型压成稳定的基础类别。"""
    token = str(field_type or "").strip()
    if token.startswith("map:"):
        return "map"
    return token or "string"


def field_types_compatible(left_type: object, right_type: object, *, allowed_types: list[str] | None = None) -> bool:
    """判断两个字段类型是否兼容。"""
    left = normalize_field_type(left_type)
    right = normalize_field_type(right_type)
    if allowed_types and left not in allowed_types:
        return False
    if allowed_types and right not in allowed_types:
        return False
    return left == right


def is_regex_literal(value: object) -> bool:
    """判断一个值是否是 `/.../` 形态的 regex 字面量。"""
    if not isinstance(value, str):
        return False
    token = value.strip()
    return len(token) >= 2 and token.startswith("/") and token.endswith("/")


def normalize_regex_pattern(value: object) -> str:
    """把输入归一成不带分隔符的 regex pattern。"""
    if not isinstance(value, str):
        raise ValueError("regex pattern must be a string")
    token = value.strip()
    if not token:
        raise ValueError("regex pattern cannot be empty")
    if is_regex_literal(token):
        token = token[1:-1].replace("\\/", "/")
    if not token:
        raise ValueError("regex pattern cannot be empty")
    return token


def render_literal_value(value: object, *, literal_format: str = "default") -> str:
    """把 Python 值渲染成 HQL 字面量。"""
    if literal_format == "regex_literal":
        pattern = normalize_regex_pattern(value)
        escaped = pattern.replace("/", "\\/")
        return f"/{escaped}/"
    if value is None:
        return '""'
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if text.startswith("now(") and text.endswith(")"):
        return text
    return f'"{text}"'


def value_matches_field_type(field_type: object, value: object) -> bool:
    """判断字面量值是否与字段类型兼容。"""
    normalized_type = normalize_field_type(field_type)
    if normalized_type == "number":
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            return True
        if isinstance(value, str):
            try:
                float(value)
            except ValueError:
                return False
            return True
        return False
    if normalized_type in {"ip", "string"}:
        return isinstance(value, str)
    if normalized_type == "time":
        return isinstance(value, (int, float, str)) and not isinstance(value, bool)
    if normalized_type == "map":
        return isinstance(value, (dict, str))
    return not isinstance(value, (dict, list, tuple, set))


@lru_cache(maxsize=1)
def load_operator_registry() -> OperatorRegistry:
    """加载算子 registry。

    这里保留单项缓存，因为同一进程里 selector 和 reviewer 都可能多次读取算子库。
    这个缓存只对当前 Python 进程有效，不跨 CLI 调用共享。
    """
    payload = json.loads(OPERATORS_PATH.read_text(encoding="utf-8"))
    return OperatorRegistry(payload)
