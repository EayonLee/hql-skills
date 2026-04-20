"""主路径使用的 internal 类型真相源。

这里只保留：
- 编译链内部 dataclass
- 跨入口共享的错误类型
- 少量稳定常量

不再承接任何 public request 解析或历史兼容逻辑。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field as dc_field
from typing import Any, Union

REQUEST_OPERATORS = {"==", "!=", ">", ">=", "<", "<=", "like", "rlike", "belong", "any_match"}


class StrictSchemaError(ValueError):
    """表示编译阶段遇到了不合法的结构或不被支持的组合。"""


class RequestParseError(ValueError):
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
        super().__init__(message)
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


@dataclass
class FieldConstraintSpec:
    """表示外部请求里的显式字段约束。"""

    field: str = ""
    field_phrase: str = ""
    operator: str = "=="
    value: Any = None


@dataclass
class SemanticConstraintSpec:
    """表示业务语义目标上的取值约束。"""

    target_id: str
    operator: str = "=="
    value: Any = None


@dataclass
class MetricSpec:
    """表示最终结果中的聚合指标定义。"""

    function: str = ""
    field: str = ""
    alias: str = ""


@dataclass
class TopKSpec:
    """表示基于聚合结果保留前 N 条。"""

    limit: int = 1
    direction: str = "desc"


@dataclass
class FinalResultSpec:
    """表示用户真正想看到的最终结果形态。"""

    type: str
    field_phrases: list[str] = dc_field(default_factory=list)
    group_by_phrase: str = ""
    metric: MetricSpec = dc_field(default_factory=MetricSpec)
    top_k: TopKSpec | None = None


@dataclass
class DetailLimitSpec:
    """表示明细查询的限制策略。"""

    mode: str = "default"
    value: int | None = None


@dataclass
class SortSpec:
    """表示一个排序目标。"""

    field: str
    direction: str = "desc"


@dataclass
class ParsedRequest:
    """表示 compiler-facing `ParsedRequestV1`。"""

    schema_version: int
    source: str
    time_range: dict[str, str] = dc_field(default_factory=dict)
    semantic_macro_ids: list[str] = dc_field(default_factory=list)
    semantic_constraints: list[SemanticConstraintSpec] = dc_field(default_factory=list)
    field_constraints: list[FieldConstraintSpec] = dc_field(default_factory=list)
    final_result: FinalResultSpec = dc_field(default_factory=lambda: FinalResultSpec(type="detail"))
    show_intermediate: bool = False
    detail_limit: DetailLimitSpec = dc_field(default_factory=DetailLimitSpec)
    sort: SortSpec | None = None
    raw_query: str = ""

    def to_dict(self) -> dict[str, Any]:
        """导出成 JSON 可序列化的字典。"""
        return asdict(self)


@dataclass
class LoadedRequest:
    """表示主入口加载到的请求及其附带告警。"""

    request: ParsedRequest
    warnings: list[str] = dc_field(default_factory=list)
    contract: str = "draft_v1"


@dataclass
class BoundPredicateAtom:
    """表示已经完成绑定的原子约束。"""

    field: str
    field_key: str = ""
    operator: str = "=="
    value: Any = None
    values: list[Any] = dc_field(default_factory=list)
    rhs_field: str = ""
    rhs_field_key: str = ""
    render_kind: str = "infix_field_value"
    literal_format: str = "default"
    operator_category: str = "comparison"
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
