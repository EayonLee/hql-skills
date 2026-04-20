"""HQL 规划器。

这个文件负责把已经完成语义绑定和字段绑定的 `CompiledIntent`
转换成 `PlanCandidate`。planner 不再直接消费原始自然语言短语，
而只根据：
- 已绑定谓词
- 最终输出列
- 中间推导步骤
- 结果策略
来决定最终 HQL 的形态。
"""

from __future__ import annotations

import json

from .internal_types import (
    BoundPredicate,
    BoundPredicateGroup,
    CompiledIntent,
    DerivationStep,
    PipelineAst,
    PipelineCommand,
    PlanCandidate,
    ResultSpec,
)
from .knowledge import index_for_source
from .operators import render_literal_value

# DERIVED_FILTER_KINDS: 表示“先求值，再把结果回填过滤条件”的推导步骤。
DERIVED_FILTER_KINDS = {"subquery_filter", "top_n_select", "derived_filter"}
# RANKING_KINDS: 表示 Top-N 排行类推导步骤。
RANKING_KINDS = {"top_n", "ranking"}


def _render_value_list(values: list[object]) -> str:
    """把值列表渲染成 HQL 函数参数中的数组字面量。"""
    return json.dumps(list(values), ensure_ascii=False)


def render_predicate(predicate: BoundPredicate) -> str:
    """把已绑定谓词渲染成 where 表达式片段。"""
    if isinstance(predicate, BoundPredicateGroup):
        connector = " or " if predicate.mode == "any_of" else " and "
        rendered_children = [render_predicate(child) for child in predicate.predicates if child.field]
        if not rendered_children:
            return ""
        body = connector.join(rendered_children)
        if predicate.mode == "any_of" or len(rendered_children) > 1:
            return f"({body})"
        return body

    if predicate.render_kind == "infix_field_field":
        expression = f"{predicate.field} {predicate.operator} {predicate.rhs_field}"
    elif predicate.render_kind == "function_field_list":
        expression = f"{predicate.operator}({predicate.field}, {_render_value_list(predicate.values)})"
    else:
        expression = f"{predicate.field} {predicate.operator} {render_literal_value(predicate.value, literal_format=predicate.literal_format)}"
    if predicate.negated:
        return f"not ({expression})"
    return expression


def where_expression(intent: CompiledIntent) -> str:
    """把所有已绑定谓词拼成顶层 where 表达式。"""
    parts = [render_predicate(predicate) for predicate in intent.bound_predicates]
    return " and ".join(part for part in parts if part)


def limit_for(intent: CompiledIntent) -> int | None:
    """返回最终明细查询的条数限制。"""
    if intent.limit is not None:
        return int(intent.limit)
    return None


def metric_outputs(intent: CompiledIntent) -> list[ResultSpec]:
    """返回所有 metric 输出列。"""
    return [item for item in intent.final_outputs if item.role == "metric"]


def dimension_outputs(intent: CompiledIntent) -> list[ResultSpec]:
    """返回所有维度输出列。"""
    return [item for item in intent.final_outputs if item.role == "dimension"]


def detail_outputs(intent: CompiledIntent) -> list[ResultSpec]:
    """返回所有普通明细字段输出列。"""
    return [item for item in intent.final_outputs if item.role == "field"]


def final_query_is_aggregate(intent: CompiledIntent) -> bool:
    """判断最终结果是否应为聚合表。"""
    return bool(metric_outputs(intent))


def wants_derived_filter(intent: CompiledIntent) -> bool:
    """判断是否存在派生值回填过滤场景。"""
    return any(step.kind in DERIVED_FILTER_KINDS for step in intent.derivations)


def prefers_multi_result(intent: CompiledIntent) -> bool:
    """判断是否允许显式输出多结果。"""
    return intent.result_policy == "explicit_multi_result"


def primary_subquery_field(intent: CompiledIntent) -> str:
    """找出用于子查询求值的主字段。"""
    for kind in ("subquery_filter", "top_n_select", "top_n", "group_aggregate", "group_aggregate_top_k", "aggregate_total"):
        for step in intent.derivations:
            if step.kind == kind and step.field:
                return step.field
    raise ValueError("当前意图缺少可用于子查询推导的主字段。")


def primary_selector_step(intent: CompiledIntent):
    """返回 detail + selector 场景对应的 top-k 选择步骤。"""
    for step in intent.derivations:
        if step.kind == "top_n_select":
            return step
    return None


def append_final_output_segments(segments: list[PipelineCommand], intent: CompiledIntent) -> None:
    """根据最终输出列，把聚合或明细结果段追加到主查询中。"""
    metrics = metric_outputs(intent)
    dimensions = dimension_outputs(intent)
    details = detail_outputs(intent)

    if metrics:
        expressions = []
        for metric in metrics:
            source_field = metric.source_field or "ID"
            expressions.append(f"{metric.aggregate_function}({source_field}) AS {metric.field}")
        stats_body = ", ".join(expressions)
        by_fields = [item.field for item in dimensions]
        if by_fields:
            stats_body = f"{stats_body} BY {', '.join(by_fields)}"
        segments.append(PipelineCommand("stats", stats_body))

        if intent.sorts:
            sort = intent.sorts[0]
            direction = "-" if sort.direction.lower() != "asc" else "+"
            segments.append(PipelineCommand("sort", f"{direction}{sort.field}"))
        elif dimensions:
            segments.append(PipelineCommand("sort", f"-{metrics[0].field}"))
        limit = limit_for(intent)
        if limit is not None:
            segments.append(PipelineCommand("head", str(limit)))
        return

    if intent.sorts:
        sort = intent.sorts[0]
        direction = "-" if sort.direction.lower() != "asc" else "+"
        segments.append(PipelineCommand("sort", f"{direction}{sort.field}"))

    output_fields = [item.field for item in details + dimensions]
    if output_fields:
        segments.append(PipelineCommand("fields", ", ".join(output_fields)))
    limit = limit_for(intent)
    if limit is not None:
        segments.append(PipelineCommand("head", str(limit)))


def plan_derived_filter(intent: CompiledIntent) -> PlanCandidate:
    """规划“派生值回填过滤条件”的单条查询。"""
    source_index = index_for_source(intent.source)
    where_body = where_expression(intent)
    group_field = primary_subquery_field(intent)
    selector_step = primary_selector_step(intent)
    ranking_limit = max(1, int(selector_step.limit or 1)) if selector_step else 1
    selector_details = selector_step.details if selector_step else {}
    ranking_alias = str(selector_details.get("alias", "数量"))
    ranking_direction = str(selector_details.get("direction", "desc")).lower()

    subquery_segments: list[PipelineCommand] = []
    if where_body:
        subquery_segments.append(PipelineCommand("where", where_body))
    stats_body = f"count(ID) AS {ranking_alias} BY {group_field}"
    if selector_step and selector_step.aggregate_function:
        source_field = selector_step.source_field or "ID"
        stats_body = f"{selector_step.aggregate_function}({source_field}) AS {ranking_alias} BY {group_field}"
    sort_direction = "+" if ranking_direction == "asc" else "-"
    subquery_segments.extend(
        [
            PipelineCommand("stats", stats_body),
            PipelineCommand("sort", f"{sort_direction}{ranking_alias}"),
            PipelineCommand("head", str(ranking_limit)),
            PipelineCommand("fields", group_field),
            PipelineCommand("format"),
        ]
    )
    subquery = PipelineAst(index=source_index, segments=subquery_segments)

    main_where = " and ".join(part for part in [where_body, "__subquery_0__"] if part)
    main_segments = [PipelineCommand("where", main_where, subqueries=[subquery])]
    append_final_output_segments(main_segments, intent)

    return PlanCandidate(
        shape="derived_filter_aggregate" if final_query_is_aggregate(intent) else "derived_filter_detail",
        ast=[PipelineAst(index=source_index, segments=main_segments)],
        single_query=True,
        cost=20,
        completeness=1.0,
        derivations=[
            DerivationStep("group_aggregate", field=group_field),
            DerivationStep("top_n_select", field=group_field, limit=ranking_limit, details=dict(selector_details)),
            DerivationStep("subquery_filter", field=group_field, limit=ranking_limit, details=dict(selector_details)),
        ],
    )


def plan_explicit_intermediate(intent: CompiledIntent) -> PlanCandidate:
    """规划显式多结果方案。"""
    source_index = index_for_source(intent.source)
    where_body = where_expression(intent)
    group_field = primary_subquery_field(intent)
    selector_step = primary_selector_step(intent)
    alias = str((selector_step.details if selector_step else {}).get("alias", f"{group_field}数量"))
    direction = str((selector_step.details if selector_step else {}).get("direction", "desc")).lower()
    source_field = selector_step.source_field if selector_step and selector_step.source_field else "ID"
    function = selector_step.aggregate_function if selector_step and selector_step.aggregate_function else "count"

    stats_segments: list[PipelineCommand] = []
    if where_body:
        stats_segments.append(PipelineCommand("where", where_body))
    stats_segments.extend(
        [
            PipelineCommand("stats", f"{function}({source_field}) AS {alias} BY {group_field}"),
            PipelineCommand("sort", f"{'+' if direction == 'asc' else '-'}{alias}"),
        ]
    )

    detail_plan = plan_derived_filter(intent)
    detail_segments = detail_plan.ast[0].segments

    return PlanCandidate(
        shape="explicit_multi_result",
        ast=[
            PipelineAst(index=source_index, segments=stats_segments),
            PipelineAst(index=source_index, segments=detail_segments),
        ],
        single_query=False,
        cost=60,
        completeness=1.0,
        fallback_reason="用户明确要求同时展示中间统计结果和最终结果。",
        derivations=[
            DerivationStep("group_aggregate", field=group_field),
            DerivationStep("top_n_select", field=group_field, limit=selector_step.limit if selector_step else None, details=dict(selector_step.details) if selector_step else {}),
            DerivationStep("subquery_filter", field=group_field, limit=selector_step.limit if selector_step else None, details=dict(selector_step.details) if selector_step else {}),
        ],
    )


def plan_aggregate(intent: CompiledIntent) -> PlanCandidate:
    """规划最终结果就是聚合表的查询。"""
    source_index = index_for_source(intent.source)
    where_body = where_expression(intent)
    segments: list[PipelineCommand] = []
    if where_body:
        segments.append(PipelineCommand("where", where_body))
    append_final_output_segments(segments, intent)
    dimensions = dimension_outputs(intent)
    if not dimensions:
        shape = "aggregate_total"
    elif intent.limit is not None:
        shape = "aggregate_top_k"
    else:
        shape = "aggregate_grouped"
    return PlanCandidate(
        shape=shape,
        ast=[PipelineAst(index=source_index, segments=segments)],
        single_query=True,
        cost=18,
        completeness=1.0,
        derivations=[DerivationStep(step.kind, field=step.field, limit=step.limit, details=dict(step.details)) for step in intent.derivations],
    )


def plan_ranking(intent: CompiledIntent) -> PlanCandidate:
    """规划最终结果是 Top-N 排行的查询。"""
    source_index = index_for_source(intent.source)
    where_body = where_expression(intent)
    ranking_field = primary_subquery_field(intent)
    ranking_limit = next((step.limit for step in intent.derivations if step.kind in RANKING_KINDS and step.limit), None)
    limit = max(1, int(ranking_limit or intent.limit or 20))
    segments: list[PipelineCommand] = []
    if where_body:
        segments.append(PipelineCommand("where", where_body))
    segments.append(PipelineCommand("top", f"{limit} {ranking_field}"))
    return PlanCandidate(
        shape="ranking_top_n",
        ast=[PipelineAst(index=source_index, segments=segments)],
        single_query=True,
        cost=15,
        completeness=1.0,
        derivations=[DerivationStep("top_n", field=ranking_field, limit=limit)],
    )


def plan_detail(intent: CompiledIntent) -> PlanCandidate:
    """规划普通明细查询。"""
    source_index = index_for_source(intent.source)
    where_body = where_expression(intent)
    segments: list[PipelineCommand] = []
    if where_body:
        segments.append(PipelineCommand("where", where_body))
    append_final_output_segments(segments, intent)
    return PlanCandidate(
        shape="detail_query",
        ast=[PipelineAst(index=source_index, segments=segments)],
        single_query=True,
        cost=8,
        completeness=1.0,
        derivations=[DerivationStep(step.kind, field=step.field, limit=step.limit, details=dict(step.details)) for step in intent.derivations],
    )


def plan_query(intent: CompiledIntent) -> PlanCandidate:
    """按固定策略顺序从 CompiledIntent 生成计划。"""
    if prefers_multi_result(intent):
        return plan_explicit_intermediate(intent)
    if wants_derived_filter(intent):
        return plan_derived_filter(intent)
    if final_query_is_aggregate(intent):
        return plan_aggregate(intent)
    if any(step.kind in RANKING_KINDS for step in intent.derivations):
        return plan_ranking(intent)
    return plan_detail(intent)
