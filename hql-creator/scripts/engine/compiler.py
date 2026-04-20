"""单次确定性编译链入口。

这个文件只负责编排结构化请求的固定编译流程：

1. 读取并校验 compiler-facing 请求（通常由 public request lowering 而来）
2. 绑定业务语义规则与字段
3. 生成内部 `CompiledIntent`
4. 规划 AST
5. 审查 AST
6. 渲染最终 HQL

这里不再承接中文解析；自然语言理解必须发生在模型侧。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .internal_types import CompiledIntent, CompiledQuery, ParsedRequest, PlanCandidate, ReviewReport, StrictSchemaError
from .knowledge import build_field_bindings, compile_intent, resolved_semantic_dicts
from .operators import load_operator_registry
from .pipeline import render_plan
from .planner import plan_query
from .reviewer import review_plan


@dataclass
class CompileFailure(StrictSchemaError):
    """表示结构化请求在编译链中途失败，并携带可诊断上下文。"""

    message: str
    stage: str
    request: ParsedRequest
    intent: CompiledIntent | None = None
    plan: PlanCandidate | None = None
    rendered_hql: str = ""
    review: ReviewReport | None = None

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        """导出成 JSON 可序列化的诊断对象。"""
        payload = {
            "error_code": "compile_failed",
            "message": self.message,
            "stage": self.stage,
            "request": self.request.to_dict(),
        }
        if self.intent is not None:
            payload["intent"] = self.intent.to_dict()
            payload["resolved_semantics"] = resolved_semantic_dicts(self.intent)
            payload["field_bindings"] = build_field_bindings(self.request, self.intent)
        if self.plan is not None:
            payload["plan"] = self.plan.to_dict()
        if self.rendered_hql:
            payload["rendered_hql"] = self.rendered_hql
        if self.review is not None:
            payload["review"] = self.review.to_dict()
        return payload


def summarize_review_failure(report: dict, rendered_hql: str) -> str:
    """把审查失败压缩成适合 CLI 直接展示的一段错误说明。"""
    parts: list[str] = ["生成后的 HQL 未通过审查。"]
    for key in ("canonical_issues", "unknown_fields", "unknown_commands", "strategy_warnings"):
        values = report.get(key) or []
        if values:
            parts.append(f"{key}=" + "、".join(values))
    parts.append("rendered_hql=" + rendered_hql)
    return " ".join(parts)


def infer_operator_context_tags(shape: str) -> tuple[str, bool]:
    """根据 planner 产出的形态推断调试输出需要展示的算子上下文。"""
    if shape.startswith("derived_filter"):
        return "derived_filter", True
    if shape in {"aggregate_total", "aggregate_grouped", "aggregate_top_k"}:
        return "stats", False
    if shape == "ranking_top_n":
        return "top_n", False
    return "detail", False


def build_operator_context(intent_tags: list[str], plan_shape: str) -> dict[str, object]:
    """生成调试包中展示的算子上下文。"""
    stage, in_subquery = infer_operator_context_tags(plan_shape)
    registry = load_operator_registry()
    return {
        "stage": stage,
        "in_subquery": in_subquery,
        "cards": registry.select_context(intent_tags=intent_tags, stage=stage, in_subquery=in_subquery),
    }


def compile_parsed_request(request: ParsedRequest) -> CompiledQuery:
    """从结构化请求执行完整编译链。"""
    compiled_intent = compile_intent(request)
    plan = plan_query(compiled_intent)
    if compiled_intent.result_policy != "explicit_multi_result" and not plan.single_query:
        raise CompileFailure(
            "当前结果策略不允许输出多结果查询。",
            stage="planning",
            request=request,
            intent=compiled_intent,
            plan=plan,
        )

    report = review_plan(compiled_intent.source, plan.ast, intent=compiled_intent)
    rendered_hql = render_plan(plan)
    if not report.ok:
        raise CompileFailure(
            summarize_review_failure(report.to_dict(), rendered_hql),
            stage="review",
            request=request,
            intent=compiled_intent,
            plan=plan,
            rendered_hql=rendered_hql,
            review=report,
        )

    return CompiledQuery(
        request=request,
        resolved_semantics=resolved_semantic_dicts(compiled_intent),
        field_bindings=build_field_bindings(request, compiled_intent),
        intent=compiled_intent,
        operator_context=build_operator_context(compiled_intent.intent_tags, plan.shape),
        plan=plan,
        rendered_hql=rendered_hql,
        review=report,
    )
