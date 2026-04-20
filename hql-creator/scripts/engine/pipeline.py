"""HQL 管道解析与渲染。

这个文件负责两件事：
1. 把 HQL 字符串解析成 `PipelineAst`，并正确识别 nested subquery。
2. 把 planner 产出的 AST 渲染回标准化 HQL 字符串。

之所以把 parse 与 render 放在一起，是因为它们共享同一套占位符和分段规则。
"""

from __future__ import annotations

import re

from .internal_types import PipelineAst, PipelineCommand, PlanCandidate

# INDEX_SEGMENT_RE: 匹配 canonical pipeline 的首段 `index == "<index>"`。
INDEX_SEGMENT_RE = re.compile(r'^index\s*==\s*"([A-Za-z0-9_*.-]+)"\s*$')


def subquery_placeholder(index: int) -> str:
    """为 nested subquery 生成临时占位符。"""
    return f"__subquery_{index}__"


def should_start_regex(current: list[str]) -> bool:
    """判断当前位置的 `/` 是否应该被视为正则开始符。"""
    prefix = "".join(current).rstrip()
    match = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*$", prefix)
    return bool(match and match.group(1).lower() == "rlike")


def split_pipeline(hql: str) -> list[str]:
    """按顶层 `|` 把 HQL 拆成多个 segment。"""
    parts: list[str] = []
    current: list[str] = []
    quote: str | None = None
    regex_mode = False
    bracket_depth = 0
    paren_depth = 0

    for char in hql:
        if quote:
            current.append(char)
            if char == quote:
                quote = None
            continue
        if regex_mode:
            current.append(char)
            if char == "/":
                regex_mode = False
            continue
        if char in {"'", '"'}:
            quote = char
            current.append(char)
            continue
        if char == "/" and should_start_regex(current):
            regex_mode = True
            current.append(char)
            continue
        # 方括号与圆括号中的 `|` 不应切开顶层 pipeline，因此要单独维护深度。
        if char == "[":
            bracket_depth += 1
            current.append(char)
            continue
        if char == "]":
            bracket_depth = max(0, bracket_depth - 1)
            current.append(char)
            continue
        if char == "(":
            paren_depth += 1
            current.append(char)
            continue
        if char == ")":
            paren_depth = max(0, paren_depth - 1)
            current.append(char)
            continue
        if char == "|" and bracket_depth == 0 and paren_depth == 0:
            segment = "".join(current).strip()
            if segment:
                parts.append(segment)
            current = []
            continue
        current.append(char)

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def parse_segment_text(segment: str) -> tuple[str, str]:
    """把单个 segment 切成命令名和命令体。"""
    parts = segment.split(None, 1)
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1].strip()


def extract_subqueries(text: str) -> tuple[str, list[PipelineAst]]:
    """从命令体中抽出 nested subquery，并用占位符替换。"""
    current: list[str] = []
    subqueries: list[PipelineAst] = []
    quote: str | None = None
    regex_mode = False
    bracket_depth = 0
    nested: list[str] = []

    for char in text:
        if quote:
            if bracket_depth > 0:
                nested.append(char)
            else:
                current.append(char)
            if char == quote:
                quote = None
            continue
        if regex_mode:
            if bracket_depth > 0:
                nested.append(char)
            else:
                current.append(char)
            if char == "/":
                regex_mode = False
            continue

        target = nested if bracket_depth > 0 else current
        if char in {"'", '"'}:
            quote = char
            target.append(char)
            continue
        if char == "/" and should_start_regex(target):
            regex_mode = True
            target.append(char)
            continue
        if char == "[":
            if bracket_depth == 0:
                bracket_depth = 1
                nested = []
            else:
                bracket_depth += 1
                nested.append(char)
            continue
        if char == "]":
            if bracket_depth == 0:
                current.append(char)
                continue
            bracket_depth -= 1
            if bracket_depth == 0:
                # 只有最外层 `[]` 闭合且内容看起来像 HQL 子查询时，才递归解析成 subquery AST。
                candidate = "".join(nested).strip()
                if candidate.lower().startswith("index =="):
                    placeholder = subquery_placeholder(len(subqueries))
                    subqueries.append(parse_hql(candidate))
                    current.append(placeholder)
                else:
                    current.append(f"[{candidate}]")
                nested = []
            else:
                nested.append(char)
            continue
        target.append(char)

    # 如果方括号没有闭合，保留原样交给 reviewer 后续报错。
    if bracket_depth > 0:
        current.append("[" + "".join(nested))
    return "".join(current).strip(), subqueries


def parse_hql(hql: str) -> PipelineAst:
    """把 HQL 字符串解析成 AST。"""
    segments = split_pipeline(hql.strip())
    if not segments:
        return PipelineAst(index="", segments=[], raw_index_segment="")

    raw_index_segment = segments[0]
    match = INDEX_SEGMENT_RE.match(raw_index_segment)
    index = match.group(1) if match else ""
    # 如果首段不是 index，仍然把 AST 构建出来，让 reviewer 再统一报 canonical 错误。
    start = 1 if match else 0
    pipeline_segments: list[PipelineCommand] = []

    for segment in segments[start:]:
        command, body = parse_segment_text(segment)
        body, subqueries = extract_subqueries(body)
        pipeline_segments.append(PipelineCommand(command=command, body=body, subqueries=subqueries))

    return PipelineAst(index=index, segments=pipeline_segments, raw_index_segment=raw_index_segment)


def render_pipeline(ast: PipelineAst) -> str:
    """把单条 AST 渲染回标准化 HQL。"""
    first = ast.raw_index_segment or (f'index == "{ast.index}"' if ast.index else "")
    parts = [first] if first else []
    for segment in ast.segments:
        body = segment.body
        # 渲染时把命令体中的占位符替换回真实子查询文本。
        for index, subquery in enumerate(segment.subqueries):
            body = body.replace(subquery_placeholder(index), f"[{render_pipeline(subquery)}]")
        parts.append(segment.command if not body else f"{segment.command} {body}")
    return " | ".join(part for part in parts if part)


def render_plan(plan: PlanCandidate) -> str:
    """把计划中的一条或多条 AST 统一渲染成输出文本。"""
    return "\n\n".join(render_pipeline(ast) for ast in plan.ast)
