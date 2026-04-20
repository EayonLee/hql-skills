"""递归 HQL 审查器。

这个文件负责把 HQL AST 与字段目录、算子 registry、canonical 规则结合起来做审查。
它既检查顶层 pipeline，也会递归检查 `[...]` 中的 nested subquery。

除了原有的字段和命令检查外，本版还加入了“语义一致性审查”：
- authoritative 业务语义规则不得被忽略
- 非 multi-result 策略下不得意外暴露中间推导结果
"""

from __future__ import annotations

import ast
import json
import re
from typing import Iterable

from .internal_types import BoundPredicate, BoundPredicateAtom, BoundPredicateGroup, CompiledIntent, PipelineAst, ReviewReport
from .expression import (
    find_unknown_symbolic_operators,
    has_single_equals_operator,
    has_now_slash_syntax,
    iter_comparisons,
    iter_function_calls,
    iter_word_operator_pairs,
    split_function_args,
    split_top_level_boolean,
    strip_outer_parens,
)
from .knowledge import INDEX_TO_SOURCE, load_catalog, index_for_source, source_for_index
from .operators import (
    field_types_compatible,
    is_regex_literal,
    load_operator_registry,
    normalize_field_type,
    normalize_regex_pattern,
    value_matches_field_type,
)
from .pipeline import parse_hql, render_pipeline

COMMANDS_WITHOUT_FIELD_CHECK = {"append", "format", "head", "join", "makeresults", "map", "tail"}
AGGREGATE_RESULT_COMMANDS = {"stats", "chart", "xyseries", "top"}
FUNCTION_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\(([^)]*)\)")
HEAD_RE = re.compile(r"^(\d+)\b")
ABSOLUTE_TIME_STRING_RE = re.compile(r'^(["\'])\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\1$')
EPOCH_MS_RE = re.compile(r"^\d{13}$")
FIELD_TOKEN_RE = re.compile(r"^[A-Za-z0-9_\u4e00-\u9fff.]+$")
AS_RE = re.compile(r"\bAS\s+([A-Za-z0-9_\u4e00-\u9fff]+)", re.IGNORECASE)
SETTING_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_-]*)=(.+)$")
PLACEHOLDER_RE = re.compile(r"__subquery_\d+__")
GENERIC_RECORD_ID_FIELD_VARIANTS = {
    "告警ID",
    "威胁告警ID",
    "原始告警ID",
    "日志ID",
    "原始日志ID",
    "合并告警ID",
}
MULTI_QUERY_SPLIT_RE = re.compile(r"\n\s*\n(?=index\s*==)", re.IGNORECASE)


def add_issue(target: list[str], value: str) -> None:
    """向问题列表中追加去重后的问题码。"""
    if value not in target:
        target.append(value)


def split_fields(text: str) -> list[str]:
    """把字段列表按逗号或空白切开。"""
    cleaned = text.replace("，", ",")
    return [item.strip() for item in re.split(r",|\s+", cleaned) if item.strip()]


def split_by_keyword(text: str, keyword: str) -> tuple[str, str]:
    """按关键字切分文本，返回关键字前后的两段内容。"""
    match = re.search(rf"\b{keyword}\b", text, flags=re.IGNORECASE)
    if not match:
        return text, ""
    return text[: match.start()].strip(), text[match.end() :].strip()


def normalize_field_token(token: str) -> str:
    """把字段 token 归一成 reviewer 内部使用的比较形式。"""
    value = token.strip()
    if "." in value:
        value = value.split(".")[-1]
    return value.strip("()")


def sanitize_expression(text: str) -> str:
    """把子查询占位符替换成安全字面量，避免干扰表达式审查。"""
    return PLACEHOLDER_RE.sub("true", text)


def canonical_value(value: object) -> str:
    """把值归一成可比较、可哈希的稳定字符串。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def field_exists(token: str, available_fields: set[str]) -> bool:
    """判断 token 是否是当前上下文里已知字段。"""
    return normalize_field_token(token) in available_fields


def field_type_for(token: str, field_types: dict[str, str]) -> str:
    """返回某个字段 token 的类型。"""
    return field_types.get(normalize_field_token(token), "")


def field_is_array(token: str, field_arrays: dict[str, bool]) -> bool:
    """判断某个字段 token 是否是数组字段。"""
    return bool(field_arrays.get(normalize_field_token(token), False))


def field_is_belong_rhs(token: str, belong_rhs_fields: dict[str, bool]) -> bool:
    """判断某个字段 token 是否可作为 `belong` 的右侧业务归属字段。"""
    return bool(belong_rhs_fields.get(normalize_field_token(token), False))


def record_unknown_field(token: str, available_fields: set[str], unknown_fields: set[str]) -> None:
    """在确认 token 不是合法字段后，把它登记到 unknown_fields。"""
    normalized = normalize_field_token(token)
    if not normalized or PLACEHOLDER_RE.fullmatch(normalized):
        return
    if normalized in available_fields:
        return
    if re.fullmatch(r"[0-9]+", normalized):
        return
    if normalized.lower() in {"and", "or", "not", "true"}:
        return
    unknown_fields.add(normalized)


def review_canonical_field_name(token: str, canonical_issues: list[str]) -> None:
    """检查字段名是否违反 skill 约定的 canonical 命名。"""
    if normalize_field_token(token) in GENERIC_RECORD_ID_FIELD_VARIANTS:
        add_issue(canonical_issues, "generic_record_identifier_must_use_ID")


def review_field_reference(
    token: str,
    available_fields: set[str],
    unknown_fields: set[str],
    canonical_issues: list[str],
) -> None:
    """统一处理一个字段引用的存在性与 canonical 校验。"""
    record_unknown_field(token, available_fields, unknown_fields)
    review_canonical_field_name(token, canonical_issues)


def review_time_literals(
    expression: str,
    available_fields: set[str],
    field_types: dict[str, str],
    canonical_issues: list[str],
) -> None:
    """检查时间字段比较时使用的字面量是否符合规范。"""
    for comparison in iter_comparisons(expression):
        field_token = comparison.field_token
        operator = comparison.operator
        value_token = comparison.value_token.strip()
        if operator in {"like", "rlike"}:
            continue
        if field_type_for(field_token, field_types) != "time":
            continue
        if value_token.startswith("now("):
            continue
        if ABSOLUTE_TIME_STRING_RE.match(value_token):
            continue
        if EPOCH_MS_RE.match(value_token):
            continue
        if FIELD_TOKEN_RE.fullmatch(value_token) and field_exists(value_token, available_fields):
            continue
        if value_token.startswith(("'", '"')):
            add_issue(canonical_issues, "absolute_time_string_must_use_full_datetime")
        elif re.fullmatch(r"\d+", value_token):
            add_issue(canonical_issues, "absolute_time_epoch_must_use_13_digit_ms")


def review_expression(
    expression: str,
    available_fields: set[str],
    field_types: dict[str, str],
    field_arrays: dict[str, bool],
    belong_rhs_fields: dict[str, bool],
    allowed_ops: set[str],
    unknown_fields: set[str],
    unknown_operators: set[str],
    unknown_functions: set[str],
    canonical_issues: list[str],
) -> None:
    """审查 where/search 表达式中的字段、运算符和时间字面量。"""
    sanitized = sanitize_expression(expression)
    registry = load_operator_registry()
    if has_single_equals_operator(sanitized):
        add_issue(canonical_issues, "single_equals_not_allowed")
    if has_now_slash_syntax(sanitized):
        add_issue(canonical_issues, "use_now_parentheses_syntax")
    review_time_literals(sanitized, available_fields, field_types, canonical_issues)

    for comparison in iter_comparisons(sanitized):
        field_token = comparison.field_token
        operator = comparison.operator
        review_field_reference(field_token, available_fields, unknown_fields, canonical_issues)
        capability = registry.predicate_capability(operator)
        if capability["rhs_literal_format"] == "regex_literal":
            rhs_token = comparison.value_token.strip()
            if not is_regex_literal(rhs_token):
                add_issue(canonical_issues, "rlike_requires_regex_literal")
                continue
            try:
                normalize_regex_pattern(rhs_token)
            except ValueError:
                add_issue(canonical_issues, "rlike_requires_regex_literal")
                continue
        if capability["render_kind"] == "infix_field_field":
            rhs_token = comparison.value_token.strip()
            if not FIELD_TOKEN_RE.fullmatch(rhs_token) or not field_exists(rhs_token, available_fields):
                add_issue(canonical_issues, "belong_requires_rhs_field")
                continue
            review_field_reference(rhs_token, available_fields, unknown_fields, canonical_issues)
            lhs_type = field_type_for(field_token, field_types)
            rhs_type = field_type_for(rhs_token, field_types)
            lhs_is_belong_rhs = field_is_belong_rhs(field_token, belong_rhs_fields)
            rhs_is_belong_rhs = field_is_belong_rhs(rhs_token, belong_rhs_fields)
            if capability["lhs_forbids_belong_rhs"] and lhs_is_belong_rhs:
                add_issue(canonical_issues, "belong_requires_business_group_field")
            if capability["rhs_requires_belong_rhs"] and not rhs_is_belong_rhs:
                add_issue(canonical_issues, "belong_requires_business_group_field")
            if not field_types_compatible(lhs_type, rhs_type, allowed_types=capability["rhs_allowed_types"]):
                add_issue(canonical_issues, "belong_field_type_mismatch")
        if operator not in allowed_ops:
            unknown_operators.add(operator)

    for operator in find_unknown_symbolic_operators(sanitized, allowed_ops):
        unknown_operators.add(operator)

    for field_token, operator in iter_word_operator_pairs(sanitized):
        if operator in allowed_ops:
            continue
        if not field_exists(field_token, available_fields):
            continue
        review_field_reference(field_token, available_fields, unknown_fields, canonical_issues)
        unknown_operators.add(operator)

    for function_call in iter_function_calls(sanitized):
        args_list = split_function_args(function_call.args_text.replace("，", ","))
        if function_call.name in registry.deprecated_predicate_functions:
            add_issue(canonical_issues, "belong_must_use_plain_operator")
            continue
        capability = registry.predicate_capability(function_call.name)
        if capability["render_kind"] == "function_field_list":
            if len(args_list) != 2:
                add_issue(canonical_issues, "any_match_requires_field_and_list")
                continue
            field_arg = args_list[0].strip()
            list_arg = args_list[1].strip()
            if not FIELD_TOKEN_RE.fullmatch(field_arg) or not field_exists(field_arg, available_fields):
                add_issue(canonical_issues, "any_match_requires_field")
                continue
            review_field_reference(field_arg, available_fields, unknown_fields, canonical_issues)
            if capability["lhs_requires_array"] and not field_is_array(field_arg, field_arrays):
                add_issue(canonical_issues, "any_match_requires_field")
            try:
                parsed_values = ast.literal_eval(list_arg)
            except (ValueError, SyntaxError):
                add_issue(canonical_issues, "any_match_requires_literal_list")
                continue
            if not isinstance(parsed_values, list) or not parsed_values:
                add_issue(canonical_issues, "any_match_requires_literal_list")
                continue
            lhs_type = normalize_field_type(field_type_for(field_arg, field_types))
            for item in parsed_values:
                if not value_matches_field_type(lhs_type, item):
                    add_issue(canonical_issues, "any_match_value_type_mismatch")
                    break
            continue
        if function_call.name not in {"ip_in", "now"} and function_call.name.isalpha():
            for arg in args_list[:1]:
                if re.fullmatch(r"[A-Za-z0-9_\u4e00-\u9fff.]+", arg):
                    review_field_reference(arg, available_fields, unknown_fields, canonical_issues)


def register_aliases(text: str, derived_fields: set[str]) -> None:
    """把 `AS 别名` 中出现的派生字段登记到上下文。"""
    for alias in AS_RE.findall(text):
        derived_fields.add(alias)


def detect_query_shape(ast: PipelineAst) -> str:
    """根据 AST 判断当前查询的稳定形态。"""
    commands = {segment.command for segment in ast.segments}
    has_subquery = any(segment.subqueries for segment in ast.segments)
    if has_subquery:
        if commands & (AGGREGATE_RESULT_COMMANDS - {"top"}):
            return "derived_filter_aggregate"
        return "derived_filter_detail"
    if "top" in commands:
        return "ranking_top_n"
    stats_segments = [segment for segment in ast.segments if segment.command == "stats"]
    if stats_segments:
        has_group_by = any(re.search(r"\bBY\b", segment.body, flags=re.IGNORECASE) for segment in stats_segments)
        if not has_group_by:
            return "aggregate_total"
        if any(segment.command == "head" for segment in ast.segments):
            return "aggregate_top_k"
        return "aggregate_grouped"
    return "detail_query"


def validate_global_rules(rendered: str, canonical_issues: list[str]) -> None:
    """校验整条 HQL 的全局 canonical 规则。"""
    for literal in re.findall(r'index\s*==\s*"([A-Za-z0-9_*.-]+)"', rendered):
        if literal not in INDEX_TO_SOURCE:
            add_issue(canonical_issues, f'unsupported_index_literal="{literal}"')


def validate_pipeline_structure(
    ast: PipelineAst,
    source: str,
    canonical_issues: list[str],
    *,
    intent: CompiledIntent | None = None,
) -> str:
    """校验顶层 pipeline 结构是否符合 skill 的 canonical 约束。"""
    if not ast.raw_index_segment and not ast.index:
        add_issue(canonical_issues, "empty_hql")
        return "detail_query"

    expected_index = index_for_source(source)
    if ast.index != expected_index:
        add_issue(canonical_issues, f'first_segment_must_be_index == "{expected_index}"')

    shape = detect_query_shape(ast)
    where_positions: list[int] = []
    limit_found = False

    for index, segment in enumerate(ast.segments):
        if segment.command == "search":
            add_issue(canonical_issues, "search_not_allowed_in_canonical_pipeline")
        if segment.command == "where":
            where_positions.append(index)
            if not segment.body:
                add_issue(canonical_issues, "where_requires_expression")
        if segment.command == "head":
            match = HEAD_RE.match(segment.body)
            if match and int(match.group(1)) > 0:
                limit_found = True
            else:
                add_issue(canonical_issues, "head_requires_positive_integer")
        if segment.command == "top":
            limit_found = True

    if where_positions and where_positions[0] != 0:
        add_issue(canonical_issues, "where_must_be_immediately_after_index")
    if len(where_positions) > 1:
        add_issue(canonical_issues, "only_one_top_level_where_is_allowed")
    # 明细查询现在允许默认不加 head；只有显式要求 limit 时，planner 才会补 head。
    return shape


def review_segment(
    command_name: str,
    body: str,
    *,
    catalog: dict[str, dict[str, dict[str, object]]],
    available_fields: set[str],
    field_types: dict[str, str],
    field_arrays: dict[str, bool],
    belong_rhs_fields: dict[str, bool],
    derived_fields: set[str],
    unknown_fields: set[str],
    unknown_commands: set[str],
    unknown_operators: set[str],
    unknown_functions: set[str],
    unknown_chart_panels: set[str],
    canonical_issues: list[str],
    notes: list[str],
) -> None:
    """按命令类型审查单个 segment。"""
    registry = load_operator_registry()
    if command_name not in registry.command_names:
        unknown_commands.add(command_name)
        return

    current_fields = available_fields | derived_fields

    if command_name in {"search", "where"}:
        review_expression(
            body,
            current_fields,
            field_types,
            field_arrays,
            belong_rhs_fields,
            registry.allowed_expression_operators,
            unknown_fields,
            unknown_operators,
            unknown_functions,
            canonical_issues,
        )
        return

    if command_name == "fields":
        payload = body[1:].strip() if body.startswith(("+", "-")) else body
        for field in split_fields(payload):
            review_field_reference(field, current_fields, unknown_fields, canonical_issues)
        return

    if command_name == "sort":
        for field in split_fields(body):
            review_field_reference(field.lstrip("+-"), current_fields, unknown_fields, canonical_issues)
        return

    if command_name == "top":
        before_by, after_by = split_by_keyword(body, "BY")
        tokens = [token for token in split_fields(before_by) if token]
        filtered = [token for token in tokens if not re.fullmatch(r"\d+", token) and not SETTING_RE.match(token)]
        for field in filtered:
            review_field_reference(field, current_fields, unknown_fields, canonical_issues)
        if after_by:
            for field in split_fields(after_by):
                review_field_reference(field, current_fields, unknown_fields, canonical_issues)
        return

    if command_name == "stats":
        valid_stats_functions = 0
        for function_name, args in FUNCTION_RE.findall(body):
            if function_name not in registry.stats_functions:
                unknown_functions.add(function_name)
                continue
            valid_stats_functions += 1
            capability = registry.metric_capability(function_name)
            arg = args.split(",", 1)[0].strip()
            if arg == "*":
                add_issue(canonical_issues, "stats_star_not_allowed")
                continue
            if not arg:
                add_issue(canonical_issues, f"{function_name}_requires_field" if capability["requires_field"] else "stats_function_missing_field")
                continue
            if re.fullmatch(r"[A-Za-z0-9_\u4e00-\u9fff.]+", arg):
                review_field_reference(arg, current_fields, unknown_fields, canonical_issues)
        if valid_stats_functions == 0:
            add_issue(canonical_issues, "stats_requires_function(field)")
        register_aliases(body, derived_fields)
        by_match = re.search(r"\bBY\b(.+)$", body)
        if by_match:
            for field in split_fields(by_match.group(1)):
                review_field_reference(field, current_fields | derived_fields, unknown_fields, canonical_issues)
        return

    if command_name == "bucket":
        as_match = re.search(r"\bAS\b\s+([A-Za-z0-9_\u4e00-\u9fff]+)", body, flags=re.IGNORECASE)
        head = body[: as_match.start()].strip() if as_match else body
        if as_match:
            derived_fields.add(as_match.group(1))
        candidates = [token for token in split_fields(head) if token and not SETTING_RE.match(token)]
        if candidates:
            review_field_reference(candidates[-1], current_fields, unknown_fields, canonical_issues)
        return

    if command_name == "chart":
        panel_match = re.search(r"panel=['\"]?([A-Za-z]+)['\"]?", body)
        if panel_match and panel_match.group(1) not in registry.chart_panel_types:
            unknown_chart_panels.add(panel_match.group(1))
        agg_match = re.search(r"agg=([A-Za-z_][A-Za-z0-9_]*)\(", body)
        if agg_match and agg_match.group(1) not in registry.stats_functions:
            unknown_functions.add(agg_match.group(1))
        return

    if command_name == "xyseries":
        tokens = [token for token in split_fields(body) if token and not SETTING_RE.match(token)]
        for field in tokens[:3]:
            review_field_reference(field, current_fields, unknown_fields, canonical_issues)
        return

    if command_name == "autoregress":
        register_aliases(body, derived_fields)
        candidates = [token for token in split_fields(body) if token and not re.fullmatch(r"\d+(?:-\d+)?", token)]
        if candidates:
            review_field_reference(candidates[0], current_fields, unknown_fields, canonical_issues)
        return

    if command_name == "eval":
        for assignment in body.split(";"):
            left, _, _ = assignment.partition("=")
            name = left.strip()
            if re.fullmatch(r"[A-Za-z0-9_\u4e00-\u9fff]+", name):
                derived_fields.add(name)
        return

    if command_name == "join":
        _, join_clause = split_by_keyword(body, "where")
        join_expression = join_clause.split("__subquery_", 1)[0].strip()
        if join_expression:
            review_expression(
                join_expression,
                current_fields,
                field_types,
                field_arrays,
                belong_rhs_fields,
                registry.allowed_expression_operators,
                unknown_fields,
                unknown_operators,
                unknown_functions,
                canonical_issues,
            )
        return

    if command_name in COMMANDS_WITHOUT_FIELD_CHECK:
        return
    notes.append(f"命令 {command_name} 已识别，但当前审查器未对其参数做字段级校验。")


def flatten_reports_ok(reports: Iterable[ReviewReport]) -> bool:
    """判断所有 nested subquery 报告是否都通过。"""
    return all(report.ok for report in reports)


def split_rendered_queries(hql: str) -> list[str]:
    """把可能包含多条查询的渲染文本拆开。"""
    parts = [part.strip() for part in MULTI_QUERY_SPLIT_RE.split((hql or "").strip()) if part.strip()]
    return parts or [""]


def collect_where_bodies(ast: PipelineAst) -> list[str]:
    """递归收集当前 HQL 中所有 where 表达式。"""
    bodies: list[str] = []
    for segment in ast.segments:
        if segment.command == "where" and segment.body:
            bodies.append(segment.body)
        for subquery in segment.subqueries:
            bodies.extend(collect_where_bodies(subquery))
    return bodies


def token_to_field_identifier(token: str, catalog: dict[str, dict[str, dict[str, object]]]) -> str:
    """把字段 token 归一成优先使用 field_key 的稳定标识。"""
    normalized = normalize_field_token(token)
    if normalized in catalog["by_name"]:
        return str(catalog["by_name"][normalized].get("key", normalized))
    if normalized in catalog["by_key"]:
        return normalized
    return normalized


def parse_literal_token(token: str, *, literal_format: str = "default") -> object:
    """把表达式里的值字面量归一成 Python 值。"""
    text = token.strip()
    if literal_format == "regex_literal":
        return normalize_regex_pattern(text)
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    if re.fullmatch(r"\d+", text):
        return int(text)
    return text


def normalize_atom(predicate: BoundPredicateAtom) -> tuple[str, str, str, bool]:
    """把已绑定原子谓词归一成 reviewer 使用的结构化签名。"""
    rhs_payload: dict[str, object] = {}
    if predicate.rhs_field or predicate.rhs_field_key:
        rhs_payload["rhs_field"] = predicate.rhs_field_key or predicate.rhs_field
    elif predicate.values:
        rhs_payload["rhs_values"] = list(predicate.values)
    else:
        rhs_payload["rhs_value"] = predicate.value
    return (
        predicate.field_key or predicate.field,
        predicate.operator,
        canonical_value(rhs_payload),
        bool(predicate.negated),
    )


def normalize_expected_predicate(predicate: BoundPredicate) -> tuple[object, ...]:
    """把 authoritative 谓词归一成结构化形式。"""
    if isinstance(predicate, BoundPredicateGroup):
        return ("group", predicate.mode, frozenset(normalize_atom(child) for child in predicate.predicates if child.field))
    return ("atom",) + normalize_atom(predicate)


def parse_normalized_atom(text: str, catalog: dict[str, dict[str, dict[str, object]]]) -> tuple[object, ...] | None:
    """把一段原子表达式解析成结构化签名。"""
    registry = load_operator_registry()
    comparisons = iter_comparisons(text.strip())
    if len(comparisons) == 1:
        comparison = comparisons[0]
        field_token = comparison.field_token
        operator = comparison.operator
        capability = registry.predicate_capability(operator)
        if capability["rhs_kind"] == "field":
            rhs_token = comparison.value_token.strip()
            if not FIELD_TOKEN_RE.fullmatch(rhs_token):
                return None
            rhs_payload = {"rhs_field": token_to_field_identifier(rhs_token, catalog)}
        else:
            rhs_payload = {"rhs_value": parse_literal_token(comparison.value_token, literal_format=capability["rhs_literal_format"])}
        return ("atom", token_to_field_identifier(field_token, catalog), operator, canonical_value(rhs_payload), False)

    function_calls = iter_function_calls(text.strip())
    if len(function_calls) != 1:
        return None
    function_call = function_calls[0]
    capability = registry.predicate_capability(function_call.name)
    if capability["render_kind"] != "function_field_list":
        return None
    args_list = split_function_args(function_call.args_text.replace("，", ","))
    if len(args_list) != 2:
        return None
    field_arg = args_list[0].strip()
    if not FIELD_TOKEN_RE.fullmatch(field_arg):
        return None
    try:
        parsed_values = ast.literal_eval(args_list[1].strip())
    except (ValueError, SyntaxError):
        return None
    if not isinstance(parsed_values, list):
        return None
    return (
        "atom",
        token_to_field_identifier(field_arg, catalog),
        function_call.name,
        canonical_value({"rhs_values": parsed_values}),
        False,
    )


def parse_normalized_group(text: str, catalog: dict[str, dict[str, dict[str, object]]]) -> tuple[object, ...] | None:
    """把单层 AND/OR 组解析成顺序无关的结构化签名。"""
    inner = strip_outer_parens(text)
    if inner == text.strip():
        return None
    for mode, keyword in (("any_of", "or"), ("all_of", "and")):
        parts = split_top_level_boolean(inner, keyword)
        if len(parts) <= 1:
            continue
        atoms = [parse_normalized_atom(part.strip(), catalog) for part in parts]
        if any(item is None for item in atoms):
            return None
        return ("group", mode, frozenset(item[1:] for item in atoms if item))
    return None


def collect_normalized_where_predicates(
    ast: PipelineAst,
    catalog: dict[str, dict[str, dict[str, object]]],
) -> list[set[tuple[object, ...]]]:
    """把 AST 中所有 where 表达式解析成结构化谓词集合。"""
    predicate_sets: list[set[tuple[object, ...]]] = []
    for body in collect_where_bodies(ast):
        sanitized = sanitize_expression(body)
        predicates: set[tuple[object, ...]] = set()
        for part in split_top_level_boolean(sanitized, "and"):
            item = part.strip()
            if not item:
                continue
            group = parse_normalized_group(item, catalog)
            if group:
                predicates.add(group)
                continue
            atom = parse_normalized_atom(item, catalog)
            if atom:
                predicates.add(atom)
        predicate_sets.append(predicates)
    return predicate_sets


def expected_shapes_for_intent(intent: CompiledIntent) -> set[str]:
    """根据内部意图推断可接受的最终查询形态。"""
    if intent.result_policy == "explicit_multi_result":
        return {"multi_result"}
    if any(step.kind == "subquery_filter" for step in intent.derivations):
        if any(output.role == "metric" for output in intent.final_outputs):
            return {"derived_filter_aggregate"}
        return {"derived_filter_detail"}
    if any(step.kind in {"top_n", "ranking"} for step in intent.derivations):
        return {"ranking_top_n"}
    if any(output.role == "metric" for output in intent.final_outputs):
        if any(output.role == "dimension" for output in intent.final_outputs):
            if intent.limit is not None:
                return {"aggregate_top_k"}
            return {"aggregate_grouped"}
        return {"aggregate_total"}
    return {"detail_query"}


def semantic_consistency_checks(
    intent: CompiledIntent | None,
    ast: PipelineAst,
    shape: str,
    rendered: str,
    catalog: dict[str, dict[str, dict[str, object]]],
) -> tuple[list[str], list[str]]:
    """基于已绑定意图做语义一致性审查。"""
    issues: list[str] = []
    warnings: list[str] = []
    if not intent:
        return issues, warnings

    if shape not in expected_shapes_for_intent(intent):
        add_issue(issues, "result_shape_mismatch")
        add_issue(warnings, "result_shape_mismatch")

    where_predicates = collect_normalized_where_predicates(ast, catalog)
    for predicate in intent.authoritative_predicates():
        expected = normalize_expected_predicate(predicate)
        if not any(expected in items for items in where_predicates):
            add_issue(issues, "authoritative_semantic_rule_ignored")
            add_issue(warnings, "authoritative_semantic_rule_ignored")

    final_fields = set(intent.final_output_fields())
    if (
        intent.result_policy != "explicit_multi_result"
        and final_fields
        and shape in {"aggregate_grouped", "aggregate_top_k", "ranking_top_n", "derived_filter_aggregate"}
        and not all(field in rendered for field in final_fields)
    ):
        add_issue(warnings, "derivation_exposed_as_final_output")

    return issues, warnings


def build_report(ast: PipelineAst, *, source: str, intent: CompiledIntent | None = None) -> ReviewReport:
    """递归构建单条 AST 的审查报告。"""
    catalog = load_catalog(source)
    available_fields = set(catalog["by_name"]) | set(catalog["by_key"])
    field_types = {
        name: field.get("type", "")
        for name, field in {**catalog["by_name"], **catalog["by_key"]}.items()
    }
    field_arrays = {
        name: bool(field.get("array", False))
        for name, field in {**catalog["by_name"], **catalog["by_key"]}.items()
    }
    belong_rhs_fields = {
        name: bool(field.get("belong_rhs", False))
        for name, field in {**catalog["by_name"], **catalog["by_key"]}.items()
    }
    canonical_issues: list[str] = []
    unknown_fields: set[str] = set()
    unknown_commands: set[str] = set()
    unknown_operators: set[str] = set()
    unknown_functions: set[str] = set()
    unknown_chart_panels: set[str] = set()
    notes: list[str] = []
    derived_fields: set[str] = set()
    nested_reports: list[ReviewReport] = []

    rendered = render_pipeline(ast)
    validate_global_rules(rendered, canonical_issues)
    shape = validate_pipeline_structure(ast, source, canonical_issues, intent=intent)

    for segment in ast.segments:
        if "__subquery_" in segment.body and not segment.subqueries:
            add_issue(canonical_issues, "nested_subquery_invalid")
        for subquery in segment.subqueries:
            nested_source = source
            if subquery.index:
                try:
                    nested_source = source_for_index(subquery.index)
                except ValueError:
                    add_issue(canonical_issues, "nested_subquery_invalid")
            nested_report = build_report(subquery, source=nested_source)
            nested_reports.append(nested_report)
            if not nested_report.ok:
                add_issue(canonical_issues, "nested_subquery_invalid")
        review_segment(
            segment.command,
            segment.body,
            catalog=catalog,
            available_fields=available_fields,
            field_types=field_types,
            field_arrays=field_arrays,
            belong_rhs_fields=belong_rhs_fields,
            derived_fields=derived_fields,
            unknown_fields=unknown_fields,
            unknown_commands=unknown_commands,
            unknown_operators=unknown_operators,
            unknown_functions=unknown_functions,
            unknown_chart_panels=unknown_chart_panels,
            canonical_issues=canonical_issues,
            notes=notes,
        )

    semantic_issues, strategy_warnings = semantic_consistency_checks(intent, ast, shape, rendered, catalog)
    for issue in semantic_issues:
        add_issue(canonical_issues, issue)
    if "nested_subquery_invalid" in canonical_issues:
        add_issue(strategy_warnings, "nested_subquery_invalid")
    ok = not any(
        [
            canonical_issues,
            unknown_fields,
            unknown_commands,
            unknown_operators,
            unknown_functions,
            unknown_chart_panels,
        ]
    ) and flatten_reports_ok(nested_reports)

    return ReviewReport(
        source=source,
        shape=shape,
        ok=ok,
        canonical_issues=canonical_issues,
        unknown_fields=sorted(unknown_fields),
        unknown_commands=sorted(unknown_commands),
        unknown_operators=sorted(unknown_operators),
        unknown_functions=sorted(unknown_functions),
        unknown_chart_panels=sorted(unknown_chart_panels),
        nested_reports=nested_reports,
        strategy_warnings=strategy_warnings,
        notes=notes,
    )


def review_plan(source: str, ast_list: list[PipelineAst], *, intent: CompiledIntent | None = None) -> ReviewReport:
    """直接审查 planner 产出的 AST，避免主路径重复 parse。"""
    if len(ast_list) == 1:
        return build_report(ast_list[0], source=source, intent=intent)

    nested_reports = [build_report(ast, source=source) for ast in ast_list]
    canonical_issues: list[str] = []
    strategy_warnings: list[str] = []
    if not intent or intent.result_policy != "explicit_multi_result":
        add_issue(canonical_issues, "unexpected_multi_result_output")
        add_issue(strategy_warnings, "unexpected_multi_result_output")

    return ReviewReport(
        source=source,
        shape="multi_result",
        ok=not canonical_issues and flatten_reports_ok(nested_reports),
        canonical_issues=canonical_issues,
        nested_reports=nested_reports,
        strategy_warnings=strategy_warnings,
    )


def review_hql(source: str, hql: str, *, intent: CompiledIntent | None = None) -> ReviewReport:
    """审查一条现成 HQL，并返回结构化报告。"""
    queries = split_rendered_queries(hql)
    return review_plan(source, [parse_hql(item.strip()) for item in queries], intent=intent)
