"""统一请求加载入口。

路由规则：
- `request_version=1` -> 当前 public contract
- `schema_version=1` -> 当前 compiler-facing internal request
- 无版本：
  - 如果形状像当前 public request，则按当前 v1 解析并自动补全版本
  - 再否则给出明确错误
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .compile_request import FinalResultSpec, LoadedRequest, MetricSpec, ParsedRequest, RequestParseError, TopKSpec
from .contracts import parse_request_json as parse_internal_request_json
from .public_request import parse_public_request_payload, public_skeleton

PUBLIC_REQUEST_KEYS = {
    "source",
    "time",
    "semantic_macros",
    "semantic_filters",
    "field_filters",
    "result",
    "show_intermediate",
    "raw_query",
}
INTERNAL_REQUEST_KEYS = {
    "final_result",
    "time_range",
    "field_constraints",
    "semantic_macro_ids",
    "semantic_constraints",
}


def normalize_version_token(value: Any) -> Any:
    """把 `v1` / `v2` 这类版本写法归一成整数。"""
    if isinstance(value, str):
        token = value.strip().lower().removeprefix("v")
        if token.isdigit():
            return int(token)
    return value


def looks_like_public_request(payload: dict[str, Any]) -> bool:
    """判断一个无版本 payload 是否更像当前公共请求。"""
    return any(key in payload for key in PUBLIC_REQUEST_KEYS) and not any(key in payload for key in INTERNAL_REQUEST_KEYS)


def missing_version_error(payload: dict[str, Any]) -> RequestParseError:
    """构造缺少版本字段时的可操作错误。"""
    if looks_like_public_request(payload):
        return RequestParseError(
            "missing_request_version",
            "请求缺少版本字段；公共主路径当前使用 `request_version=1`。",
            unsupported_spans=["request"],
            suggestions=[
                "如果你在写公共请求，可以直接补 `request_version=1`。",
                "也可以先运行 `python3 scripts/main.py skeleton detail` 查看当前 skeleton。",
            ],
            suggested_shape="推荐先使用 `main.py skeleton detail|aggregate-total|aggregate-grouped|aggregate-topk|detail-topk` 输出当前 v1 skeleton。",
            example_request=public_skeleton("detail"),
        )
    return RequestParseError(
        "missing_request_version",
        "请求缺少版本字段。",
        unsupported_spans=["request"],
        suggestions=[
            "公共请求请填写 `request_version=1`。",
            "如果你在写 compiler-facing internal request，请显式填写 `schema_version=1`。",
        ],
        suggested_shape="推荐先使用 `main.py skeleton detail` 输出当前 v1 skeleton。",
        example_request=public_skeleton("detail"),
    )


def parse_request_payload(payload: dict[str, Any]) -> LoadedRequest:
    """根据版本字段把 payload 路由到对应解析器。"""
    normalized = dict(payload)
    if "request_version" in normalized:
        normalized["request_version"] = normalize_version_token(normalized["request_version"])
    if "schema_version" in normalized:
        normalized["schema_version"] = normalize_version_token(normalized["schema_version"])

    if normalized.get("request_version") == 1:
        return parse_public_request_payload(normalized)
    if normalized.get("schema_version") == 1:
        loaded = parse_internal_request_json(json.dumps(normalized, ensure_ascii=False))
        return LoadedRequest(
            request=normalize_internal_request(loaded.request),
            warnings=list(loaded.warnings),
            contract=loaded.contract,
        )
    if "request_version" in normalized or "schema_version" in normalized:
        raise RequestParseError(
            "unsupported_version",
            "当前只支持 `request_version=1`，以及 internal request 的 `schema_version=1`。",
            unsupported_spans=["request"],
            suggestions=["公共主路径请使用 `request_version=1`。", "compiler-facing internal request 请使用 `schema_version=1`。"],
        )
    if looks_like_public_request(normalized):
        return parse_public_request_payload(normalized)
    raise missing_version_error(normalized)


def parse_request_json(text: str) -> LoadedRequest:
    """把 JSON 文本解析成统一 LoadedRequest。"""
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
        raise RequestParseError("invalid_request", "顶层请求必须是对象。", unsupported_spans=["request"])
    return parse_request_payload(payload)


def parse_request_file(path: str | Path) -> LoadedRequest:
    """从文件读取并解析请求。"""
    request_path = Path(path)
    if not request_path.exists():
        raise RequestParseError(
            "request_file_not_found",
            f"请求文件不存在：{request_path}",
            unsupported_spans=[str(request_path)],
        )
    return parse_request_json(request_path.read_text(encoding="utf-8"))


def request_skeleton(name: str) -> dict[str, Any]:
    """返回当前 v1 官方 skeleton。"""
    return public_skeleton(name)


def normalize_internal_metric(metric: Any) -> MetricSpec:
    """把 internal parser 产出的旧 metric 结构归一成当前 internal metric。"""
    if metric is None:
        return MetricSpec()
    return MetricSpec(
        function=str(getattr(metric, "function", "") or ""),
        field=str(getattr(metric, "field", "") or ""),
        alias=str(getattr(metric, "alias", "") or ""),
    )


def normalize_internal_request(request: Any) -> ParsedRequest:
    """把 internal parser 产出的请求压成当前 internal 结果代数。"""
    final_result = getattr(request, "final_result", None)
    if final_result is None or hasattr(final_result, "type"):
        return request

    kind = str(getattr(final_result, "kind", "detail") or "detail")
    field_phrases = list(getattr(final_result, "field_phrases", []) or [])
    group_by_phrase = str(getattr(final_result, "group_by_phrase", "") or "")
    metric = normalize_internal_metric(getattr(final_result, "metric", None))
    top_k = None

    if kind == "detail":
        selector = getattr(final_result, "selector", None)
        if selector is not None:
            group_by_phrase = str(getattr(selector, "group_by_phrase", "") or "")
            metric = normalize_internal_metric(getattr(selector, "metric", None))
            top_k = TopKSpec(
                limit=max(1, int(getattr(selector, "limit", 1) or 1)),
                direction=str(getattr(selector, "direction", "desc") or "desc"),
            )
        result_type = "detail"
    elif kind == "group_count":
        result_type = "aggregate"
    elif kind == "ranking":
        result_type = "aggregate"
        if not metric.function:
            metric = MetricSpec(function="count", field="ID", alias="数量")
        limit = getattr(getattr(request, "detail_limit", None), "value", None)
        top_k = TopKSpec(limit=max(1, int(limit or 20)), direction=str(getattr(getattr(request, "sort", None), "direction", "desc") or "desc"))
    else:
        result_type = "detail"

    return ParsedRequest(
        schema_version=1,
        source=str(getattr(request, "source", "")),
        time_range=dict(getattr(request, "time_range", {}) or {}),
        semantic_macro_ids=list(getattr(request, "semantic_macro_ids", []) or []),
        semantic_constraints=list(getattr(request, "semantic_constraints", []) or []),
        field_constraints=list(getattr(request, "field_constraints", []) or []),
        final_result=FinalResultSpec(
            type=result_type,
            field_phrases=field_phrases,
            group_by_phrase=group_by_phrase,
            metric=metric,
            top_k=top_k,
        ),
        show_intermediate=bool(getattr(request, "show_intermediate", False)),
        detail_limit=getattr(request, "detail_limit", None),
        sort=getattr(request, "sort", None),
        raw_query=str(getattr(request, "raw_query", "") or ""),
    )
