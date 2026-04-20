#!/usr/bin/env python3
"""HQL Creator 主入口。

这个脚本是 skill 的唯一正常入口：

1. 编译模式：优先接收 `DraftRequestV1`，执行完整编译链，输出最终 HQL。
2. 调试模式：用 `--json` 输出单次编译的完整调试包。
3. 子命令模式：
   - `lookup-fields`：字段检索调试
   - `review`：复审现成 HQL

这个文件刻意不再接收中文原句，避免把自然语言解析职责塞回 engine。
"""

from __future__ import annotations

import argparse
import json
import sys

from engine.compiler import CompileFailure, compile_parsed_request
from engine.compile_request import LoadedRequest, RequestParseError, StrictSchemaError
from engine.knowledge import compile_intent, search_fields, serialize_search_result, source_file_name, trim_options
from engine.public_request import PUBLIC_SOURCES
from engine.request_loader import parse_request_file, parse_request_json, request_skeleton
from engine.reviewer import review_hql


def localize_argparse() -> None:
    """把 argparse 默认帮助文案切换成中文。"""
    translations = {
        "usage: ": "用法：",
        "positional arguments": "位置参数",
        "options": "可选参数",
        "optional arguments": "可选参数",
        "show this help message and exit": "显示帮助并退出",
        "subcommands": "子命令",
    }
    argparse._ = lambda text: translations.get(text, text)


def print_root_help() -> None:
    """打印主入口的总帮助信息。"""
    print(
        "\n".join(
            [
                "用法：main.py [--json] (--request '<json>' | --request-file <path>)",
                "      main.py lookup-fields <source> <term>",
                "      main.py skeleton <detail|aggregate-total|aggregate-grouped|aggregate-topk|detail-topk>",
                "      main.py review <source> '<现成HQL>' [--request '<json>' | --request-file <path>] [--json]",
                "",
                "说明：",
                "  编译模式优先接收 DraftRequestV1；internal request 使用 `schema_version=1`。",
                "  skeleton 输出官方 DraftRequestV1 骨架，便于先选形态再补字段和值。",
                "  --json 输出单次编译的完整调试包。",
                "  lookup-fields 和 review 只用于诊断。",
                "  source 推荐写中文标准值：日志、告警、原始告警。",
            ]
        )
    )


def build_compile_parser() -> argparse.ArgumentParser:
    """构建默认编译模式的参数解析器。"""
    parser = argparse.ArgumentParser(description="把 DraftRequestV1 编译成最终 HQL。")
    parser.add_argument("--request", help="内联传入的 DraftRequestV1 JSON")
    parser.add_argument("--request-file", help="从文件读取 DraftRequestV1 JSON")
    parser.add_argument("--json", action="store_true", help="输出单次编译的完整调试包")
    return parser


def build_lookup_parser() -> argparse.ArgumentParser:
    """构建字段检索子命令解析器。"""
    parser = argparse.ArgumentParser(description="检索字段元数据。")
    parser.add_argument("source", nargs="?", help="查询源，推荐写：日志、告警、原始告警")
    parser.add_argument("query", nargs="*", help="字段关键词，可使用中文、字段 key、拼音或常见别名")
    parser.add_argument("--all", action="store_true", help="跨全部查询源检索")
    parser.add_argument("--limit", type=int, default=8, help="最多输出多少条结果")
    parser.add_argument("--show-options", action="store_true", help="展示枚举字段的选项预览")
    parser.add_argument("--json", action="store_true", help="输出机器可读的 JSON 结果")
    return parser


def build_review_parser() -> argparse.ArgumentParser:
    """构建现成 HQL 复审子命令解析器。"""
    parser = argparse.ArgumentParser(description="复审现成 HQL。")
    parser.add_argument("source", help="查询源，推荐写：日志、告警、原始告警")
    parser.add_argument("hql", help="待复审的 HQL 文本")
    parser.add_argument("--request", help="可选：内联 DraftRequestV1 JSON，用于语义一致性复审")
    parser.add_argument("--request-file", help="可选：从文件读取 DraftRequestV1 JSON，用于语义一致性复审")
    parser.add_argument("--json", action="store_true", help="输出 JSON 审查报告")
    return parser


def build_skeleton_parser() -> argparse.ArgumentParser:
    """构建 skeleton 子命令解析器。"""
    parser = argparse.ArgumentParser(description="输出官方 DraftRequestV1 skeleton。")
    parser.add_argument("kind", help="可选值：detail、aggregate-total、aggregate-grouped、aggregate-topk、detail-topk")
    parser.add_argument("--json", action="store_true", help="输出 JSON 结果")
    return parser


def load_request_args(inline_request: str | None, request_file: str | None) -> LoadedRequest:
    """按统一规则读取结构化请求。"""
    if bool(inline_request) == bool(request_file):
        raise SystemExit("请二选一提供 `--request` 或 `--request-file`。")
    if inline_request:
        return parse_request_json(inline_request)
    return parse_request_file(request_file or "")


def format_request_parse_error(exc: RequestParseError) -> str:
    """把结构化请求错误压成更可操作的纯文本。"""
    lines = [f"解析失败：{exc.message}"]
    if exc.unknown_keys:
        lines.append("未知字段：" + "、".join(exc.unknown_keys))
    if exc.nearest_valid_keys:
        suggestions = []
        for key, candidates in exc.nearest_valid_keys.items():
            if not candidates:
                continue
            suggestions.append(f"{key} -> {' / '.join(candidates)}")
        if suggestions:
            lines.append("近似合法字段：" + "；".join(suggestions))
    if exc.suggested_shape:
        lines.append("建议形态：" + exc.suggested_shape)
    if exc.suggestions:
        lines.append("建议：" + "；".join(exc.suggestions))
    if exc.example_request is not None:
        lines.append("示例请求：")
        lines.append(json.dumps(exc.example_request, ensure_ascii=False, indent=2))
    return "\n".join(lines)


def normalize_cli_source(source: str) -> str:
    """把用户态中文 source 归一成内部 canonical source。"""
    token = (source or "").strip()
    return PUBLIC_SOURCES.get(token, token)


def print_lookup_results(results: list[dict], show_options: bool, as_json: bool) -> int:
    """按统一格式打印字段检索结果。"""
    if not results:
        if as_json:
            print("[]")
        else:
            print("未找到匹配字段。")
        return 1

    if as_json:
        print(
            json.dumps(
                [serialize_search_result(item, show_options=show_options) for item in results],
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    current_source = None
    for index, item in enumerate(results, start=1):
        source = item["source"]
        if source != current_source:
            if current_source is not None:
                print()
            print(f"source={source} file=references/{source_file_name(source)}")
            current_source = source

        field = item["field"]
        print(
            f"{index}. {field['name']} | key={field['key']} | type={field['type']} | "
            f"pinyin={field.get('pinyin', '')} | array={field.get('array', False)} | "
            f"score={item['score']}"
        )
        payload = serialize_search_result(item, show_options=show_options)
        aliases = payload.get("aliases", [])
        if aliases:
            print(f"   aliases={', '.join(aliases)}")
        if show_options and field.get("options"):
            preview = trim_options(field)
            suffix = " ..." if len(field["options"]) > len(preview) else ""
            print(f"   options={preview}{suffix}")
    return 0


def format_review_report(report: dict) -> str:
    """把结构化复审结果格式化成人类可扫读文本。"""
    lines = [f"ok={str(report['ok']).lower()}", f"shape={report['shape']}"]
    for key in (
        "canonical_issues",
        "unknown_commands",
        "unknown_fields",
        "unknown_operators",
        "unknown_functions",
        "unknown_chart_panels",
        "strategy_warnings",
    ):
        values = report.get(key) or []
        if values:
            lines.append(f"{key}=" + ", ".join(values))
    if report.get("nested_reports"):
        lines.append(f"nested_reports={len(report['nested_reports'])}")
    if report.get("notes"):
        lines.append("notes=" + " | ".join(report["notes"]))
    return "\n".join(lines)


def run_lookup_fields(args: argparse.Namespace) -> int:
    """执行字段检索子命令。"""
    query_parts = list(args.query)
    if args.all and args.source and not query_parts:
        query_parts = [args.source]
        source = None
    else:
        source = args.source

    query_text = " ".join(query_parts).strip()
    if not query_text:
        raise SystemExit("请提供字段检索词。")

    source = normalize_cli_source(source) if source else None
    results = search_fields(source, query_text, use_all=args.all, limit=args.limit)
    return print_lookup_results(results, args.show_options, args.json)


def run_review(args: argparse.Namespace) -> int:
    """执行现成 HQL 的复审子命令。"""
    source = normalize_cli_source(args.source)
    intent = None
    warnings: list[str] = []
    if args.request or args.request_file:
        loaded = load_request_args(args.request, args.request_file)
        request = loaded.request
        warnings = list(loaded.warnings)
        if request.source != source:
            raise StrictSchemaError(
                f"review 子命令的 source=`{args.source}` 与结构化请求中的 source=`{request.source}` 不一致。"
            )
        # review 只需要拿到已绑定约束后的内部意图，用于做语义一致性审查。
        # 这里不重复跑完整编译链，避免无谓的规划、审查和渲染。
        intent = compile_intent(request)

    report = review_hql(source, args.hql.strip(), intent=intent).to_dict()
    if args.json:
        if warnings:
            report["warnings"] = list(warnings)
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        for warning in warnings:
            print(f"warning: {warning}", file=sys.stderr)
        print(format_review_report(report))
    return 0 if report["ok"] else 1


def run_skeleton(args: argparse.Namespace) -> int:
    """输出官方请求骨架。"""
    print(json.dumps(request_skeleton(args.kind), ensure_ascii=False, indent=2))
    return 0


def run_compile(args: argparse.Namespace) -> int:
    """执行默认编译模式。"""
    loaded = load_request_args(args.request, args.request_file)
    compiled = compile_parsed_request(loaded.request)
    if args.json:
        payload = compiled.to_dict()
        if loaded.warnings:
            payload["warnings"] = list(loaded.warnings)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        for warning in loaded.warnings:
            print(f"warning: {warning}", file=sys.stderr)
        print(compiled.rendered_hql)
    return 0


def main() -> int:
    """解析命令行参数并分发到不同模式。"""
    localize_argparse()
    argv = sys.argv[1:]
    if not argv or argv[0] in {"-h", "--help"}:
        print_root_help()
        return 0
    if argv[0] == "lookup-fields":
        args = build_lookup_parser().parse_args(argv[1:])
        args.subcommand = "lookup-fields"
    elif argv[0] == "skeleton":
        args = build_skeleton_parser().parse_args(argv[1:])
        args.subcommand = "skeleton"
    elif argv[0] == "review":
        args = build_review_parser().parse_args(argv[1:])
        args.subcommand = "review"
    else:
        if not argv[0].startswith("-"):
            raise SystemExit("主入口不再接受中文原句，请先生成 DraftRequestV1，再使用 `--request` 或 `--request-file`。")
        args = build_compile_parser().parse_args(argv)
        args.subcommand = None

    try:
        if args.subcommand == "lookup-fields":
            return run_lookup_fields(args)
        if args.subcommand == "skeleton":
            return run_skeleton(args)
        if args.subcommand == "review":
            return run_review(args)
        return run_compile(args)
    except RequestParseError as exc:
        if getattr(args, "json", False):
            print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2))
        else:
            raise SystemExit(format_request_parse_error(exc)) from exc
        return 1
    except CompileFailure as exc:
        if getattr(args, "json", False):
            print(json.dumps(exc.to_dict(), ensure_ascii=False, indent=2))
        else:
            raise SystemExit(f"编译失败：{exc.message}") from exc
        return 1
    except StrictSchemaError as exc:
        if getattr(args, "json", False):
            print(
                json.dumps(
                    {
                        "error_code": "compile_failed",
                        "stage": "binding",
                        "message": str(exc),
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 1
        raise SystemExit(f"编译失败：{exc}") from exc


if __name__ == "__main__":
    sys.exit(main())
