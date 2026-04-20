"""where/search 表达式的轻量解析工具。

这个模块故意只覆盖 reviewer 需要的一个很小子集：
- 标识符
- 字符串字面量
- 数值
- regex 字面量
- now(...)
- 比较运算符
- and / or / not
- 顶层括号

它不是通用 HQL 解析器；目标只是让 reviewer 能在不误伤字符串内容的前提下，
做结构化的 operator、comparison 和 boolean group 审查。
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class ExpressionToken:
    """表达式里的一个稳定 token。"""

    kind: str
    text: str
    start: int
    end: int


@dataclass(frozen=True)
class ParsedComparison:
    """一段最小比较表达式。"""

    field_token: str
    operator: str
    value_token: str


@dataclass(frozen=True)
class ParsedFunctionCall:
    """表达式里出现的函数调用 token。"""

    name: str
    args_text: str


IDENTIFIER_CHARS = set("._") | {chr(codepoint) for codepoint in range(ord("0"), ord("9") + 1)} | {
    chr(codepoint) for codepoint in range(ord("A"), ord("Z") + 1)
} | {chr(codepoint) for codepoint in range(ord("a"), ord("z") + 1)} | {
    chr(codepoint) for codepoint in range(0x4E00, 0x9FFF + 1)
}
SYMBOLIC_OPERATOR_CHARS = set("!<>=~")
LOGICAL_KEYWORDS = {"and", "or", "not"}
VALUE_TOKEN_KINDS = {"identifier", "string", "number", "regex", "function", "placeholder"}
STRUCTURAL_PUNCTUATION_MAP = {
    "，": ",",
    "（": "(",
    "）": ")",
    "［": "[",
    "］": "]",
}


@lru_cache(maxsize=1)
def text_operators() -> set[str]:
    """返回表达式里按文本 token 出现的操作符集合。"""
    from .operators import load_operator_registry

    return load_operator_registry().text_predicate_operators


def _is_identifier_char(char: str) -> bool:
    return char in IDENTIFIER_CHARS


def _read_quoted(text: str, start: int) -> int:
    quote = text[start]
    index = start + 1
    while index < len(text):
        char = text[index]
        if char == "\\":
            index += 2
            continue
        if char == quote:
            return index + 1
        index += 1
    return len(text)


def _read_regex(text: str, start: int) -> int:
    index = start + 1
    while index < len(text):
        char = text[index]
        if char == "\\":
            index += 2
            continue
        if char == "/":
            return index + 1
        index += 1
    return len(text)


def _read_function(text: str, start: int) -> int:
    index = start
    while index < len(text) and _is_identifier_char(text[index]):
        index += 1
    if index >= len(text) or text[index] != "(":
        return index
    depth = 0
    while index < len(text):
        char = text[index]
        if char in {"'", '"'}:
            index = _read_quoted(text, index)
            continue
        if char == "/":
            index = _read_regex(text, index)
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return len(text)


def normalize_expression_syntax(text: str) -> str:
    """把表达式中的全角结构符号归一成 ASCII，忽略字符串和 regex 内部内容。"""
    chars: list[str] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char in {"'", '"'}:
            end = _read_quoted(text, index)
            chars.append(text[index:end])
            index = end
            continue
        if char == "/":
            end = _read_regex(text, index)
            chars.append(text[index:end])
            index = end
            continue
        chars.append(STRUCTURAL_PUNCTUATION_MAP.get(char, char))
        index += 1
    return "".join(chars)


def lex_expression(text: str) -> list[ExpressionToken]:
    """把表达式切成 quote-aware 的 token 流。"""
    text = normalize_expression_syntax(text)
    tokens: list[ExpressionToken] = []
    index = 0
    while index < len(text):
        char = text[index]
        if char.isspace():
            index += 1
            continue
        if char in {"(", ")"}:
            tokens.append(ExpressionToken("paren", char, index, index + 1))
            index += 1
            continue
        if char in {"'", '"'}:
            end = _read_quoted(text, index)
            tokens.append(ExpressionToken("string", text[index:end], index, end))
            index = end
            continue
        if char == "/":
            end = _read_regex(text, index)
            tokens.append(ExpressionToken("regex", text[index:end], index, end))
            index = end
            continue
        if char in SYMBOLIC_OPERATOR_CHARS:
            end = index + 1
            while end < len(text) and text[end] in SYMBOLIC_OPERATOR_CHARS:
                end += 1
            tokens.append(ExpressionToken("operator", text[index:end], index, end))
            index = end
            continue
        if char.isdigit():
            end = index + 1
            while end < len(text) and text[end].isdigit():
                end += 1
            tokens.append(ExpressionToken("number", text[index:end], index, end))
            index = end
            continue
        if _is_identifier_char(char):
            end = index + 1
            while end < len(text) and _is_identifier_char(text[end]):
                end += 1
            word = text[index:end]
            lowered = word.lower()
            if end < len(text) and text[end] == "(":
                function_end = _read_function(text, index)
                tokens.append(ExpressionToken("function", text[index:function_end], index, function_end))
                index = function_end
                continue
            if word.startswith("__subquery_"):
                tokens.append(ExpressionToken("placeholder", word, index, end))
            elif lowered in LOGICAL_KEYWORDS:
                tokens.append(ExpressionToken("logical", lowered, index, end))
            elif lowered in text_operators():
                tokens.append(ExpressionToken("operator", lowered, index, end))
            else:
                tokens.append(ExpressionToken("identifier", word, index, end))
            index = end
            continue
        tokens.append(ExpressionToken("symbol", char, index, index + 1))
        index += 1
    return tokens


def iter_function_calls(text: str) -> list[ParsedFunctionCall]:
    """提取表达式里真正处于 token 层的函数调用。"""
    calls: list[ParsedFunctionCall] = []
    for token in lex_expression(text):
        if token.kind != "function":
            continue
        head, _, tail = token.text.partition("(")
        args_text = tail[:-1] if tail.endswith(")") else tail
        calls.append(ParsedFunctionCall(name=head, args_text=args_text))
    return calls


def split_function_args(text: str) -> list[str]:
    """按顶层逗号切分函数参数，忽略字符串、数组和括号内部的逗号。"""
    text = normalize_expression_syntax(text)
    items: list[str] = []
    start = 0
    depth_round = 0
    depth_square = 0
    index = 0
    while index < len(text):
        char = text[index]
        if char in {"'", '"'}:
            index = _read_quoted(text, index)
            continue
        if char == "/":
            index = _read_regex(text, index)
            continue
        if char == "(":
            depth_round += 1
        elif char == ")":
            depth_round = max(0, depth_round - 1)
        elif char == "[":
            depth_square += 1
        elif char == "]":
            depth_square = max(0, depth_square - 1)
        elif char == "," and depth_round == 0 and depth_square == 0:
            item = text[start:index].strip()
            if item:
                items.append(item)
            start = index + 1
        index += 1
    tail = text[start:].strip()
    if tail:
        items.append(tail)
    return items


def has_single_equals_operator(text: str) -> bool:
    """判断表达式里是否真的出现了单独的 `=` 运算符。"""
    return any(token.kind == "operator" and token.text == "=" for token in lex_expression(text))


def has_now_slash_syntax(text: str) -> bool:
    """判断表达式里是否真的出现了非法的 `now/` 语法。"""
    tokens = lex_expression(text)
    for index in range(len(tokens) - 1):
        left = tokens[index]
        right = tokens[index + 1]
        if left.kind != "identifier" or left.text.lower() != "now":
            continue
        if right.kind == "regex" and right.text.startswith("/"):
            return True
        if right.kind == "symbol" and right.text == "/":
            return True
    return False


def find_unknown_symbolic_operators(text: str, allowed_operators: set[str]) -> set[str]:
    """找出所有未知的符号型 operator。"""
    unknown: set[str] = set()
    for token in lex_expression(text):
        if token.kind != "operator":
            continue
        if token.text in text_operators():
            continue
        if token.text not in allowed_operators:
            unknown.add(token.text)
    return unknown


def iter_word_operator_pairs(text: str) -> list[tuple[str, str]]:
    """找出 `field contains value` 这类文本 operator 形态。"""
    pairs: list[tuple[str, str]] = []
    tokens = lex_expression(text)
    for index in range(len(tokens) - 2):
        left, operator, value = tokens[index : index + 3]
        if left.kind != "identifier":
            continue
        if operator.kind != "identifier":
            continue
        if operator.text.lower() in LOGICAL_KEYWORDS:
            continue
        if value.kind not in VALUE_TOKEN_KINDS:
            continue
        pairs.append((left.text, operator.text))
    return pairs


def iter_comparisons(text: str) -> list[ParsedComparison]:
    """从表达式里提取最小比较表达式。"""
    comparisons: list[ParsedComparison] = []
    tokens = lex_expression(text)
    for index in range(len(tokens) - 2):
        left, operator, value = tokens[index : index + 3]
        if left.kind not in {"identifier", "placeholder"}:
            continue
        if operator.kind != "operator":
            continue
        if value.kind not in VALUE_TOKEN_KINDS:
            continue
        comparisons.append(ParsedComparison(field_token=left.text, operator=operator.text, value_token=value.text))
    return comparisons


def split_top_level_boolean(text: str, keyword: str) -> list[str]:
    """按顶层 and/or 切开表达式，忽略括号和字面量里的关键字。"""
    tokens = lex_expression(text)
    parts: list[str] = []
    segment_start = 0
    depth = 0
    for token in tokens:
        if token.kind == "paren":
            depth += 1 if token.text == "(" else -1
            continue
        if depth == 0 and token.kind == "logical" and token.text == keyword:
            segment = text[segment_start:token.start].strip()
            if segment:
                parts.append(segment)
            segment_start = token.end
    tail = text[segment_start:].strip()
    if tail:
        parts.append(tail)
    return parts


def strip_outer_parens(text: str) -> str:
    """如果表达式被一对完整最外层括号包住，则去掉这一层。"""
    candidate = text.strip()
    while True:
        tokens = lex_expression(candidate)
        if len(tokens) < 2:
            return candidate
        if tokens[0].kind != "paren" or tokens[0].text != "(":
            return candidate
        if tokens[-1].kind != "paren" or tokens[-1].text != ")":
            return candidate
        depth = 0
        closes_at_end = False
        balanced = True
        for token in tokens:
            if token.kind != "paren":
                continue
            depth += 1 if token.text == "(" else -1
            if depth < 0:
                balanced = False
                break
            if depth == 0:
                closes_at_end = token.end == len(candidate)
                if not closes_at_end:
                    balanced = False
                    break
        if not balanced or not closes_at_end:
            return candidate
        candidate = candidate[1:-1].strip()
