"""公共 V1 时间层。

这个模块只负责两件事：
1. 校验并 lowering V1 canonical 时间对象。
2. 在受控范围内解析公共时间字符串简写。

这里允许使用 `dateparser`，但边界固定为：
- 只处理结构化端点字符串或受控时间简写
- 不读取 `raw_query`
- 不做自由中文时间理解
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

import dateparser

from .internal_types import RequestParseError

LOCAL_TZ = ZoneInfo("Asia/Shanghai")
DATEPARSER_LANGUAGES = ["zh", "en"]
CANONICAL_PRESETS = {"今天", "昨天", "本周", "本月", "今年"}
CANONICAL_RELATIVE_UNITS = {"分钟", "小时", "天", "周", "月", "年"}


def now_local() -> datetime:
    """返回带本地时区的当前时间。"""
    return datetime.now(LOCAL_TZ)


def parse_time_endpoint(text: str, *, path: str) -> str:
    """把公共时间端点字符串解析成稳定时间字面量。"""
    value = str(text or "").strip()
    if not value:
        raise RequestParseError(
            "invalid_time_endpoint",
            f"`{path}` 不能为空。",
            unsupported_spans=[path],
        )
    parsed = dateparser.parse(
        value,
        languages=DATEPARSER_LANGUAGES,
        settings={
            "TIMEZONE": "Asia/Shanghai",
            "TO_TIMEZONE": "Asia/Shanghai",
            "RETURN_AS_TIMEZONE_AWARE": True,
            "RELATIVE_BASE": now_local(),
            "PREFER_LOCALE_DATE_ORDER": True,
        },
    )
    if parsed is None:
        raise RequestParseError(
            "invalid_time_endpoint",
            f"`{path}` 无法解析时间：{value}",
            unsupported_spans=[path],
            suggestions=["请使用类似 `2026-04-18 00:00:00` 或 `2026-04-18T00:00:00+08:00` 的时间端点。"],
        )
    return parsed.astimezone(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")


@dataclass(frozen=True)
class PublicRelativeTime:
    """V1 canonical 相对时间。"""

    unit: Literal["分钟", "小时", "天", "周", "月", "年"]
    value: int


def lower_time_preset(preset: str) -> dict[str, str]:
    """把公共 preset lower 成内部 from/to。"""
    current = now_local()
    if preset == "今天":
        return {"from": "now(d)", "to": "now()"}
    if preset == "昨天":
        return {"from": "now(d-1d)", "to": "now(d)"}
    if preset == "本周":
        offset = current.weekday()
        start = "now(d)" if offset == 0 else f"now(d-{offset}d)"
        return {"from": start, "to": "now()"}
    if preset == "本月":
        offset = current.day - 1
        start = "now(d)" if offset == 0 else f"now(d-{offset}d)"
        return {"from": start, "to": "now()"}
    if preset == "今年":
        offset = int(current.strftime("%j")) - 1
        start = "now(d)" if offset == 0 else f"now(d-{offset}d)"
        return {"from": start, "to": "now()"}
    raise RequestParseError(
        "invalid_time_preset",
        f"不支持的时间预设：`{preset}`。",
        unsupported_spans=["request.time.preset"],
    )


def lower_relative_time(relative: PublicRelativeTime) -> dict[str, str]:
    """把公共 relative lower 成内部 from/to。"""
    current = now_local()
    unit = relative.unit
    value = int(relative.value)
    if value <= 0:
        raise RequestParseError(
            "invalid_time_relative",
            "`request.time.relative.value` 必须是正整数。",
            unsupported_spans=["request.time.relative.value"],
        )
    if unit == "分钟":
        start = (current - timedelta(minutes=value)).strftime("%Y-%m-%d %H:%M:%S")
        return {"from": start, "to": "now()"}
    if unit == "小时":
        return {"from": f"now(h-{value}h)", "to": "now()"}
    if unit == "天":
        return {"from": f"now(d-{value}d)", "to": "now()"}
    if unit == "周":
        return {"from": f"now(d-{value * 7}d)", "to": "now()"}
    if unit == "月":
        target_year = current.year
        target_month = current.month - value
        while target_month <= 0:
            target_month += 12
            target_year -= 1
        day = min(current.day, calendar.monthrange(target_year, target_month)[1])
        start = current.replace(year=target_year, month=target_month, day=day).strftime("%Y-%m-%d %H:%M:%S")
        return {"from": start, "to": "now()"}
    if unit == "年":
        target_year = current.year - value
        day = min(current.day, calendar.monthrange(target_year, current.month)[1])
        start = current.replace(year=target_year, day=day).strftime("%Y-%m-%d %H:%M:%S")
        return {"from": start, "to": "now()"}
    raise RequestParseError(
        "invalid_time_relative",
        f"不支持的相对时间单位：`{unit}`。",
        unsupported_spans=["request.time.relative.unit"],
    )


def lower_between(from_text: str, to_text: str) -> dict[str, str]:
    """把公共 between lower 成内部 from/to。"""
    return {
        "from": parse_time_endpoint(from_text, path="request.time.between.from"),
        "to": parse_time_endpoint(to_text, path="request.time.between.to"),
    }


def parse_compat_time_string(text: str) -> dict[str, object]:
    """把受控时间字符串解析成 V1 canonical 时间对象。"""
    value = str(text or "").strip()
    if not value:
        raise RequestParseError("invalid_time", "`request.time` 不能为空。", unsupported_spans=["request.time"])

    preset_aliases = {
        "today": "今天",
        "今天": "今天",
        "yesterday": "昨天",
        "昨天": "昨天",
        "this_week": "本周",
        "本周": "本周",
        "this_month": "本月",
        "本月": "本月",
        "this_year": "今年",
        "今年": "今年",
    }
    compact = value.lower().replace(" ", "").replace("-", "_")
    if value in preset_aliases:
        return {"preset": preset_aliases[value]}
    if compact in preset_aliases:
        return {"preset": preset_aliases[compact]}

    for separator in (" - ", " ~ ", " 至 ", "~", "至"):
        if separator in value:
            left, right = value.split(separator, 1)
            return {"between": {"from": left.strip(), "to": right.strip()}}

    import re

    match = re.fullmatch(
        r"(?:last|recent|past)_?(\d+)_(minutes?|hours?|days?|weeks?|months?|years?)",
        compact,
    )
    if match:
        amount, unit_token = match.groups()
        unit_map = {
            "minute": "分钟",
            "minutes": "分钟",
            "hour": "小时",
            "hours": "小时",
            "day": "天",
            "days": "天",
            "week": "周",
            "weeks": "周",
            "month": "月",
            "months": "月",
            "year": "年",
            "years": "年",
        }
        return {"relative": {"unit": unit_map[unit_token], "value": int(amount)}}

    raise RequestParseError(
        "invalid_time",
        f"`request.time` 不支持 `{value}`。",
        unsupported_spans=["request.time"],
        suggestions=[
            "V1 canonical 请使用 `preset` / `relative` / `between` 三种结构之一。",
            "受控时间字符串仅支持 today / yesterday / 本周 / 本月 / 今年 / last_30_days 一类闭集写法。",
        ],
    )
