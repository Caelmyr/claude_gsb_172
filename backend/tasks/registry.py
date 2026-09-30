"""Named map/reduce function registry.

Jobs reference a mapper and reducer **by name**; the registry resolves names to
the callables in ``worker.mappers_reducers``.  This indirection is what lets the
Master dispatch a task with only ``{"mapper": "wordcount_mapper", ...}`` — no
code is shipped over the wire, and a subprocess can resolve the function by
importing this module and looking the name up.
"""

from __future__ import annotations

from typing import Any, Callable, Optional

from backend.worker import mappers_reducers as _fn

Mapper = Callable[[list[Any], dict], list[tuple[str, Any]]]
Reducer = Callable[[str, list[Any], dict], dict]

_MAPPERS: dict[str, Mapper] = {}
_REDUCERS: dict[str, Reducer] = {}
_DESCRIPTIONS: dict[str, str] = {}


def register_mapper(name: str, fn: Mapper, description: str = "") -> None:
    _MAPPERS[name] = fn
    _DESCRIPTIONS[name] = description


def register_reducer(name: str, fn: Reducer, description: str = "") -> None:
    _REDUCERS[name] = fn
    _DESCRIPTIONS[name] = description


def get_mapper(name: str) -> Mapper:
    if name not in _MAPPERS:
        raise KeyError(f"unknown mapper: {name!r}; available: {sorted(_MAPPERS)}")
    return _MAPPERS[name]


def get_reducer(name: str) -> Reducer:
    if name not in _REDUCERS:
        raise KeyError(f"unknown reducer: {name!r}; available: {sorted(_REDUCERS)}")
    return _REDUCERS[name]


def has_mapper(name: str) -> bool:
    return name in _MAPPERS


def has_reducer(name: str) -> bool:
    return name in _REDUCERS


def list_mappers() -> list[dict]:
    return [{"name": n, "description": _DESCRIPTIONS.get(n, "")} for n in sorted(_MAPPERS)]


def list_reducers() -> list[dict]:
    return [{"name": n, "description": _DESCRIPTIONS.get(n, "")} for n in sorted(_REDUCERS)]


def list_all() -> dict:
    return {"mappers": list_mappers(), "reducers": list_reducers()}


# ---------------------------------------------------------------------------
# Built-in registration
# ---------------------------------------------------------------------------
register_mapper("wordcount_mapper", _fn.wordcount_mapper, "WordCount 词频统计")
register_mapper("word_length_mapper", _fn.word_length_mapper, "Word length 词长统计")
register_mapper("grep_mapper", _fn.grep_mapper, "Grep 关键词检索")
register_mapper("kv_mapper", _fn.kv_mapper, "Key-value 键值聚合")
register_mapper("distinct_mapper", _fn.distinct_mapper, "Distinct 去重枚举")

register_reducer("count_reducer", _fn.count_reducer, "Count 求和计数")
register_reducer("stats_reducer", _fn.stats_reducer, "Stats 统计(min/max/avg)")
register_reducer("sum_reducer", _fn.sum_reducer, "Sum 求和聚合")
