"""Streamish - Iterator and async iterator utilities."""

from collections.abc import AsyncIterable, Iterable

from streamish.ops import (
    batch,
    chain,
    chain_async,
    distinct,
    distinct_by,
    enumerate,
    filter,
    flat_map,
    flatten,
    interleave,
    map,
    map_async,
    merge,
    partition,
    partition_async,
    scan,
    skip,
    skip_while,
    take,
    take_while,
    window,
    zip,
    zip_async,
)
from streamish.stream import Stream

__all__ = [
    "stream",
    "Stream",
    "map",
    "map_async",
    "filter",
    "take",
    "skip",
    "take_while",
    "skip_while",
    "distinct",
    "distinct_by",
    "flatten",
    "flat_map",
    "enumerate",
    "scan",
    "batch",
    "window",
    "partition",
    "partition_async",
    "zip",
    "zip_async",
    "chain",
    "chain_async",
    "interleave",
    "merge",
]


def stream[T](source: Iterable[T] | AsyncIterable[T]) -> Stream[T]:
    """Create a Stream from an iterable or async iterable."""
    return Stream(source)
