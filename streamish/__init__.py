"""Iterator and async iterator utilities.

Each operation exists as a function that takes the iterable as an argument,
and as a chainable method on `Stream`. Most operations accept both sync and
async iterables: an async input gives an async result, and some operations,
such as `map` with a coroutine function, also turn a sync input async.

Examples
--------
>>> import streamish as st
>>> list(st.take(2, st.filter(lambda x: x > 4, st.map(lambda x: x * 2, range(10)))))
[6, 8]
>>> list(st.stream(range(10)).map(lambda x: x * 2).filter(lambda x: x > 4).take(2))
[6, 8]
"""

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
    """Wrap `source` in a `Stream` for chaining operations.

    Parameters
    ----------
    source
        The elements to stream, sync or async.

    Returns
    -------
    Stream[T]
        A lazy stream over `source`.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.stream("abc").map(str.upper))
    ['A', 'B', 'C']
    """
    return Stream(source)
