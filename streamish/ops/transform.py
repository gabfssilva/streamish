"""Transform operations."""

import asyncio
import builtins
from collections import deque
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
)
from typing import overload

from streamish._util import ensure_async_iterator, is_async_iterable, is_awaitable

__all__ = ["map_", "filter_", "enumerate_", "scan", "flatten", "flat_map", "map_async"]


@overload
def map_[T, U](fn: Callable[[T], U], it: Iterable[T]) -> Iterator[U]: ...


@overload
def map_[T, U](fn: Callable[[T], U], it: AsyncIterable[T]) -> AsyncIterator[U]: ...


@overload
def map_[T, U](
    fn: Callable[[T], Awaitable[U]], it: Iterable[T]
) -> AsyncIterator[U]: ...


@overload
def map_[T, U](
    fn: Callable[[T], Awaitable[U]], it: AsyncIterable[T]
) -> AsyncIterator[U]: ...


def map_[T, U](
    fn: Callable[[T], U] | Callable[[T], Awaitable[U]],
    it: Iterable[T] | AsyncIterable[T],
) -> Iterator[U] | AsyncIterator[U]:
    """Apply `fn` to each element.

    The result is a sync iterator only when both `it` and `fn` are sync. If
    `it` is an async iterable or `fn` is a coroutine function, the result is
    an async iterator. Calls to `fn` run one at a time, in source order.

    Parameters
    ----------
    fn
        Function applied to each element. It counts as async only if it is a
        coroutine function (defined with `async def`).
    it
        Source elements.

    Returns
    -------
    Iterator[U] | AsyncIterator[U]
        The results of `fn`, in source order.

    See Also
    --------
    map_async : Run async calls concurrently, preserving order.

    Notes
    -----
    A `lambda` or any other callable that returns a coroutine without being a
    coroutine function is treated as sync: the result yields un-awaited
    coroutines. Use an `async def` function, or `map_async`, which always
    awaits the result of `fn`.

    Examples
    --------
    >>> import asyncio
    >>> import streamish as st
    >>> list(st.map(str.upper, ["a", "b"]))
    ['A', 'B']

    With a coroutine function the result is async:

    >>> async def double(x):
    ...     return x * 2
    >>> async def main():
    ...     return [x async for x in st.map(double, [1, 2, 3])]
    >>> asyncio.run(main())
    [2, 4, 6]
    """
    if is_async_iterable(it) or is_awaitable(fn):
        return _map_async(fn, it)  # type: ignore[arg-type]
    return _map_sync(fn, it)  # type: ignore[arg-type, return-value]


def _map_sync[T, U](fn: Callable[[T], U], it: Iterable[T]) -> Iterator[U]:
    for item in it:
        yield fn(item)


async def _map_async[T, U](
    fn: Callable[[T], U] | Callable[[T], Awaitable[U]],
    it: Iterable[T] | AsyncIterable[T],
) -> AsyncIterator[U]:
    is_fn_async = is_awaitable(fn)
    if is_async_iterable(it):
        async_it: AsyncIterable[T] = it  # type: ignore[assignment]
        async for item in async_it:
            if is_fn_async:
                yield await fn(item)  # type: ignore[misc]
            else:
                yield fn(item)  # type: ignore[misc]
    else:
        sync_it: Iterable[T] = it  # type: ignore[assignment]
        for item in sync_it:
            if is_fn_async:
                yield await fn(item)  # type: ignore[misc]
            else:
                yield fn(item)  # type: ignore[misc]


@overload
def filter_[T](pred: Callable[[T], bool], it: Iterable[T]) -> Iterator[T]: ...


@overload
def filter_[T](pred: Callable[[T], bool], it: AsyncIterable[T]) -> AsyncIterator[T]: ...


def filter_[T](
    pred: Callable[[T], bool],
    it: Iterable[T] | AsyncIterable[T],
) -> Iterator[T] | AsyncIterator[T]:
    """Keep only the elements for which `pred` returns true.

    Parameters
    ----------
    pred
        Predicate called once per element. It must be sync, even when `it` is
        async.
    it
        Source elements. The result is async if `it` is async.

    Returns
    -------
    Iterator[T] | AsyncIterator[T]
        The matching elements, in source order.

    See Also
    --------
    partition : Keep both the matches and the non-matches.
    take_while : Stop at the first element that fails.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.filter(lambda x: x % 2 == 0, range(6)))
    [0, 2, 4]
    """
    if is_async_iterable(it):
        return _filter_async(pred, it)  # type: ignore[arg-type]
    return _filter_sync(pred, it)  # type: ignore[arg-type, return-value]


def _filter_sync[T](pred: Callable[[T], bool], it: Iterable[T]) -> Iterator[T]:
    for item in it:
        if pred(item):
            yield item


async def _filter_async[T](
    pred: Callable[[T], bool], it: AsyncIterable[T]
) -> AsyncIterator[T]:
    async for item in it:
        if pred(item):
            yield item


@overload
def enumerate_[T](it: Iterable[T], start: int = 0) -> Iterator[tuple[int, T]]: ...


@overload
def enumerate_[T](
    it: AsyncIterable[T], start: int = 0
) -> AsyncIterator[tuple[int, T]]: ...


def enumerate_[T](
    it: Iterable[T] | AsyncIterable[T], start: int = 0
) -> Iterator[tuple[int, T]] | AsyncIterator[tuple[int, T]]:
    """Pair each element with its index.

    Parameters
    ----------
    it
        Source elements. The result is async if `it` is async.
    start
        Index of the first element.

    Returns
    -------
    Iterator[tuple[int, T]] | AsyncIterator[tuple[int, T]]
        `(index, element)` pairs, in source order.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.enumerate(["a", "b"], start=1))
    [(1, 'a'), (2, 'b')]
    """
    if is_async_iterable(it):
        return _enumerate_async(it, start)  # type: ignore[arg-type]
    return _enumerate_sync(it, start)  # type: ignore[arg-type, return-value]


def _enumerate_sync[T](it: Iterable[T], start: int) -> Iterator[tuple[int, T]]:
    for i, item in builtins.enumerate(it, start):
        yield (i, item)


async def _enumerate_async[T](
    it: AsyncIterable[T], start: int
) -> AsyncIterator[tuple[int, T]]:
    i = start
    async for item in it:
        yield (i, item)
        i += 1


@overload
def scan[T, U](
    fn: Callable[[U, T], U], it: Iterable[T], *, initial: U
) -> Iterator[U]: ...


@overload
def scan[T, U](
    fn: Callable[[U, T], U], it: AsyncIterable[T], *, initial: U
) -> AsyncIterator[U]: ...


def scan[T, U](
    fn: Callable[[U, T], U], it: Iterable[T] | AsyncIterable[T], *, initial: U
) -> Iterator[U] | AsyncIterator[U]:
    """Emit a running accumulation of the elements.

    Starting from `initial`, each element is combined with the accumulator by
    `fn`, and every new accumulator is emitted. `initial` itself is not
    emitted, so an empty source produces nothing.

    Parameters
    ----------
    fn
        Function taking the accumulator and an element and returning the new
        accumulator.
    it
        Source elements. The result is async if `it` is async.
    initial
        Accumulator before the first element.

    Returns
    -------
    Iterator[U] | AsyncIterator[U]
        One accumulator per element.

    See Also
    --------
    itertools.accumulate : Sync equivalent that also emits `initial` first.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.scan(lambda total, x: total + x, [1, 2, 3], initial=0))
    [1, 3, 6]
    """
    if is_async_iterable(it):
        return _scan_async(fn, it, initial)  # type: ignore[arg-type]
    return _scan_sync(fn, it, initial)  # type: ignore[arg-type, return-value]


def _scan_sync[T, U](
    fn: Callable[[U, T], U], it: Iterable[T], initial: U
) -> Iterator[U]:
    acc = initial
    for item in it:
        acc = fn(acc, item)
        yield acc


async def _scan_async[T, U](
    fn: Callable[[U, T], U], it: AsyncIterable[T], initial: U
) -> AsyncIterator[U]:
    acc = initial
    async for item in it:
        acc = fn(acc, item)
        yield acc


@overload
def flatten[T](it: Iterable[Iterable[T]]) -> Iterator[T]: ...


@overload
def flatten[T](it: AsyncIterable[Iterable[T]]) -> AsyncIterator[T]: ...


def flatten[T](
    it: Iterable[Iterable[T]] | AsyncIterable[Iterable[T]],
) -> Iterator[T] | AsyncIterator[T]:
    """Flatten one level of nesting.

    Parameters
    ----------
    it
        Iterable of iterables. The outer one may be async; the inner ones must
        be sync. The result is async if `it` is async.

    Returns
    -------
    Iterator[T] | AsyncIterator[T]
        The inner elements, in order.

    See Also
    --------
    flat_map : Map each element to an iterable, then flatten.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.flatten([[1, 2], [], [3]]))
    [1, 2, 3]

    Only one level is removed:

    >>> list(st.flatten([[1, [2]], [3]]))
    [1, [2], 3]
    """
    if is_async_iterable(it):
        return _flatten_async(it)  # type: ignore[arg-type]
    return _flatten_sync(it)  # type: ignore[arg-type, return-value]


def _flatten_sync[T](it: Iterable[Iterable[T]]) -> Iterator[T]:
    for inner in it:
        yield from inner


async def _flatten_async[T](it: AsyncIterable[Iterable[T]]) -> AsyncIterator[T]:
    async for inner in it:
        for item in inner:
            yield item


@overload
def flat_map[T, U](fn: Callable[[T], Iterable[U]], it: Iterable[T]) -> Iterator[U]: ...


@overload
def flat_map[T, U](
    fn: Callable[[T], Iterable[U]], it: AsyncIterable[T]
) -> AsyncIterator[U]: ...


def flat_map[T, U](
    fn: Callable[[T], Iterable[U]], it: Iterable[T] | AsyncIterable[T]
) -> Iterator[U] | AsyncIterator[U]:
    """Map each element to an iterable and emit the elements of each.

    Parameters
    ----------
    fn
        Function returning a sync iterable for each element.
    it
        Source elements. The result is async if `it` is async.

    Returns
    -------
    Iterator[U] | AsyncIterator[U]
        The elements of each iterable returned by `fn`, in order.

    See Also
    --------
    flatten : Flatten without mapping.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.flat_map(lambda word: word.split("-"), ["a-b", "c"]))
    ['a', 'b', 'c']
    """
    if is_async_iterable(it):
        return _flat_map_async(fn, it)  # type: ignore[arg-type]
    return _flat_map_sync(fn, it)  # type: ignore[arg-type, return-value]


def _flat_map_sync[T, U](
    fn: Callable[[T], Iterable[U]], it: Iterable[T]
) -> Iterator[U]:
    for item in it:
        yield from fn(item)


async def _flat_map_async[T, U](
    fn: Callable[[T], Iterable[U]], it: AsyncIterable[T]
) -> AsyncIterator[U]:
    async for item in it:
        for result in fn(item):
            yield result


def map_async[T, U](
    fn: Callable[[T], Awaitable[U]],
    it: Iterable[T] | AsyncIterable[T],
    *,
    concurrency: int = 1,
) -> AsyncIterator[U]:
    """Apply `fn` to elements concurrently, emitting results in source order.

    Up to `concurrency` elements are in progress at once. An element stays in
    progress until its result is emitted, so a slow element holds back the
    results after it, and no new element is read from `it` while
    `concurrency` elements are in progress. The result is always async.

    Parameters
    ----------
    fn
        Function returning an awaitable, such as a coroutine function or a
        `lambda` that calls one.
    it
        Source elements, sync or async.
    concurrency
        Maximum number of elements in progress at once. Must be positive.

    Returns
    -------
    AsyncIterator[U]
        The results of `fn`, in source order.

    Raises
    ------
    ValueError
        If `concurrency` is less than 1. Raised when `map_async` is called,
        not when iteration starts.

    See Also
    --------
    map : Apply `fn` to one element at a time.

    Notes
    -----
    If a call to `fn` raises, the exception propagates without waiting for the
    calls before it to finish. Whenever iteration stops, including on an
    exception or when the consumer closes the iterator, calls still in
    progress are cancelled.

    Examples
    --------
    >>> import asyncio
    >>> import streamish as st
    >>> async def double(x):
    ...     await asyncio.sleep(0.01 * (3 - x))  # later elements finish first
    ...     return x * 2
    >>> async def main():
    ...     return [y async for y in st.map_async(double, [1, 2, 3], concurrency=3)]
    >>> asyncio.run(main())
    [2, 4, 6]
    """
    if concurrency < 1:
        raise ValueError("concurrency must be at least 1")
    return _map_async_concurrent(fn, it, concurrency)


async def _map_async_concurrent[T, U](
    fn: Callable[[T], Awaitable[U]],
    it: Iterable[T] | AsyncIterable[T],
    concurrency: int,
) -> AsyncIterator[U]:
    source: AsyncIterator[T] = (
        it.__aiter__()
        if isinstance(it, AsyncIterable)
        else ensure_async_iterator(iter(it))
    )
    queue: deque[asyncio.Future[U]] = deque()
    failed: asyncio.Future[asyncio.Future[U]] = (
        asyncio.get_running_loop().create_future()
    )

    def on_done(future: asyncio.Future[U]) -> None:
        if not (failed.done() or future.cancelled() or future.exception() is None):
            failed.set_result(future)

    async def next_result() -> U:
        await asyncio.wait((queue[0], failed), return_when=asyncio.FIRST_COMPLETED)
        return await (failed.result() if failed.done() else queue[0])

    try:
        async for item in source:
            call = asyncio.ensure_future(fn(item))
            call.add_done_callback(on_done)
            queue.append(call)
            if len(queue) == concurrency:
                # The head leaves the queue only once emitted, so `finally` still
                # cancels and awaits it if iteration stops while waiting on it.
                yield await next_result()
                queue.popleft()
        while queue:
            yield await next_result()
            queue.popleft()
    finally:
        for future in queue:
            future.cancel()
        await asyncio.gather(*queue, return_exceptions=True)
