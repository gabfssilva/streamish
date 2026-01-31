"""Transform operations."""

import asyncio
import builtins
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
    """Apply function to each element."""
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
    """Keep elements that satisfy predicate."""
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
    """Enumerate elements with index."""
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
    """Accumulator that emits intermediate values."""
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
    """Flatten one level of nesting."""
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
    """Map then flatten."""
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


async def map_async[T, U](
    fn: Callable[[T], Awaitable[U]],
    it: Iterable[T] | AsyncIterable[T],
    *,
    concurrency: int = 1,
) -> AsyncIterator[U]:
    """Map with concurrent async execution, preserving order."""
    if concurrency < 1:
        raise ValueError("concurrency must be at least 1")

    ait: AsyncIterator[T]
    if is_async_iterable(it):
        ait = it.__aiter__()  # type: ignore[union-attr]
    else:
        sync_it: Iterable[T] = it  # type: ignore[assignment]
        ait = ensure_async_iterator(iter(sync_it))

    pending: dict[int, asyncio.Task[U]] = {}
    next_idx = 0
    emit_idx = 0
    results: dict[int, U] = {}
    exhausted = False

    async def get_next() -> T:
        return await ait.__anext__()

    async def fetch_input() -> tuple[int, T] | None:
        nonlocal next_idx, exhausted
        try:
            item = await get_next()
            idx = next_idx
            next_idx += 1
            return (idx, item)
        except StopAsyncIteration:
            exhausted = True
            return None

    async def apply_fn(item: T) -> U:
        return await fn(item)

    # Fill initial slots
    for _ in range(concurrency):
        inp = await fetch_input()
        if inp is None:
            break
        idx, item = inp
        pending[idx] = asyncio.create_task(apply_fn(item))

    while pending or results:
        # Yield any ready results in order
        while emit_idx in results:
            yield results.pop(emit_idx)
            emit_idx += 1

        if not pending:
            break

        # Wait for any task to complete
        done, _ = await asyncio.wait(
            pending.values(), return_when=asyncio.FIRST_COMPLETED
        )

        for task in done:
            # Find which index this task belongs to
            for idx, t in list(pending.items()):
                if t is task:
                    results[idx] = task.result()
                    del pending[idx]
                    break

            # Fill slot with new item if available
            if not exhausted:
                inp = await fetch_input()
                if inp is not None:
                    idx, item = inp
                    pending[idx] = asyncio.create_task(apply_fn(item))

    # Yield remaining results in order
    while emit_idx in results:
        yield results.pop(emit_idx)
        emit_idx += 1
