"""Combine operations."""

import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator

from streamish._util import ensure_async_iterator, is_async_iterable

__all__ = ["zip_", "zip_async", "chain", "chain_async", "interleave", "merge"]


def zip_[T](*iterables: Iterable[T]) -> Iterator[tuple[T, ...]]:
    """Zip iterables together."""
    iters = [iter(it) for it in iterables]
    while True:
        result: list[T] = []
        for it in iters:
            try:
                result.append(next(it))
            except StopIteration:
                return
        yield tuple(result)


async def zip_async[T](
    *iterables: AsyncIterable[T] | Iterable[T],
) -> AsyncIterator[tuple[T, ...]]:
    """Zip async iterables together."""
    aiters: list[AsyncIterator[T]] = [
        it.__aiter__() if is_async_iterable(it) else ensure_async_iterator(iter(it))  # type: ignore[union-attr, arg-type]
        for it in iterables
    ]
    while True:
        try:
            results: list[T] = []
            for ait in aiters:
                results.append(await ait.__anext__())
            yield tuple(results)
        except StopAsyncIteration:
            break


def chain[T](*iterables: Iterable[T]) -> Iterator[T]:
    """Chain iterables together."""
    for it in iterables:
        yield from it


async def chain_async[T](
    *iterables: AsyncIterable[T] | Iterable[T],
) -> AsyncIterator[T]:
    """Chain async iterables together."""
    for it in iterables:
        if is_async_iterable(it):
            async for item in it:  # type: ignore[union-attr]
                yield item
        else:
            for item in it:  # type: ignore[union-attr]
                yield item


def interleave[T](*iterables: Iterable[T]) -> Iterator[T]:
    """Alternate elements from iterables (round-robin)."""
    iters: list[Iterator[T]] = [iter(it) for it in iterables]
    while iters:
        next_iters: list[Iterator[T]] = []
        for it in iters:
            try:
                yield next(it)
                next_iters.append(it)
            except StopIteration:
                pass
        iters = next_iters


async def merge[T](*iterables: AsyncIterable[T]) -> AsyncIterator[T]:
    """Merge async iterables - emit as items arrive."""
    pending: set[asyncio.Task[tuple[int, T | None, bool]]] = set()
    aiters: dict[int, AsyncIterator[T]] = {}

    for i, it in enumerate(iterables):
        aiters[i] = it.__aiter__()
        task = asyncio.create_task(_fetch_next(i, aiters[i]))
        pending.add(task)

    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            idx, value, exhausted = task.result()
            if exhausted:
                continue
            yield value  # type: ignore[misc]
            new_task = asyncio.create_task(_fetch_next(idx, aiters[idx]))
            pending.add(new_task)


async def _fetch_next[T](idx: int, ait: AsyncIterator[T]) -> tuple[int, T | None, bool]:
    try:
        value = await ait.__anext__()
        return (idx, value, False)
    except StopAsyncIteration:
        return (idx, None, True)
