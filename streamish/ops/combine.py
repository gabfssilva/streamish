"""Combine operations."""

import asyncio
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Iterable,
    Iterator,
)

from streamish._util import ensure_async_iterator, is_async_iterable

__all__ = ["zip_", "zip_async", "chain", "chain_async", "interleave", "merge"]


def zip_[T](*iterables: Iterable[T]) -> Iterator[tuple[T, ...]]:
    """Combine elements of `iterables` into tuples, by position.

    Stops at the end of the shortest input.

    Parameters
    ----------
    *iterables
        Sync inputs to zip.

    Yields
    ------
    tuple[T, ...]
        One element from each input, by position.

    See Also
    --------
    zip_async : The same for async or mixed inputs.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.zip([1, 2, 3], [10, 20]))
    [(1, 10), (2, 20)]
    """
    if not iterables:
        return
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
    """Combine elements of sync or async `iterables` into tuples, by position.

    Stops at the end of the shortest input. Inputs are advanced one after
    another, not concurrently.

    Parameters
    ----------
    *iterables
        Inputs to zip, sync or async.

    Yields
    ------
    tuple[T, ...]
        One element from each input, by position.

    See Also
    --------
    zip : The same for sync inputs.

    Examples
    --------
    >>> import asyncio
    >>> import streamish as st
    >>> async def letters():
    ...     yield "a"
    ...     yield "b"
    >>> async def main():
    ...     return [pair async for pair in st.zip_async([1, 2, 3], letters())]
    >>> asyncio.run(main())
    [(1, 'a'), (2, 'b')]
    """
    if not iterables:
        return
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
    """Emit the elements of each of `iterables` in turn.

    Parameters
    ----------
    *iterables
        Sync inputs, consumed in order.

    Yields
    ------
    T
        All elements of the first input, then the second, and so on.

    See Also
    --------
    chain_async : The same for async or mixed inputs.
    interleave : Take one element from each input in turn.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.chain([1, 2], [3], []))
    [1, 2, 3]
    """
    for it in iterables:
        yield from it


async def chain_async[T](
    *iterables: AsyncIterable[T] | Iterable[T],
) -> AsyncIterator[T]:
    """Emit the elements of each of the sync or async `iterables` in turn.

    Parameters
    ----------
    *iterables
        Inputs to consume in order, sync or async.

    Yields
    ------
    T
        All elements of the first input, then the second, and so on.

    See Also
    --------
    chain : The same for sync inputs.
    merge : Emit elements of async inputs as they arrive.

    Examples
    --------
    >>> import asyncio
    >>> import streamish as st
    >>> async def numbers():
    ...     yield 3
    >>> async def main():
    ...     return [x async for x in st.chain_async([1, 2], numbers())]
    >>> asyncio.run(main())
    [1, 2, 3]
    """
    for it in iterables:
        if is_async_iterable(it):
            async for item in it:  # type: ignore[union-attr]
                yield item
        else:
            for item in it:  # type: ignore[union-attr]
                yield item


def interleave[T](*iterables: Iterable[T]) -> Iterator[T]:
    """Take one element from each of `iterables` in turn, round-robin.

    An exhausted input is skipped, and the rest continue until all are
    exhausted.

    Parameters
    ----------
    *iterables
        Sync inputs to interleave.

    Yields
    ------
    T
        The elements in round-robin order.

    See Also
    --------
    chain : Emit each input entirely before the next.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.interleave([1, 2, 3], [10, 20], [100]))
    [1, 10, 100, 2, 20, 3]
    """
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
    """Emit elements from async `iterables` as soon as each arrives.

    All inputs are read concurrently. Order is kept within each input but not
    across inputs.

    Parameters
    ----------
    *iterables
        Async inputs to merge.

    Yields
    ------
    T
        Elements from all inputs, in arrival order.

    See Also
    --------
    chain_async : Emit each input entirely before the next.

    Notes
    -----
    An exception from an input propagates unchanged. When iteration stops for
    any reason, pending reads are cancelled and async generator inputs are
    closed.

    Examples
    --------
    >>> import asyncio
    >>> import streamish as st
    >>> async def slow():
    ...     await asyncio.sleep(0.1)
    ...     yield "slow"
    >>> async def fast():
    ...     yield "fast 1"
    ...     yield "fast 2"
    >>> async def main():
    ...     return [x async for x in st.merge(slow(), fast())]
    >>> asyncio.run(main())
    ['fast 1', 'fast 2', 'slow']
    """
    sources = [it.__aiter__() for it in iterables]
    tasks = {asyncio.create_task(_fetch_next(source)): source for source in sources}
    try:
        while tasks:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                source = tasks.pop(task)
                try:
                    value = task.result()
                except StopAsyncIteration:
                    continue
                yield value
                tasks[asyncio.create_task(_fetch_next(source))] = source
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for source in sources:
            if isinstance(source, AsyncGenerator):
                await source.aclose()


async def _fetch_next[T](source: AsyncIterator[T]) -> T:
    return await anext(source)
