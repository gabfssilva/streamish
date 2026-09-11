"""Group operations."""

import asyncio
from collections import deque
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
)
from contextlib import suppress
from typing import overload

from streamish._util import ensure_async_iterator, is_async_iterable

__all__ = ["batch", "window", "partition", "partition_async"]


@overload
def batch[T](
    size: int, it: Iterable[T], *, timeout: float | None = None
) -> Iterator[list[T]]: ...


@overload
def batch[T](
    size: int, it: AsyncIterable[T], *, timeout: float | None = None
) -> AsyncIterator[list[T]]: ...


def batch[T](
    size: int,
    it: Iterable[T] | AsyncIterable[T],
    *,
    timeout: float | None = None,
) -> Iterator[list[T]] | AsyncIterator[list[T]]:
    """Group elements into batches of up to `size`.

    With `timeout`, iteration is async and a partial batch is emitted `timeout`
    seconds after its first element. A background task reads the source up to
    `size` elements ahead, and an async generator source is closed when
    iteration stops.
    """
    if size <= 0:
        raise ValueError("size must be positive")
    if timeout is not None:
        return _batch_timeout(size, it, timeout)
    if is_async_iterable(it):
        return _batch_async(size, it)
    return _batch_sync(size, it)  # type: ignore[arg-type, return-value]


def _batch_sync[T](size: int, it: Iterable[T]) -> Iterator[list[T]]:
    current: list[T] = []
    for item in it:
        current.append(item)
        if len(current) >= size:
            yield current
            current = []
    if current:
        yield current


async def _batch_async[T](size: int, it: AsyncIterable[T]) -> AsyncIterator[list[T]]:
    current: list[T] = []
    async for item in it:
        current.append(item)
        if len(current) >= size:
            yield current
            current = []
    if current:
        yield current


class _Done:
    """Marks that the reader task of `_batch_timeout` has finished."""


async def _batch_timeout[T](
    size: int, it: Iterable[T] | AsyncIterable[T], timeout: float
) -> AsyncIterator[list[T]]:
    source: AsyncIterator[T] = (
        it.__aiter__()
        if isinstance(it, AsyncIterable)
        else ensure_async_iterator(iter(it))
    )
    # A single task reads the source, so a deadline never cancels a pending
    # `__anext__`, and items it already queued are taken without suspending.
    queue: asyncio.Queue[T | _Done] = asyncio.Queue(size)

    async def read() -> None:
        async for item in source:
            await queue.put(item)

    def wake(_: asyncio.Task[None]) -> None:
        # A full queue means the consumer is not blocked on it, and the consumer
        # checks the reader before blocking again.
        with suppress(asyncio.QueueFull):
            queue.put_nowait(_Done())

    async def wait(deadline: float | None) -> T | _Done:
        if reader.done():
            return _Done()
        async with asyncio.timeout_at(deadline):
            return await queue.get()

    reader = asyncio.create_task(read())
    reader.add_done_callback(wake)
    loop = asyncio.get_running_loop()
    try:
        while True:
            item = queue.get_nowait() if not queue.empty() else await wait(None)
            current: list[T] = []
            deadline = loop.time() + timeout
            while not isinstance(item, _Done):
                current.append(item)
                if len(current) >= size:
                    break
                try:
                    item = (
                        queue.get_nowait()
                        if not queue.empty()
                        else await wait(deadline)
                    )
                except TimeoutError:
                    break
            if isinstance(item, _Done):
                reader.result()
                if current:
                    yield current
                return
            yield current
    finally:
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        if isinstance(source, AsyncGenerator):
            await source.aclose()


@overload
def window[T](size: int, it: Iterable[T], *, step: int = 1) -> Iterator[list[T]]: ...


@overload
def window[T](
    size: int, it: AsyncIterable[T], *, step: int = 1
) -> AsyncIterator[list[T]]: ...


def window[T](
    size: int, it: Iterable[T] | AsyncIterable[T], *, step: int = 1
) -> Iterator[list[T]] | AsyncIterator[list[T]]:
    """Sliding window over elements."""
    if size <= 0:
        raise ValueError("size must be positive")
    if step <= 0:
        raise ValueError("step must be positive")
    if is_async_iterable(it):
        return _window_async(size, it, step)  # type: ignore[arg-type]
    return _window_sync(size, it, step)  # type: ignore[arg-type, return-value]


def _window_sync[T](size: int, it: Iterable[T], step: int) -> Iterator[list[T]]:
    buf: deque[T] = deque(maxlen=size)
    skip = 0
    for item in it:
        if skip > 0:
            skip -= 1
            buf.append(item)
            continue
        buf.append(item)
        if len(buf) == size:
            yield list(buf)
            skip = step - 1
            for _ in range(min(step, size)):
                if buf:
                    buf.popleft()


async def _window_async[T](
    size: int, it: AsyncIterable[T], step: int
) -> AsyncIterator[list[T]]:
    buf: deque[T] = deque(maxlen=size)
    skip = 0
    async for item in it:
        if skip > 0:
            skip -= 1
            buf.append(item)
            continue
        buf.append(item)
        if len(buf) == size:
            yield list(buf)
            skip = step - 1
            for _ in range(min(step, size)):
                if buf:
                    buf.popleft()


def partition[T](pred: Callable[[T], bool], it: Iterable[T]) -> tuple[list[T], list[T]]:
    """Split into (matches, non_matches). Terminal operation."""
    matches: list[T] = []
    non_matches: list[T] = []
    for item in it:
        if pred(item):
            matches.append(item)
        else:
            non_matches.append(item)
    return matches, non_matches


async def partition_async[T](
    pred: Callable[[T], bool], it: AsyncIterable[T]
) -> tuple[list[T], list[T]]:
    """Split into (matches, non_matches). Terminal operation (async)."""
    matches: list[T] = []
    non_matches: list[T] = []
    async for item in it:
        if pred(item):
            matches.append(item)
        else:
            non_matches.append(item)
    return matches, non_matches
