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
    size: int, it: Iterable[T], *, timeout: None = None
) -> Iterator[list[T]]: ...


@overload
def batch[T](
    size: int, it: AsyncIterable[T], *, timeout: float | None = None
) -> AsyncIterator[list[T]]: ...


@overload
def batch[T](
    size: int, it: Iterable[T] | AsyncIterable[T], *, timeout: float
) -> AsyncIterator[list[T]]: ...


def batch[T](
    size: int,
    it: Iterable[T] | AsyncIterable[T],
    *,
    timeout: float | None = None,
) -> Iterator[list[T]] | AsyncIterator[list[T]]:
    """Group elements into lists of up to `size`.

    A batch is emitted when it is full. The last batch holds whatever remains
    and may be shorter.

    Without `timeout`, the result is async only if `it` is async. With
    `timeout`, the result is always async, and a batch that is not full is
    also emitted `timeout` seconds after it received its first element.

    Parameters
    ----------
    size
        Maximum number of elements per batch. Must be positive.
    it
        Source elements.
    timeout
        Seconds a batch may wait for more elements after receiving its first
        one. `None` waits until the batch is full or the source ends.

    Returns
    -------
    Iterator[list[T]] | AsyncIterator[list[T]]
        The batches, in source order.

    Raises
    ------
    ValueError
        If `size` is not positive. Raised when `batch` is called, not when
        iteration starts.

    See Also
    --------
    window : Fixed-size groups that can overlap.

    Notes
    -----
    With `timeout`, a background task reads `it`, so a deadline never
    interrupts a pending read. The task reads at most `size` + 1 elements
    beyond those already emitted. When iteration stops, the task is cancelled
    and an async generator source is closed.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.batch(2, [1, 2, 3, 4, 5]))
    [[1, 2], [3, 4], [5]]
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
    """Slide a fixed-size window over the elements.

    Each window is a new list of `size` consecutive elements. The next window
    starts `step` elements after the previous one, so windows overlap when
    `step < size` and skip elements when `step > size`. Only full windows are
    emitted; trailing elements that cannot fill one are dropped.

    Parameters
    ----------
    size
        Number of elements per window. Must be positive.
    it
        Source elements. The result is async if `it` is async.
    step
        Distance between the starts of consecutive windows. Must be positive.

    Returns
    -------
    Iterator[list[T]] | AsyncIterator[list[T]]
        The windows, in source order.

    Raises
    ------
    ValueError
        If `size` or `step` is not positive. Raised when `window` is called,
        not when iteration starts.

    See Also
    --------
    batch : Non-overlapping groups that keep the trailing partial group.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.window(3, [1, 2, 3, 4, 5]))
    [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
    >>> list(st.window(2, [1, 2, 3, 4, 5], step=2))
    [[1, 2], [3, 4]]
    """
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
    """Split elements into those that satisfy `pred` and those that don't.

    This is a terminal operation: it consumes `it` entirely and returns lists,
    so `it` must be finite.

    Parameters
    ----------
    pred
        Predicate called once per element.
    it
        Source elements. Must be sync; use `partition_async` for async sources.

    Returns
    -------
    tuple[list[T], list[T]]
        `(matches, non_matches)`, each in source order.

    See Also
    --------
    partition_async : The same for async iterables.
    filter : Lazily keep only the matches.

    Examples
    --------
    >>> import streamish as st
    >>> st.partition(lambda x: x % 2 == 0, range(6))
    ([0, 2, 4], [1, 3, 5])
    """
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
    """Split async elements into those that satisfy `pred` and those that don't.

    This is a terminal operation: it consumes `it` entirely, so `it` must be
    finite.

    Parameters
    ----------
    pred
        Predicate called once per element.
    it
        Source elements.

    Returns
    -------
    tuple[list[T], list[T]]
        `(matches, non_matches)`, each in source order.

    See Also
    --------
    partition : The same for sync iterables.

    Examples
    --------
    >>> import asyncio
    >>> import streamish as st
    >>> async def numbers():
    ...     for i in range(6):
    ...         yield i
    >>> asyncio.run(st.partition_async(lambda x: x % 2 == 0, numbers()))
    ([0, 2, 4], [1, 3, 5])
    """
    matches: list[T] = []
    non_matches: list[T] = []
    async for item in it:
        if pred(item):
            matches.append(item)
        else:
            non_matches.append(item)
    return matches, non_matches
