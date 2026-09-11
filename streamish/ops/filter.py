"""Filter operations."""

import time
from collections import deque
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Container,
    Hashable,
    Iterable,
    Iterator,
)
from itertools import islice
from typing import overload

from streamish._util import is_async_iterable

__all__ = ["take", "skip", "take_while", "skip_while", "distinct", "distinct_by"]


@overload
def take[T](n: int, it: Iterable[T]) -> Iterator[T]: ...


@overload
def take[T](n: int, it: AsyncIterable[T]) -> AsyncIterator[T]: ...


def take[T](
    n: int, it: Iterable[T] | AsyncIterable[T]
) -> Iterator[T] | AsyncIterator[T]:
    """Take the first `n` elements.

    No element after the `n`-th is read from `it`, so the rest stay available
    when `it` is a shared iterator.

    Parameters
    ----------
    n
        Maximum number of elements to take. If `n <= 0`, nothing is taken and
        nothing is read.
    it
        Source elements. The result is async if `it` is async.

    Returns
    -------
    Iterator[T] | AsyncIterator[T]
        Up to `n` elements, in source order.

    See Also
    --------
    take_while : Take elements while a predicate holds.
    skip : Drop the first `n` elements instead.

    Examples
    --------
    >>> import streamish as st
    >>> numbers = iter(range(5))
    >>> list(st.take(2, numbers))
    [0, 1]
    >>> list(numbers)
    [2, 3, 4]
    """
    if is_async_iterable(it):
        return _take_async(n, it)  # type: ignore[arg-type]
    return _take_sync(n, it)  # type: ignore[arg-type, return-value]


def _take_sync[T](n: int, it: Iterable[T]) -> Iterator[T]:
    yield from islice(it, max(n, 0))


async def _take_async[T](n: int, it: AsyncIterable[T]) -> AsyncIterator[T]:
    if n <= 0:
        return
    count = 0
    async for item in it:
        yield item
        count += 1
        if count == n:
            break


@overload
def skip[T](n: int, it: Iterable[T]) -> Iterator[T]: ...


@overload
def skip[T](n: int, it: AsyncIterable[T]) -> AsyncIterator[T]: ...


def skip[T](
    n: int, it: Iterable[T] | AsyncIterable[T]
) -> Iterator[T] | AsyncIterator[T]:
    """Skip the first `n` elements and emit the rest.

    Parameters
    ----------
    n
        Number of elements to skip. If `n <= 0`, nothing is skipped.
    it
        Source elements. The result is async if `it` is async.

    Returns
    -------
    Iterator[T] | AsyncIterator[T]
        The elements after the first `n`, in source order.

    See Also
    --------
    skip_while : Skip elements while a predicate holds.
    take : Keep the first `n` elements instead.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.skip(3, range(5)))
    [3, 4]
    """
    if is_async_iterable(it):
        return _skip_async(n, it)  # type: ignore[arg-type]
    return _skip_sync(n, it)  # type: ignore[arg-type, return-value]


def _skip_sync[T](n: int, it: Iterable[T]) -> Iterator[T]:
    count = 0
    for item in it:
        if count < n:
            count += 1
            continue
        yield item


async def _skip_async[T](n: int, it: AsyncIterable[T]) -> AsyncIterator[T]:
    count = 0
    async for item in it:
        if count < n:
            count += 1
            continue
        yield item


@overload
def take_while[T](pred: Callable[[T], bool], it: Iterable[T]) -> Iterator[T]: ...


@overload
def take_while[T](
    pred: Callable[[T], bool], it: AsyncIterable[T]
) -> AsyncIterator[T]: ...


def take_while[T](
    pred: Callable[[T], bool], it: Iterable[T] | AsyncIterable[T]
) -> Iterator[T] | AsyncIterator[T]:
    """Take elements until `pred` first returns false.

    The first element that fails `pred` is read from `it` but not emitted, and
    nothing after it is read.

    Parameters
    ----------
    pred
        Predicate called on each element until it returns false.
    it
        Source elements. The result is async if `it` is async.

    Returns
    -------
    Iterator[T] | AsyncIterator[T]
        The leading elements that satisfy `pred`.

    See Also
    --------
    skip_while : Drop the leading elements instead.
    filter : Test every element, not only the leading ones.

    Examples
    --------
    >>> import streamish as st
    >>> numbers = iter([1, 2, 5, 1])
    >>> list(st.take_while(lambda x: x < 3, numbers))
    [1, 2]
    >>> list(numbers)
    [1]
    """
    if is_async_iterable(it):
        return _take_while_async(pred, it)  # type: ignore[arg-type]
    return _take_while_sync(pred, it)  # type: ignore[arg-type, return-value]


def _take_while_sync[T](pred: Callable[[T], bool], it: Iterable[T]) -> Iterator[T]:
    for item in it:
        if not pred(item):
            break
        yield item


async def _take_while_async[T](
    pred: Callable[[T], bool], it: AsyncIterable[T]
) -> AsyncIterator[T]:
    async for item in it:
        if not pred(item):
            break
        yield item


@overload
def skip_while[T](pred: Callable[[T], bool], it: Iterable[T]) -> Iterator[T]: ...


@overload
def skip_while[T](
    pred: Callable[[T], bool], it: AsyncIterable[T]
) -> AsyncIterator[T]: ...


def skip_while[T](
    pred: Callable[[T], bool], it: Iterable[T] | AsyncIterable[T]
) -> Iterator[T] | AsyncIterator[T]:
    """Skip elements until `pred` first returns false, then emit the rest.

    Once an element fails `pred`, it and every later element are emitted
    without calling `pred` again.

    Parameters
    ----------
    pred
        Predicate called on each element until it returns false.
    it
        Source elements. The result is async if `it` is async.

    Returns
    -------
    Iterator[T] | AsyncIterator[T]
        The elements from the first one that fails `pred` onward.

    See Also
    --------
    take_while : Keep the leading elements instead.
    filter : Test every element, not only the leading ones.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.skip_while(lambda x: x < 3, [1, 2, 5, 1]))
    [5, 1]
    """
    if is_async_iterable(it):
        return _skip_while_async(pred, it)  # type: ignore[arg-type]
    return _skip_while_sync(pred, it)  # type: ignore[arg-type, return-value]


def _skip_while_sync[T](pred: Callable[[T], bool], it: Iterable[T]) -> Iterator[T]:
    skipping = True
    for item in it:
        if skipping and pred(item):
            continue
        skipping = False
        yield item


async def _skip_while_async[T](
    pred: Callable[[T], bool], it: AsyncIterable[T]
) -> AsyncIterator[T]:
    skipping = True
    async for item in it:
        if skipping and pred(item):
            continue
        skipping = False
        yield item


@overload
def distinct[T: Hashable](
    it: Iterable[T],
    *,
    window: int | None = None,
    timeout: float | None = None,
) -> Iterator[T]: ...


@overload
def distinct[T: Hashable](
    it: AsyncIterable[T],
    *,
    window: int | None = None,
    timeout: float | None = None,
) -> AsyncIterator[T]: ...


def distinct[T: Hashable](
    it: Iterable[T] | AsyncIterable[T],
    *,
    window: int | None = None,
    timeout: float | None = None,
) -> Iterator[T] | AsyncIterator[T]:
    """Drop elements equal to one emitted earlier.

    Each emitted element is remembered, and later equal elements are dropped
    while it is remembered. By default nothing is forgotten, so memory grows
    with the number of distinct elements. `window` and `timeout` bound that
    memory; once an element is forgotten, an equal one can be emitted again.

    Parameters
    ----------
    it
        Source elements, which must be hashable. The result is async if `it`
        is async.
    window
        Remember only the last `window` emitted elements. Must be positive.
    timeout
        Forget each element `timeout` seconds after it was emitted. Must be
        positive.

    Returns
    -------
    Iterator[T] | AsyncIterator[T]
        The elements that were not dropped, in source order.

    Raises
    ------
    ValueError
        If `window` or `timeout` is not positive. Raised when `distinct` is
        called, not when iteration starts.

    See Also
    --------
    distinct_by : Compare elements by a key.

    Notes
    -----
    A dropped duplicate does not extend how long the original is remembered.
    With both `window` and `timeout`, an element is forgotten as soon as
    either limit is reached. `timeout` is measured with `time.monotonic`.

    Examples
    --------
    >>> import streamish as st
    >>> list(st.distinct([1, 2, 1, 3, 1]))
    [1, 2, 3]

    With `window=2`, only the last two emitted elements are remembered, so the
    final `1` passes again:

    >>> list(st.distinct([1, 2, 1, 3, 1], window=2))
    [1, 2, 3, 1]
    """
    return _distinct(None, it, window, timeout)


@overload
def distinct_by[T, K: Hashable](
    key_fn: Callable[[T], K],
    it: Iterable[T],
    *,
    window: int | None = None,
    timeout: float | None = None,
) -> Iterator[T]: ...


@overload
def distinct_by[T, K: Hashable](
    key_fn: Callable[[T], K],
    it: AsyncIterable[T],
    *,
    window: int | None = None,
    timeout: float | None = None,
) -> AsyncIterator[T]: ...


def distinct_by[T, K: Hashable](
    key_fn: Callable[[T], K],
    it: Iterable[T] | AsyncIterable[T],
    *,
    window: int | None = None,
    timeout: float | None = None,
) -> Iterator[T] | AsyncIterator[T]:
    """Drop elements whose key equals the key of an element emitted earlier.

    The first element with each key is emitted, and later elements with the
    same key are dropped while that key is remembered. Keys are remembered as
    in `distinct`: forever by default, or bounded by `window` and `timeout`.

    Parameters
    ----------
    key_fn
        Function returning a hashable key. Called once per element.
    it
        Source elements. The result is async if `it` is async.
    window
        Remember only the keys of the last `window` emitted elements. Must be
        positive.
    timeout
        Forget each key `timeout` seconds after its element was emitted. Must
        be positive.

    Returns
    -------
    Iterator[T] | AsyncIterator[T]
        The elements that were not dropped, in source order.

    Raises
    ------
    ValueError
        If `window` or `timeout` is not positive. Raised when `distinct_by` is
        called, not when iteration starts.

    See Also
    --------
    distinct : Compare the elements themselves.

    Examples
    --------
    >>> import streamish as st
    >>> users = [("ana", 1), ("bia", 2), ("ana", 3)]
    >>> list(st.distinct_by(lambda user: user[0], users))
    [('ana', 1), ('bia', 2)]
    """
    return _distinct(key_fn, it, window, timeout)


def _distinct[T, K](
    key_fn: Callable[[T], K] | None,
    it: Iterable[T] | AsyncIterable[T],
    window: int | None,
    timeout: float | None,
) -> Iterator[T] | AsyncIterator[T]:
    remembered: Container[T | K]
    remember: Callable[[T | K], None]
    if window is None and timeout is None:
        keys: set[T | K] = set()
        remembered, remember = keys, keys.add
    else:
        seen: _SeenKeys[T | K] = _SeenKeys(window, timeout)
        # Only `timeout` forgets keys lazily, so without it the dict is exact and
        # checking it directly skips a Python-level `__contains__` per element.
        remembered = seen if timeout is not None else seen.emitted_at
        remember = seen.add
    if is_async_iterable(it):
        return _distinct_async(key_fn, it, remembered, remember)
    return _distinct_sync(key_fn, it, remembered, remember)  # type: ignore[arg-type, return-value]


class _SeenKeys[K]:
    """Keys of emitted elements that are still remembered, oldest first."""

    def __init__(self, window: int | None, timeout: float | None) -> None:
        if window is not None and window < 1:
            raise ValueError("window must be positive")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive")
        self._window = window
        self._timeout = timeout
        self.emitted_at: dict[K, float] = {}
        self._order: deque[K] = deque()

    def __contains__(self, key: object) -> bool:
        emitted_at = self.emitted_at
        if self._timeout is not None:
            order = self._order
            cutoff = time.monotonic() - self._timeout
            while order and emitted_at[order[0]] <= cutoff:
                del emitted_at[order.popleft()]
        return key in emitted_at

    def add(self, key: K) -> None:
        self.emitted_at[key] = 0.0 if self._timeout is None else time.monotonic()
        order = self._order
        order.append(key)
        # Keys leave only from the front, so the keys after a remembered key are
        # exactly the elements emitted after it.
        if self._window is not None and len(order) > self._window:
            del self.emitted_at[order.popleft()]


def _distinct_sync[T, K](
    key_fn: Callable[[T], K] | None,
    it: Iterable[T],
    remembered: Container[T | K],
    remember: Callable[[T | K], None],
) -> Iterator[T]:
    for item in it:
        key = item if key_fn is None else key_fn(item)
        if key not in remembered:
            remember(key)
            yield item


async def _distinct_async[T, K](
    key_fn: Callable[[T], K] | None,
    it: AsyncIterable[T],
    remembered: Container[T | K],
    remember: Callable[[T | K], None],
) -> AsyncIterator[T]:
    async for item in it:
        key = item if key_fn is None else key_fn(item)
        if key not in remembered:
            remember(key)
            yield item
