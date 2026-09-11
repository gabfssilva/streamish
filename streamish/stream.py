"""Stream class for fluent iterator operations."""

from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Hashable,
    Iterable,
    Iterator,
)
from typing import Any

from streamish._util import is_async_iterable
from streamish.ops.combine import chain as chain_fn
from streamish.ops.combine import chain_async, zip_async
from streamish.ops.combine import interleave as interleave_op
from streamish.ops.combine import merge as merge_op
from streamish.ops.combine import zip_ as zip_fn
from streamish.ops.filter import distinct as distinct_op
from streamish.ops.filter import distinct_by as distinct_by_op
from streamish.ops.filter import skip as skip_op
from streamish.ops.filter import skip_while as skip_while_op
from streamish.ops.filter import take as take_op
from streamish.ops.filter import take_while as take_while_op
from streamish.ops.group import batch as batch_op
from streamish.ops.group import partition as partition_op
from streamish.ops.group import window as window_op
from streamish.ops.transform import enumerate_ as enumerate_op
from streamish.ops.transform import filter_ as filter_op
from streamish.ops.transform import flat_map as flat_map_op
from streamish.ops.transform import flatten as flatten_op
from streamish.ops.transform import map_ as map_op
from streamish.ops.transform import map_async as map_async_op
from streamish.ops.transform import scan as scan_op

__all__ = ["Stream"]


class Stream[T]:
    """A lazy, chainable pipeline over a sync or async iterable.

    Each operation returns a new `Stream` and does no work until the stream is
    iterated. A stream is async if its source is async or an operation made it
    async, such as `map` with a coroutine function. Sync streams support both
    `for` and `async for`; async streams support only `async for`, and
    iterating one with `for` raises `TypeError`.

    Operations are backed by generators, so a stream produced by an operation
    can be iterated only once.

    Parameters
    ----------
    source
        The elements to stream.

    See Also
    --------
    streamish.stream

    Examples
    --------
    >>> import streamish as st
    >>> doubled = st.Stream([1, 2, 3]).map(lambda x: x * 2)
    >>> list(doubled)
    [2, 4, 6]
    >>> list(doubled)
    []
    """

    __slots__ = ("_is_async", "_source")

    def __init__(self, source: Iterable[T] | AsyncIterable[T]) -> None:
        self._source = source
        self._is_async = is_async_iterable(source)

    def __iter__(self) -> Iterator[T]:
        if self._is_async:
            raise TypeError(
                "Cannot use sync iteration on async source. Use 'async for'."
            )
        source = self._source
        if not isinstance(source, Iterable):
            raise TypeError("Source is not iterable")
        return iter(source)

    def __aiter__(self) -> AsyncIterator[T]:
        return self._aiter_impl()

    async def _aiter_impl(self) -> AsyncIterator[T]:
        if self._is_async:
            source = self._source
            if not isinstance(source, AsyncIterable):
                raise TypeError("Source is not async iterable")
            async for item in source:
                yield item
        else:
            source = self._source
            if not isinstance(source, Iterable):
                raise TypeError("Source is not iterable")
            for item in source:
                yield item

    def map[U](self, fn: Callable[[T], U] | Callable[[T], Awaitable[U]]) -> "Stream[U]":
        """Apply `fn` to each element.

        The stream becomes async if `fn` is a coroutine function.

        Parameters
        ----------
        fn
            Function applied to each element.

        See Also
        --------
        streamish.map

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream([1, 2, 3]).map(lambda x: x * 10))
        [10, 20, 30]
        """
        return Stream(map_op(fn, self._source))  # type: ignore[arg-type]

    def take(self, n: int) -> "Stream[T]":
        """Take the first `n` elements.

        Parameters
        ----------
        n
            Maximum number of elements to take. If `n <= 0`, the stream is
            empty.

        See Also
        --------
        streamish.take

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream(range(10)).take(3))
        [0, 1, 2]
        """
        return Stream(take_op(n, self._source))

    def skip(self, n: int) -> "Stream[T]":
        """Skip the first `n` elements.

        Parameters
        ----------
        n
            Number of elements to skip. If `n <= 0`, nothing is skipped.

        See Also
        --------
        streamish.skip

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream(range(5)).skip(3))
        [3, 4]
        """
        return Stream(skip_op(n, self._source))

    def filter(self, pred: Callable[[T], bool]) -> "Stream[T]":
        """Keep only the elements for which `pred` returns true.

        Parameters
        ----------
        pred
            Predicate called once per element.

        See Also
        --------
        streamish.filter

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream(range(6)).filter(lambda x: x % 2 == 0))
        [0, 2, 4]
        """
        return Stream(filter_op(pred, self._source))

    def take_while(self, pred: Callable[[T], bool]) -> "Stream[T]":
        """Take elements until `pred` first returns false.

        Parameters
        ----------
        pred
            Predicate called on each element until it returns false.

        See Also
        --------
        streamish.take_while

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream([1, 2, 5, 1]).take_while(lambda x: x < 3))
        [1, 2]
        """
        return Stream(take_while_op(pred, self._source))

    def skip_while(self, pred: Callable[[T], bool]) -> "Stream[T]":
        """Skip elements until `pred` first returns false, then emit the rest.

        Parameters
        ----------
        pred
            Predicate called on each element until it returns false.

        See Also
        --------
        streamish.skip_while

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream([1, 2, 5, 1]).skip_while(lambda x: x < 3))
        [5, 1]
        """
        return Stream(skip_while_op(pred, self._source))

    def distinct(
        self,
        *,
        window: int | None = None,
        timeout: float | None = None,
    ) -> "Stream[T]":
        """Drop elements equal to one emitted earlier.

        Elements must be hashable. Every emitted element is remembered unless
        `window` or `timeout` limits that memory; a forgotten element can be
        emitted again.

        Parameters
        ----------
        window
            Remember only the last `window` emitted elements. Must be positive.
        timeout
            Forget each element `timeout` seconds after it was emitted. Must be
            positive.

        See Also
        --------
        streamish.distinct

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream([1, 2, 1, 3, 1]).distinct())
        [1, 2, 3]
        >>> list(st.stream([1, 2, 1, 3, 1]).distinct(window=2))
        [1, 2, 3, 1]
        """
        return Stream(distinct_op(self._source, window=window, timeout=timeout))  # type: ignore[arg-type]

    def distinct_by[K: Hashable](
        self,
        key_fn: Callable[[T], K],
        *,
        window: int | None = None,
        timeout: float | None = None,
    ) -> "Stream[T]":
        """Drop elements whose key equals that of an element emitted earlier.

        Keys are remembered as in `distinct`: forever by default, or bounded by
        `window` and `timeout`.

        Parameters
        ----------
        key_fn
            Function returning a hashable key for each element.
        window
            Remember only the keys of the last `window` emitted elements. Must
            be positive.
        timeout
            Forget each key `timeout` seconds after its element was emitted.
            Must be positive.

        See Also
        --------
        streamish.distinct_by

        Examples
        --------
        >>> import streamish as st
        >>> words = ["apple", "avocado", "banana"]
        >>> list(st.stream(words).distinct_by(lambda w: w[0]))
        ['apple', 'banana']
        """
        return Stream(
            distinct_by_op(key_fn, self._source, window=window, timeout=timeout)
        )  # type: ignore[arg-type]

    def flatten[U](self: "Stream[Iterable[U]]") -> "Stream[U]":
        """Flatten one level of nesting.

        Inner iterables must be sync, even in an async stream.

        See Also
        --------
        streamish.flatten

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream([[1, 2], [3]]).flatten())
        [1, 2, 3]
        """
        return Stream(flatten_op(self._source))  # type: ignore[arg-type]

    def flat_map[U](self, fn: Callable[[T], Iterable[U]]) -> "Stream[U]":
        """Map each element to an iterable and emit the elements of each.

        Parameters
        ----------
        fn
            Function returning a sync iterable for each element.

        See Also
        --------
        streamish.flat_map

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream([1, 2]).flat_map(lambda x: [x, x * 10]))
        [1, 10, 2, 20]
        """
        return Stream(flat_map_op(fn, self._source))

    def enumerate(self, start: int = 0) -> "Stream[tuple[int, T]]":
        """Pair each element with its index.

        Parameters
        ----------
        start
            Index of the first element.

        See Also
        --------
        streamish.enumerate

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream("ab").enumerate(start=1))
        [(1, 'a'), (2, 'b')]
        """
        return Stream(enumerate_op(self._source, start))

    def scan[U](self, fn: Callable[[U, T], U], *, initial: U) -> "Stream[U]":
        """Emit a running accumulation of the elements.

        Each element is combined with the accumulator by `fn`, and the new
        accumulator is emitted. `initial` itself is not emitted.

        Parameters
        ----------
        fn
            Function taking the accumulator and an element and returning the
            new accumulator.
        initial
            Accumulator before the first element.

        See Also
        --------
        streamish.scan

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream([1, 2, 3]).scan(lambda total, x: total + x, initial=0))
        [1, 3, 6]
        """
        return Stream(scan_op(fn, self._source, initial=initial))

    def batch(self, size: int, *, timeout: float | None = None) -> "Stream[list[T]]":
        """Group elements into lists of up to `size`.

        Parameters
        ----------
        size
            Maximum number of elements per batch. Must be positive.
        timeout
            If set, a batch that is not full is emitted `timeout` seconds after
            it received its first element, and the stream becomes async.

        See Also
        --------
        streamish.batch

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream(range(5)).batch(2))
        [[0, 1], [2, 3], [4]]
        """
        return Stream(batch_op(size, self._source, timeout=timeout))

    def window(self, size: int, *, step: int = 1) -> "Stream[list[T]]":
        """Slide a fixed-size window over the stream.

        Parameters
        ----------
        size
            Number of elements per window. Must be positive.
        step
            Distance between the starts of consecutive windows. Must be
            positive.

        See Also
        --------
        streamish.window

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream([1, 2, 3, 4, 5]).window(3))
        [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
        """
        return Stream(window_op(size, self._source, step=step))

    def partition(self, pred: Callable[[T], bool]) -> tuple[list[T], list[T]]:
        """Split the elements into those that satisfy `pred` and those that don't.

        This is a terminal operation: it consumes the stream and returns lists.

        Parameters
        ----------
        pred
            Predicate called once per element.

        Returns
        -------
        tuple[list[T], list[T]]
            `(matches, non_matches)`, each in source order.

        Raises
        ------
        TypeError
            If the stream is async. Use `streamish.partition_async` instead.

        See Also
        --------
        streamish.partition

        Examples
        --------
        >>> import streamish as st
        >>> st.stream(range(6)).partition(lambda x: x % 2 == 0)
        ([0, 2, 4], [1, 3, 5])
        """
        if self._is_async:
            raise TypeError("Use partition_async() for async sources")
        return partition_op(pred, self._source)  # type: ignore[arg-type]

    def zip(
        self, *others: Iterable[Any] | AsyncIterable[Any]
    ) -> "Stream[tuple[Any, ...]]":
        """Combine elements with those of `others` into tuples, by position.

        Stops at the end of the shortest input. The stream becomes async if it
        or any of `others` is async.

        Parameters
        ----------
        *others
            Inputs to zip with, sync or async.

        See Also
        --------
        streamish.zip
        streamish.zip_async

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream([1, 2, 3]).zip("ab"))
        [(1, 'a'), (2, 'b')]
        """
        if self._is_async or any(is_async_iterable(o) for o in others):
            return Stream(zip_async(self._source, *others))
        return Stream(zip_fn(self._source, *others))  # type: ignore[arg-type]

    def chain(self, *others: Iterable[T] | AsyncIterable[T]) -> "Stream[T]":
        """Emit this stream's elements, then those of each of `others` in order.

        The stream becomes async if it or any of `others` is async.

        Parameters
        ----------
        *others
            Inputs to append, sync or async.

        See Also
        --------
        streamish.chain
        streamish.chain_async

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream([1, 2]).chain([3], (4, 5)))
        [1, 2, 3, 4, 5]
        """
        if self._is_async or any(is_async_iterable(o) for o in others):
            return Stream(chain_async(self._source, *others))
        return Stream(chain_fn(self._source, *others))  # type: ignore[arg-type]

    def interleave(self, *others: Iterable[T]) -> "Stream[T]":
        """Alternate elements from this stream and `others`, round-robin.

        Exhausted inputs are skipped and the rest continue. The stream and all
        `others` must be sync.

        Parameters
        ----------
        *others
            Sync iterables to interleave with.

        See Also
        --------
        streamish.interleave

        Examples
        --------
        >>> import streamish as st
        >>> list(st.stream([1, 2, 3]).interleave([10, 20]))
        [1, 10, 2, 20, 3]
        """
        return Stream(interleave_op(self._source, *others))  # type: ignore[arg-type]

    def merge(self, *others: AsyncIterable[T]) -> "Stream[T]":
        """Emit elements from this stream and `others` as soon as each arrives.

        Order is kept within each input but not across inputs. The stream and
        all `others` must be async.

        Parameters
        ----------
        *others
            Async iterables to merge with.

        See Also
        --------
        streamish.merge

        Examples
        --------
        >>> import asyncio
        >>> import streamish as st
        >>> async def agen(*items):
        ...     for item in items:
        ...         yield item
        >>> async def main():
        ...     merged = st.stream(agen(1, 2)).merge(agen(3))
        ...     return sorted([x async for x in merged])
        >>> asyncio.run(main())
        [1, 2, 3]
        """
        return Stream(merge_op(self._source, *others))  # type: ignore[arg-type]

    def map_async[U](
        self, fn: Callable[[T], Awaitable[U]], *, concurrency: int = 1
    ) -> "Stream[U]":
        """Apply `fn` to elements concurrently, emitting results in source order.

        The stream becomes async.

        Parameters
        ----------
        fn
            Function returning an awaitable for each element.
        concurrency
            Maximum number of elements in progress at once, counting calls
            still running and results waiting for an earlier one. Must be
            positive.

        See Also
        --------
        streamish.map_async

        Examples
        --------
        >>> import asyncio
        >>> import streamish as st
        >>> async def double(x):
        ...     return x * 2
        >>> async def main():
        ...     doubled = st.stream([1, 2, 3]).map_async(double, concurrency=2)
        ...     return [x async for x in doubled]
        >>> asyncio.run(main())
        [2, 4, 6]
        """
        return Stream(map_async_op(fn, self._source, concurrency=concurrency))
