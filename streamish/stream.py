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
    """Wrapper for iterables providing fluent chainable operations."""

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
        """Apply function to each element."""
        return Stream(map_op(fn, self._source))  # type: ignore[arg-type]

    def take(self, n: int) -> "Stream[T]":
        """Take first n elements."""
        return Stream(take_op(n, self._source))

    def skip(self, n: int) -> "Stream[T]":
        """Skip first n elements."""
        return Stream(skip_op(n, self._source))

    def filter(self, pred: Callable[[T], bool]) -> "Stream[T]":
        """Keep elements that satisfy predicate."""
        return Stream(filter_op(pred, self._source))

    def take_while(self, pred: Callable[[T], bool]) -> "Stream[T]":
        """Take elements while predicate is true."""
        return Stream(take_while_op(pred, self._source))

    def skip_while(self, pred: Callable[[T], bool]) -> "Stream[T]":
        """Skip elements while predicate is true."""
        return Stream(skip_while_op(pred, self._source))

    def distinct(self) -> "Stream[T]":
        """Remove duplicates."""
        return Stream(distinct_op(self._source))  # type: ignore[arg-type]

    def distinct_by[K: Hashable](self, key_fn: Callable[[T], K]) -> "Stream[T]":
        """Remove duplicates by key function."""
        return Stream(distinct_by_op(key_fn, self._source))

    def flatten[U](self: "Stream[Iterable[U]]") -> "Stream[U]":
        """Flatten one level of nesting."""
        return Stream(flatten_op(self._source))  # type: ignore[arg-type]

    def flat_map[U](self, fn: Callable[[T], Iterable[U]]) -> "Stream[U]":
        """Map then flatten."""
        return Stream(flat_map_op(fn, self._source))

    def enumerate(self, start: int = 0) -> "Stream[tuple[int, T]]":
        """Enumerate elements with index."""
        return Stream(enumerate_op(self._source, start))

    def scan[U](self, fn: Callable[[U, T], U], *, initial: U) -> "Stream[U]":
        """Accumulator that emits intermediate values."""
        return Stream(scan_op(fn, self._source, initial=initial))

    def batch(self, size: int, *, timeout: float | None = None) -> "Stream[list[T]]":
        """Group elements into batches by size or timeout."""
        return Stream(batch_op(size, self._source, timeout=timeout))

    def window(self, size: int, *, step: int = 1) -> "Stream[list[T]]":
        """Sliding window over elements."""
        return Stream(window_op(size, self._source, step=step))

    def partition(self, pred: Callable[[T], bool]) -> tuple[list[T], list[T]]:
        """Split into (matches, non_matches). Terminal - consumes stream."""
        if self._is_async:
            raise TypeError("Use partition_async() for async sources")
        return partition_op(pred, self._source)  # type: ignore[arg-type]

    def zip(self, *others: Iterable[Any]) -> "Stream[tuple[Any, ...]]":
        """Zip with other iterables."""
        if self._is_async or any(is_async_iterable(o) for o in others):
            return Stream(zip_async(self._source, *others))
        return Stream(zip_fn(self._source, *others))  # type: ignore[arg-type]

    def chain(self, *others: Iterable[T] | AsyncIterable[T]) -> "Stream[T]":
        """Chain with other iterables."""
        if self._is_async or any(is_async_iterable(o) for o in others):
            return Stream(chain_async(self._source, *others))
        return Stream(chain_fn(self._source, *others))  # type: ignore[arg-type]

    def interleave(self, *others: Iterable[T]) -> "Stream[T]":
        """Interleave with other iterables (round-robin)."""
        return Stream(interleave_op(self._source, *others))  # type: ignore[arg-type]

    def merge(self, *others: AsyncIterable[T]) -> "Stream[T]":
        """Merge async iterables - emit as items arrive."""
        return Stream(merge_op(self._source, *others))  # type: ignore[arg-type]

    def map_async[U](
        self, fn: Callable[[T], Awaitable[U]], *, concurrency: int = 1
    ) -> "Stream[U]":
        """Map with concurrent async execution."""
        return Stream(map_async_op(fn, self._source, concurrency=concurrency))
