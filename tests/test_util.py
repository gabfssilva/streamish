"""Tests for utility helpers."""

from collections.abc import AsyncIterator

from streamish._util import ensure_async_iterator, is_async_iterable, is_awaitable


def test_is_async_iterable_with_sync() -> None:
    assert is_async_iterable([1, 2, 3]) is False


def test_is_async_iterable_with_async() -> None:
    async def gen() -> AsyncIterator[int]:
        yield 1

    assert is_async_iterable(gen()) is True


def test_is_awaitable_with_sync_fn() -> None:
    def fn(x: int) -> int:
        return x * 2

    assert is_awaitable(fn) is False


def test_is_awaitable_with_async_fn() -> None:
    async def fn(x: int) -> int:
        return x * 2

    assert is_awaitable(fn) is True


async def test_ensure_async_iterator_from_sync() -> None:
    result = [x async for x in ensure_async_iterator(iter([1, 2, 3]))]
    assert result == [1, 2, 3]


async def test_ensure_async_iterator_from_async() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in [1, 2, 3]:
            yield i

    result = [x async for x in ensure_async_iterator(gen())]
    assert result == [1, 2, 3]
