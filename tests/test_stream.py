"""Tests for Stream class."""

from collections.abc import AsyncIterator

import pytest

from streamish.stream import Stream


def test_stream_sync_iteration() -> None:
    s = Stream([1, 2, 3])
    result = list(s)
    assert result == [1, 2, 3]


def test_stream_reusable() -> None:
    s = Stream([1, 2, 3])
    assert list(s) == [1, 2, 3]
    assert list(s) == [1, 2, 3]


async def test_stream_async_iteration_from_sync() -> None:
    s = Stream([1, 2, 3])
    result = [x async for x in s]
    assert result == [1, 2, 3]


async def test_stream_async_iteration_from_async() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in [1, 2, 3]:
            yield i

    s = Stream(gen())
    result = [x async for x in s]
    assert result == [1, 2, 3]


async def test_stream_zip_with_async() -> None:
    async def gen() -> AsyncIterator[str]:
        for c in "ab":
            yield c

    result = [x async for x in Stream([1, 2, 3]).zip(gen())]
    assert result == [(1, "a"), (2, "b")]


async def test_stream_to_async_from_sync() -> None:
    s = Stream([1, 2, 3]).to_async()
    assert [x async for x in s] == [1, 2, 3]


async def test_stream_to_async_from_async() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in [1, 2, 3]:
            yield i

    s = Stream(gen()).to_async()
    assert [x async for x in s] == [1, 2, 3]


def test_stream_to_async_refuses_sync_iteration() -> None:
    s = Stream([1, 2, 3]).to_async()
    with pytest.raises(TypeError):
        list(s)


async def test_stream_to_async_allows_merge_with_sync_source() -> None:
    async def gen() -> AsyncIterator[int]:
        yield 3

    merged = Stream([1, 2]).to_async().merge(gen())
    assert sorted([x async for x in merged]) == [1, 2, 3]
