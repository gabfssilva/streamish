"""Tests for combine operations."""

from collections.abc import AsyncIterator

import streamish as st


def test_zip_sync() -> None:
    result = list(st.zip([1, 2, 3], ["a", "b", "c"]))
    assert result == [(1, "a"), (2, "b"), (3, "c")]


def test_zip_fluent() -> None:
    result = list(st.stream([1, 2]).zip([10, 20]))
    assert result == [(1, 10), (2, 20)]


def test_zip_unequal_length() -> None:
    result = list(st.zip([1, 2, 3], ["a", "b"]))
    assert result == [(1, "a"), (2, "b")]


def test_chain_sync() -> None:
    result = list(st.chain([1, 2], [3, 4], [5]))
    assert result == [1, 2, 3, 4, 5]


def test_chain_fluent() -> None:
    result = list(st.stream([1, 2]).chain([3, 4]))
    assert result == [1, 2, 3, 4]


async def test_chain_async() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in [3, 4]:
            yield i

    result = [x async for x in st.stream([1, 2]).chain(gen())]
    assert result == [1, 2, 3, 4]


def test_interleave_sync() -> None:
    result = list(st.interleave([1, 2, 3], [10, 20, 30]))
    assert result == [1, 10, 2, 20, 3, 30]


def test_interleave_unequal() -> None:
    result = list(st.stream([1, 2]).interleave([10, 20, 30, 40]))
    assert result == [1, 10, 2, 20, 30, 40]


async def test_merge_async() -> None:
    import asyncio

    async def gen1() -> AsyncIterator[int]:
        for i in [1, 3]:
            await asyncio.sleep(0.01)
            yield i

    async def gen2() -> AsyncIterator[int]:
        for i in [2, 4]:
            await asyncio.sleep(0.005)
            yield i

    result = [x async for x in st.merge(gen1(), gen2())]
    assert sorted(result) == [1, 2, 3, 4]
