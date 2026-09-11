"""Tests for combine operations."""

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Iterator
from contextlib import suppress

import pytest

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


def test_zip_no_iterables() -> None:
    zipped: Iterator[tuple[int, ...]] = st.zip()
    assert list(st.take(3, zipped)) == []


async def test_zip_async_no_iterables() -> None:
    zipped: AsyncIterator[tuple[int, ...]] = st.zip_async()
    assert [x async for x in st.take(3, zipped)] == []


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


async def test_merge_close_stops_sources() -> None:
    produced: list[str] = []
    closed = asyncio.Event()

    async def fast() -> AsyncIterator[str]:
        try:
            yield "a0"
        finally:
            closed.set()

    async def slow() -> AsyncIterator[str]:
        await asyncio.sleep(0.03)
        produced.append("b0")
        yield "b0"

    tasks = asyncio.all_tasks()
    merged = st.merge(fast(), slow())
    assert await anext(merged) == "a0"
    assert isinstance(merged, AsyncGenerator)
    await merged.aclose()
    assert closed.is_set()
    assert asyncio.all_tasks() == tasks
    await asyncio.sleep(0.05)
    assert produced == []


async def test_merge_source_error_stops_sources() -> None:
    produced: list[str] = []

    async def failing() -> AsyncIterator[str]:
        yield "a0"
        raise ValueError("boom")

    async def slow() -> AsyncIterator[str]:
        await asyncio.sleep(0.03)
        produced.append("b0")
        yield "b0"

    tasks = asyncio.all_tasks()
    merged = st.merge(failing(), slow())
    assert await anext(merged) == "a0"
    with pytest.raises(ValueError, match="boom"):
        await anext(merged)
    assert asyncio.all_tasks() == tasks
    await asyncio.sleep(0.05)
    assert produced == []


async def test_merge_cancel_stops_sources() -> None:
    produced: list[str] = []

    async def slow(name: str) -> AsyncIterator[str]:
        await asyncio.sleep(0.03)
        produced.append(name)
        yield name

    async def consume() -> None:
        async for _ in st.merge(slow("a0"), slow("b0")):
            pass

    tasks = asyncio.all_tasks()
    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0.01)
    consumer.cancel()
    with suppress(asyncio.CancelledError):
        await consumer
    assert asyncio.all_tasks() == tasks
    await asyncio.sleep(0.05)
    assert produced == []
