"""Tests for transform operations."""

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import suppress

import pytest

import streamish as st


def test_map_standalone_sync() -> None:
    result = list(st.map(lambda x: x * 2, [1, 2, 3]))
    assert result == [2, 4, 6]


async def test_map_standalone_async_source() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in [1, 2, 3]:
            yield i

    result = [x async for x in st.map(lambda x: x * 2, gen())]
    assert result == [2, 4, 6]


async def test_map_standalone_async_fn() -> None:
    async def double(x: int) -> int:
        return x * 2

    result = [x async for x in st.map(double, [1, 2, 3])]
    assert result == [2, 4, 6]


def test_map_fluent_sync() -> None:
    result = list(st.stream([1, 2, 3]).map(lambda x: x * 2))
    assert result == [2, 4, 6]


async def test_map_fluent_async() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in [1, 2, 3]:
            yield i

    result = [x async for x in st.stream(gen()).map(lambda x: x * 2)]
    assert result == [2, 4, 6]


def test_map_chained() -> None:
    result = list(st.stream([1, 2, 3]).map(lambda x: x * 2).map(lambda x: x + 1))
    assert result == [3, 5, 7]


def test_filter_standalone_sync() -> None:
    result = list(st.filter(lambda x: x % 2 == 0, [1, 2, 3, 4]))
    assert result == [2, 4]


async def test_filter_standalone_async_source() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in [1, 2, 3, 4]:
            yield i

    result = [x async for x in st.filter(lambda x: x % 2 == 0, gen())]
    assert result == [2, 4]


def test_filter_fluent_sync() -> None:
    result = list(st.stream([1, 2, 3, 4]).filter(lambda x: x % 2 == 0))
    assert result == [2, 4]


def test_map_filter_chain() -> None:
    result = list(st.stream([1, 2, 3, 4]).map(lambda x: x * 2).filter(lambda x: x > 4))
    assert result == [6, 8]


def test_flatten_sync() -> None:
    result = list(st.flatten([[1, 2], [3, 4], [5]]))
    assert result == [1, 2, 3, 4, 5]


def test_flatten_fluent() -> None:
    result = list(st.stream([[1, 2], [3]]).flatten())
    assert result == [1, 2, 3]


def test_flat_map_sync() -> None:
    result = list(st.flat_map(lambda x: [x, x * 2], [1, 2, 3]))
    assert result == [1, 2, 2, 4, 3, 6]


def test_flat_map_fluent() -> None:
    result = list(st.stream([1, 2]).flat_map(lambda x: range(x)))
    assert result == [0, 0, 1]


def test_enumerate_sync() -> None:
    result = list(st.enumerate(["a", "b", "c"]))
    assert result == [(0, "a"), (1, "b"), (2, "c")]


def test_enumerate_with_start() -> None:
    result = list(st.stream(["a", "b"]).enumerate(start=1))
    assert result == [(1, "a"), (2, "b")]


def test_scan_sync() -> None:
    result = list(st.scan(lambda acc, x: acc + x, [1, 2, 3, 4], initial=0))
    assert result == [1, 3, 6, 10]


def test_scan_fluent() -> None:
    result = list(st.stream([1, 2, 3]).scan(lambda acc, x: acc * x, initial=1))
    assert result == [1, 2, 6]


async def test_map_async_concurrency() -> None:
    call_times: list[float] = []

    async def slow_fn(x: int) -> int:
        call_times.append(asyncio.get_event_loop().time())
        await asyncio.sleep(0.1)
        return x * 2

    start = asyncio.get_event_loop().time()
    result = [x async for x in st.map_async(slow_fn, [1, 2, 3, 4], concurrency=2)]
    elapsed = asyncio.get_event_loop().time() - start

    assert sorted(result) == [2, 4, 6, 8]
    assert elapsed < 0.35


async def test_map_async_preserves_order() -> None:
    async def delayed(x: int) -> int:
        await asyncio.sleep(0.05 * (4 - x))
        return x

    result = [x async for x in st.map_async(delayed, [1, 2, 3, 4], concurrency=4)]
    assert result == [1, 2, 3, 4]


async def test_map_async_fluent() -> None:
    async def double(x: int) -> int:
        return x * 2

    result = [x async for x in st.stream([1, 2, 3]).map_async(double, concurrency=2)]
    assert result == [2, 4, 6]


async def test_map_async_close_cancels_calls() -> None:
    async def fn(x: int) -> int:
        if x > 1:
            await asyncio.Event().wait()
        return x

    tasks = asyncio.all_tasks()
    results = st.map_async(fn, [1, 2, 3, 4], concurrency=3)
    assert await anext(results) == 1
    assert isinstance(results, AsyncGenerator)
    await results.aclose()
    assert asyncio.all_tasks() == tasks


async def test_map_async_cancel_cancels_calls() -> None:
    async def fn(x: int) -> int:
        await asyncio.Event().wait()
        return x

    async def consume() -> None:
        async for _ in st.map_async(fn, [1, 2, 3], concurrency=2):
            pass

    tasks = asyncio.all_tasks()
    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0.01)
    consumer.cancel()
    with suppress(asyncio.CancelledError):
        await consumer
    assert asyncio.all_tasks() == tasks


async def test_map_async_fn_error_cancels_calls() -> None:
    async def fn(x: int) -> int:
        if x == 1:
            raise ValueError("boom")
        await asyncio.Event().wait()
        return x

    tasks = asyncio.all_tasks()
    with pytest.raises(ValueError, match="boom"):
        await anext(st.map_async(fn, [1, 2, 3], concurrency=3))
    assert asyncio.all_tasks() == tasks


async def test_map_async_fn_error_fails_fast() -> None:
    async def fn(x: int) -> int:
        if x == 2:
            raise ValueError("boom")
        await asyncio.sleep(1)
        return x

    loop = asyncio.get_running_loop()
    tasks = asyncio.all_tasks()
    start = loop.time()
    with pytest.raises(ValueError, match="boom"):
        await anext(st.map_async(fn, [1, 2, 3], concurrency=3))
    assert loop.time() - start < 0.5
    assert asyncio.all_tasks() == tasks


async def test_map_async_source_error_cancels_calls() -> None:
    async def fn(x: int) -> int:
        await asyncio.Event().wait()
        return x

    async def failing() -> AsyncIterator[int]:
        yield 1
        raise ValueError("boom")

    tasks = asyncio.all_tasks()
    with pytest.raises(ValueError, match="boom"):
        await anext(st.map_async(fn, failing(), concurrency=2))
    assert asyncio.all_tasks() == tasks


async def test_map_async_bounds_read_ahead() -> None:
    started: list[int] = []

    async def fn(x: int) -> int:
        started.append(x)
        if x == 0:
            await asyncio.sleep(0.01)
        return x

    results = st.map_async(fn, range(50), concurrency=2)
    assert await anext(results) == 0
    assert started == [0, 1]
    assert [x async for x in results] == list(range(1, 50))


def test_map_async_validates_concurrency_eagerly() -> None:
    async def fn(x: int) -> int:
        return x

    with pytest.raises(ValueError, match="concurrency"):
        st.map_async(fn, [1], concurrency=0)
