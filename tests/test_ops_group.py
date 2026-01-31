"""Tests for group operations."""

import asyncio
from collections.abc import AsyncIterator

import streamish as st


def test_batch_sync() -> None:
    result = list(st.batch(2, [1, 2, 3, 4, 5]))
    assert result == [[1, 2], [3, 4], [5]]


def test_batch_fluent() -> None:
    result = list(st.stream(range(7)).batch(3))
    assert result == [[0, 1, 2], [3, 4, 5], [6]]


async def test_batch_async() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in range(5):
            yield i

    result = [x async for x in st.batch(2, gen())]
    assert result == [[0, 1], [2, 3], [4]]


async def test_batch_with_timeout() -> None:
    async def slow_gen() -> AsyncIterator[int]:
        for i in range(3):
            yield i
            if i < 2:
                await asyncio.sleep(0.15)

    result = [x async for x in st.batch(10, slow_gen(), timeout=0.1)]
    assert result == [[0], [1], [2]]


def test_window_sync() -> None:
    result = list(st.window(3, [1, 2, 3, 4, 5]))
    assert result == [[1, 2, 3], [2, 3, 4], [3, 4, 5]]


def test_window_with_step() -> None:
    result = list(st.stream([1, 2, 3, 4, 5, 6]).window(3, step=2))
    assert result == [[1, 2, 3], [3, 4, 5]]


def test_window_size_larger_than_input() -> None:
    result = list(st.window(5, [1, 2, 3]))
    assert result == []


def test_partition_sync() -> None:
    evens, odds = st.partition(lambda x: x % 2 == 0, [1, 2, 3, 4, 5])
    assert evens == [2, 4]
    assert odds == [1, 3, 5]


def test_partition_fluent() -> None:
    evens, odds = st.stream(range(6)).partition(lambda x: x % 2 == 0)
    assert evens == [0, 2, 4]
    assert odds == [1, 3, 5]


async def test_partition_async() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in range(5):
            yield i

    evens, odds = await st.partition_async(lambda x: x % 2 == 0, gen())
    assert evens == [0, 2, 4]
    assert odds == [1, 3]
