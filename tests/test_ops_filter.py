"""Tests for filter operations."""

import time
from collections.abc import AsyncIterator

import streamish as st


def test_take_sync() -> None:
    result = list(st.take(2, [1, 2, 3, 4, 5]))
    assert result == [1, 2]


def test_take_fluent() -> None:
    result = list(st.stream([1, 2, 3, 4, 5]).take(2))
    assert result == [1, 2]


async def test_take_async() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in range(10):
            yield i

    result = [x async for x in st.take(3, gen())]
    assert result == [0, 1, 2]


def test_skip_sync() -> None:
    result = list(st.skip(2, [1, 2, 3, 4, 5]))
    assert result == [3, 4, 5]


def test_skip_fluent() -> None:
    result = list(st.stream([1, 2, 3, 4, 5]).skip(2))
    assert result == [3, 4, 5]


async def test_skip_async() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in range(5):
            yield i

    result = [x async for x in st.skip(2, gen())]
    assert result == [2, 3, 4]


def test_take_skip_chain() -> None:
    result = list(st.stream(range(10)).skip(2).take(3))
    assert result == [2, 3, 4]


def test_take_while_sync() -> None:
    result = list(st.take_while(lambda x: x < 4, [1, 2, 3, 4, 5, 1]))
    assert result == [1, 2, 3]


def test_take_while_fluent() -> None:
    result = list(st.stream([1, 2, 3, 4, 5]).take_while(lambda x: x < 3))
    assert result == [1, 2]


def test_skip_while_sync() -> None:
    result = list(st.skip_while(lambda x: x < 3, [1, 2, 3, 4, 5]))
    assert result == [3, 4, 5]


def test_skip_while_fluent() -> None:
    result = list(st.stream([1, 2, 3, 2, 1]).skip_while(lambda x: x < 3))
    assert result == [3, 2, 1]


def test_distinct_sync() -> None:
    result = list(st.distinct([1, 2, 2, 3, 1, 4]))
    assert result == [1, 2, 3, 4]


def test_distinct_fluent() -> None:
    result = list(st.stream([1, 1, 2, 2, 3]).distinct())
    assert result == [1, 2, 3]


def test_distinct_by_sync() -> None:
    data = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 1, "v": "c"}]
    result = list(st.distinct_by(lambda x: x["id"], data))
    assert result == [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]


def test_distinct_by_fluent() -> None:
    result = list(st.stream(["a", "bb", "c", "dd"]).distinct_by(len))
    assert result == ["a", "bb"]


def test_distinct_with_window() -> None:
    # Window of 3: element can reappear after 3 new elements
    result = list(st.distinct([1, 2, 3, 1, 4, 1], window=3))
    # 1 seen, 2 seen, 3 seen, 1 still in window (skip), 4 seen (1 pushed out), 1 not in window (yield)
    assert result == [1, 2, 3, 4, 1]


def test_distinct_with_window_fluent() -> None:
    result = list(st.stream([1, 2, 1, 3, 1]).distinct(window=2))
    # 1 seen, 2 seen, 1 still in window (skip), 3 seen (1 pushed out), 1 not in window (yield)
    assert result == [1, 2, 3, 1]


def test_distinct_with_timeout() -> None:
    result = []
    for item in st.distinct([1, 2, 1, 3, 1], timeout=0.05):
        result.append(item)
        if item == 2:
            time.sleep(0.1)  # Wait for 1 to expire

    # 1 seen, 2 seen, sleep 0.1s, 1 expired (yield), 3 seen, 1 still fresh (skip)
    assert result == [1, 2, 1, 3]


async def test_distinct_with_window_async() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in [1, 2, 3, 1, 4, 1]:
            yield i

    result = [x async for x in st.distinct(gen(), window=3)]
    assert result == [1, 2, 3, 4, 1]
