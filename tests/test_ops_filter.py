"""Tests for filter operations."""

import asyncio
import time
from collections.abc import AsyncIterator

import pytest

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


def test_take_sync_leaves_rest_in_source() -> None:
    src = iter([1, 2, 3, 4])
    result = list(st.take(2, src))
    assert result == [1, 2]
    assert list(src) == [3, 4]


def test_take_non_positive_sync_pulls_nothing() -> None:
    for n in (0, -1):
        src = iter([1, 2, 3])
        result = list(st.take(n, src))
        assert result == []
        assert list(src) == [1, 2, 3]


async def test_take_async_leaves_rest_in_source() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in [1, 2, 3, 4]:
            yield i

    src = gen()
    result = [x async for x in st.take(2, src)]
    assert result == [1, 2]
    assert [x async for x in src] == [3, 4]


async def test_take_non_positive_async_pulls_nothing() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in [1, 2, 3]:
            yield i

    for n in (0, -1):
        src = gen()
        result = [x async for x in st.take(n, src)]
        assert result == []
        assert [x async for x in src] == [1, 2, 3]


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
    result: list[int] = []
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


def test_distinct_by_with_window() -> None:
    # Window of 2: key can reappear after 2 new keys
    data = [{"id": 1}, {"id": 2}, {"id": 1}, {"id": 3}, {"id": 1}]
    result = list(st.distinct_by(lambda x: x["id"], data, window=2))
    # id=1 seen, id=2 seen, id=1 still in window (skip), id=3 seen (id=1 pushed out), id=1 not in window (yield)
    assert result == [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 1}]


def test_distinct_by_with_window_fluent() -> None:
    result = list(st.stream(["a", "bb", "a", "ccc", "a"]).distinct_by(len, window=2))
    # len=1 seen, len=2 seen, len=1 still in window (skip), len=3 seen (len=1 pushed out), len=1 not in window (yield)
    assert result == ["a", "bb", "ccc", "a"]


def test_distinct_by_with_timeout() -> None:
    data = [{"id": 1}, {"id": 2}, {"id": 1}, {"id": 3}, {"id": 1}]
    result: list[dict[str, int]] = []
    for item in st.distinct_by(lambda x: x["id"], data, timeout=0.05):
        result.append(item)
        if item["id"] == 2:
            time.sleep(0.1)  # Wait for id=1 to expire

    # id=1 seen, id=2 seen, sleep 0.1s, id=1 expired (yield), id=3 seen, id=1 still fresh (skip)
    assert result == [{"id": 1}, {"id": 2}, {"id": 1}, {"id": 3}]


async def test_distinct_by_with_window_async() -> None:
    async def gen() -> AsyncIterator[dict[str, int]]:
        for item in [{"id": 1}, {"id": 2}, {"id": 1}, {"id": 3}, {"id": 1}]:
            yield item

    result = [x async for x in st.distinct_by(lambda x: x["id"], gen(), window=2)]
    assert result == [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 1}]


def test_distinct_with_window_and_timeout() -> None:
    result = list(st.distinct(["a", "b", "a"], window=2, timeout=60))
    assert result == ["a", "b"]


async def test_distinct_with_window_and_timeout_async() -> None:
    async def gen() -> AsyncIterator[str]:
        for item in ["a", "b", "a"]:
            yield item

    result = [x async for x in st.distinct(gen(), window=2, timeout=60)]
    assert result == ["a", "b"]


def test_distinct_by_with_window_and_timeout() -> None:
    result = list(st.distinct_by(str.lower, ["a", "b", "A"], window=2, timeout=60))
    assert result == ["a", "b"]


async def test_distinct_by_with_window_and_timeout_async() -> None:
    async def gen() -> AsyncIterator[str]:
        for item in ["a", "b", "A"]:
            yield item

    result = [x async for x in st.distinct_by(str.lower, gen(), window=2, timeout=60)]
    assert result == ["a", "b"]


def test_distinct_window_keeps_key_reemitted_after_timeout() -> None:
    result: list[str] = []
    for item in st.distinct(["a", "a", "b", "a"], window=2, timeout=0.05):
        result.append(item)
        if result == ["a"]:
            time.sleep(0.1)  # Wait for "a" to expire

    # a seen, sleep 0.1s, a expired (yield), b seen, a in window and fresh (skip)
    assert result == ["a", "a", "b"]


async def test_distinct_window_keeps_key_reemitted_after_timeout_async() -> None:
    async def gen() -> AsyncIterator[str]:
        for item in ["a", "a", "b", "a"]:
            yield item

    result: list[str] = []
    async for item in st.distinct(gen(), window=2, timeout=0.05):
        result.append(item)
        if result == ["a"]:
            await asyncio.sleep(0.1)  # Wait for "a" to expire

    assert result == ["a", "a", "b"]


def test_distinct_by_window_keeps_key_reemitted_after_timeout() -> None:
    result: list[str] = []
    for item in st.distinct_by(str.lower, ["a", "A", "b", "a"], window=2, timeout=0.05):
        result.append(item)
        if result == ["a"]:
            time.sleep(0.1)  # Wait for key "a" to expire

    # a seen, sleep 0.1s, A expired (yield), b seen, a in window and fresh (skip)
    assert result == ["a", "A", "b"]


async def test_distinct_by_window_keeps_key_reemitted_after_timeout_async() -> None:
    async def gen() -> AsyncIterator[str]:
        for item in ["a", "A", "b", "a"]:
            yield item

    result: list[str] = []
    async for item in st.distinct_by(str.lower, gen(), window=2, timeout=0.05):
        result.append(item)
        if result == ["a"]:
            await asyncio.sleep(0.1)  # Wait for key "a" to expire

    assert result == ["a", "A", "b"]


invalid_window_or_timeout = pytest.mark.parametrize(
    ("window", "timeout", "message"),
    [
        (0, None, "window must be positive"),
        (-1, None, "window must be positive"),
        (None, 0, "timeout must be positive"),
        (None, -0.5, "timeout must be positive"),
    ],
)


@invalid_window_or_timeout
def test_distinct_rejects_invalid_window_or_timeout(
    window: int | None, timeout: float | None, message: str
) -> None:
    async def gen() -> AsyncIterator[str]:
        yield "a"

    with pytest.raises(ValueError, match=message):
        st.distinct(["a", "a"], window=window, timeout=timeout)
    with pytest.raises(ValueError, match=message):
        st.distinct(gen(), window=window, timeout=timeout)


@invalid_window_or_timeout
def test_distinct_by_rejects_invalid_window_or_timeout(
    window: int | None, timeout: float | None, message: str
) -> None:
    async def gen() -> AsyncIterator[str]:
        yield "a"

    with pytest.raises(ValueError, match=message):
        st.distinct_by(str.lower, ["a", "a"], window=window, timeout=timeout)
    with pytest.raises(ValueError, match=message):
        st.distinct_by(str.lower, gen(), window=window, timeout=timeout)
