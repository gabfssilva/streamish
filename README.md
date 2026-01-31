# streamish

[![PyPI version](https://img.shields.io/pypi/v/streamish)](https://pypi.org/project/streamish/)
[![Python](https://img.shields.io/pypi/pyversions/streamish)](https://pypi.org/project/streamish/)
[![CI](https://github.com/gabfssilva/streamish/actions/workflows/ci.yml/badge.svg)](https://github.com/gabfssilva/streamish/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Iterator and async iterator utilities for Python 3.12+.

## Features

- **Hybrid API**: Fluent chains or standalone functions
- **Unified sync/async**: Same functions work with both iterators and async iterators
- **Type safe**: Full pyright strict mode support
- **Zero dependencies**: stdlib only

## Installation

```bash
pip install streamish
```

## Quick Start

```python
import streamish as st

# Fluent API
result = list(
    st.stream([1, 2, 3, 4, 5])
    .map(lambda x: x * 2)
    .filter(lambda x: x > 4)
    .take(2)
)
# [6, 8]

# Standalone functions
result = list(st.take(2, st.filter(lambda x: x > 4, st.map(lambda x: x * 2, [1, 2, 3, 4, 5]))))
# [6, 8]
```

## Async Support

Functions automatically detect async iterables and async functions:

```python
import streamish as st

async def fetch(url: str) -> Response:
    ...

# Async source
async def urls():
    yield "https://example.com/1"
    yield "https://example.com/2"

# Automatically async
async for response in st.stream(urls()).map(fetch):
    print(response)

# Concurrent execution with map_async
async for response in st.map_async(fetch, urls(), concurrency=10):
    print(response)
```

## Operations

### Transform

| Operation | Description |
|-----------|-------------|
| `map(fn, it)` | Apply function to each element |
| `filter(pred, it)` | Keep elements satisfying predicate |
| `flatten(it)` | Flatten one level of nesting |
| `flat_map(fn, it)` | Map then flatten |
| `enumerate(it, start=0)` | Add index to elements |
| `scan(fn, it, initial=x)` | Cumulative reduce, yielding intermediate values |
| `map_async(fn, it, concurrency=1)` | Concurrent async map, preserving order |

### Filter

| Operation | Description |
|-----------|-------------|
| `take(n, it)` | Take first n elements |
| `skip(n, it)` | Skip first n elements |
| `take_while(pred, it)` | Take while predicate is true |
| `skip_while(pred, it)` | Skip while predicate is true |
| `distinct(it, window=N, timeout=T)` | Remove duplicates (with optional LRU window or expiry) |
| `distinct_by(key_fn, it)` | Remove duplicates by key |

### Group

| Operation | Description |
|-----------|-------------|
| `batch(size, it, timeout=None)` | Group into batches by size or timeout |
| `window(size, it, step=1)` | Sliding window |
| `partition(pred, it)` | Split into (matches, non_matches) |

### Combine

| Operation | Description |
|-----------|-------------|
| `zip(*iterables)` | Zip iterables together |
| `chain(*iterables)` | Chain iterables sequentially |
| `interleave(*iterables)` | Alternate elements round-robin |
| `merge(*async_iterables)` | Merge async iterables, emit as they arrive |

## Examples

### Processing a file line by line

```python
import streamish as st

with open("data.txt") as f:
    result = list(
        st.stream(f)
        .map(str.strip)
        .filter(bool)  # skip empty lines
        .distinct()
        .take(100)
    )
```

### Batching API requests

```python
import streamish as st

async def send_batch(items: list[Item]) -> None:
    ...

async def process(items: AsyncIterable[Item]) -> None:
    async for batch in st.stream(items).batch(100, timeout=5.0):
        await send_batch(batch)
```

### Concurrent HTTP requests

```python
import httpx
import streamish as st

async def fetch(client: httpx.AsyncClient, url: str) -> Response:
    return await client.get(url)

async def main():
    urls = ["https://example.com/1", "https://example.com/2", ...]

    async with httpx.AsyncClient() as client:
        async for response in st.map_async(
            lambda url: fetch(client, url),
            urls,
            concurrency=10,
        ):
            print(response.status_code)
```

### Windowed statistics

```python
import streamish as st

# Moving average over last 5 values
values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

averages = list(
    st.stream(values)
    .window(5)
    .map(lambda w: sum(w) / len(w))
)
# [3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
```

### Merging async streams

```python
import streamish as st

async def stream_a():
    for i in range(3):
        await asyncio.sleep(0.1)
        yield f"a{i}"

async def stream_b():
    for i in range(3):
        await asyncio.sleep(0.15)
        yield f"b{i}"

# Items emitted as they arrive
async for item in st.merge(stream_a(), stream_b()):
    print(item)
# a0, b0, a1, a2, b1, b2 (order depends on timing)
```

## License

MIT
