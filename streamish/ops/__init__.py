"""Stream operations."""

from streamish.ops.combine import chain, chain_async, interleave, merge, zip_async
from streamish.ops.combine import zip_ as zip
from streamish.ops.filter import (
    distinct,
    distinct_by,
    skip,
    skip_while,
    take,
    take_while,
)
from streamish.ops.group import batch, partition, partition_async, window
from streamish.ops.transform import enumerate_ as enumerate
from streamish.ops.transform import filter_ as filter
from streamish.ops.transform import flat_map, flatten, map_async, scan
from streamish.ops.transform import map_ as map

__all__ = [
    "map",
    "filter",
    "take",
    "skip",
    "take_while",
    "skip_while",
    "distinct",
    "distinct_by",
    "enumerate",
    "scan",
    "flatten",
    "flat_map",
    "batch",
    "window",
    "partition",
    "partition_async",
    "zip",
    "zip_async",
    "chain",
    "chain_async",
    "interleave",
    "merge",
    "map_async",
]
