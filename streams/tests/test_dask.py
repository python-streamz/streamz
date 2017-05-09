from operator import add
from time import time

import pytest

from distributed.client import Future
from distributed.utils_test import inc, double, gen_test, gen_cluster
from distributed.utils import tmpfile
from tornado import gen
from tornado.queues import Queue

from ..core import *
import streams.dask as ds
from ..sources import *


@gen_cluster(client=True)
def test_scatter_gather(c, s, a, b):
    source = Stream()
    a = ds.scatter(source)
    b = ds.gather(a)
    L1 = a.sink_to_list()
    L2 = b.sink_to_list()
    L3 = b.map(inc).sink_to_list()

    for i in range(50):
        yield source.emit(i)

    yield a.flush()
    yield b.flush()

    assert len(L1) == 50
    assert all(isinstance(x, Future) for x in L1)

    results = yield c._gather(L1)
    assert results == L2 == list(range(50))

    assert L3 == list(range(1, 51))


@gen_cluster(client=True)
def test_map(c, s, a, b):
    source = Stream()
    L = source.map(inc).sink_to_list()

    futures = yield c._scatter(list(range(3)))
    for f in futures:
        source.emit(f)

    assert len(L) == 3
    assert all(isinstance(x, Future) for x in L)

    results = yield c._gather(L)
    assert results == [1, 2, 3]


@gen_cluster(client=True)
def test_scan(c, s, a, b):
    source = Stream()
    L = source.scan(add, start=0).sink_to_list()

    futures = yield c._scatter(list(range(3)))
    for f in futures:
        source.emit(f)

    assert len(L) == 3
    assert all(isinstance(x, Future) for x in L)

    results = yield c._gather(L)
    assert results == [0, 1, 3]
