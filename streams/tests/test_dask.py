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
    L1 = source.scatter(source).sink_to_list()
    L2 = a.gather().sink_to_list()

    for i in range(50):
        yield source.emit(i)

    yield a.flush()
    yield b.flush()

    assert len(L1) == 50
    assert all(isinstance(x, Future) for x in L1)

    results = yield c._gather(L1)
    assert results == L2 == list(range(50))


@gen_cluster(client=True)
def test_map(c, s, a, b):
    source = Stream()
    a = ds.map(inc, source)

    L = sink_to_list(a)

    for i in range(3):
        source.emit(i)

    assert len(L) == 3
    assert all(isinstance(x, Future) for x in L)

    results = yield c._gather(L)
    assert results == [1, 2, 3]


@gen_cluster(client=True)
def test_scan(c, s, a, b):
    source = Stream()
    a = ds.scan(add, source, start=0)

    L = sink_to_list(a)

    for i in range(3):
        source.emit(i)

    assert len(L) == 3
    assert all(isinstance(x, Future) for x in L)

    results = yield c._gather(L)
    assert results == [0, 1, 3]
