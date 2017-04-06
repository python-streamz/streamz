from operator import add
from time import time

import pytest

from distributed.client import Future
from distributed.utils_test import inc, double, gen_test, gen_cluster
from distributed.utils import tmpfile
from tornado import gen
from tornado.queues import Queue

from ..core import *
from ..dask import *
from ..sources import *

@gen_cluster(client=True)
def test_scatter_gather(c, s, a, b):
    source = Stream()
    a = scatter(source)
    b = gather(a)

    L1 = sink_to_list(a)
    L2 = sink_to_list(b)

    for i in range(50):
        yield source.emit(i)

    yield a.flush()
    yield b.flush()

    assert len(L1) == 50
    assert all(isinstance(x, Future) for x in L1)

    results = yield c._gather(L1)
    assert results == L2 == list(range(50))
