import asyncio
from operator import add
import random
import time

import pytest
pytest.importorskip('dask.distributed')

from tornado import gen

from streamz.dask import scatter
from streamz import RefCounter, Stream

from distributed import Future, Client
from distributed.utils import sync
from distributed.utils_test import gen_cluster, inc, cluster, loop, slowinc  # noqa: F401


@gen_cluster(client=True)
async def test_map(c, s, a, b):
    source = Stream(asynchronous=True)
    futures = scatter(source).map(inc)
    futures_L = futures.sink_to_list()
    L = futures.gather().sink_to_list()

    for i in range(5):
        await source.emit(i)

    assert L == [1, 2, 3, 4, 5]
    assert all(isinstance(f, Future) for f in futures_L)


@gen_cluster(client=True)
async def test_map_on_dict(c, s, a, b):
    # dask treats dicts differently, so we have to make sure
    # the user sees no difference in the streamz api.
    # Regression test against #336
    def add_to_dict(d):
        d["x"] = d["i"]
        return d

    source = Stream(asynchronous=True)
    futures = source.scatter().map(add_to_dict)
    L = futures.gather().sink_to_list()

    for i in range(5):
        await source.emit({"i": i})

    assert len(L) == 5
    for i, item in enumerate(sorted(L, key=lambda x: x["x"])):
        assert item["x"] == i
        assert item["i"] == i


@gen_cluster(client=True)
async def test_partition_then_scatter_async(c, s, a, b):
    # Ensure partition w/ timeout before scatter works correctly for
    # asynchronous
    start = time.monotonic()
    source = Stream(asynchronous=True)

    L = source.partition(2, timeout=.1).scatter().map(
            lambda x: [xx+1 for xx in x]).buffer(2).gather().flatten().sink_to_list()

    rc = RefCounter(loop=source.loop)
    for i in range(3):
        await source.emit(i, metadata=[{'ref': rc}])

    while rc.count != 0 and time.monotonic() - start < 1.:
        await gen.sleep(1e-2)

    assert L == [1, 2, 3]


def test_partition_then_scatter_sync(loop):
    # Ensure partition w/ timeout before scatter works correctly for synchronous
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:  # noqa: F841
            start = time.monotonic()
            source = Stream()
            L = source.partition(2, timeout=.1).scatter().map(
                    lambda x: [xx+1 for xx in x]).gather().flatten().sink_to_list()
            assert source.loop is client.loop

            rc = RefCounter()
            for i in range(3):
                source.emit(i, metadata=[{'ref': rc}])

            while rc.count != 0 and time.monotonic() - start < 2.:
                time.sleep(1e-2)

            assert L == [1, 2, 3]


@gen_cluster(client=True)
async def test_non_unique_emit(c, s, a, b):
    """Regression for https://github.com/python-streamz/streams/issues/397

    Non-unique stream entries still need to each be processed.
    """
    source = Stream(asynchronous=True)
    futures = source.scatter().map(lambda x: random.random())
    L = futures.gather().sink_to_list()

    for _ in range(3):
        # Emit non-unique values
        await source.emit(0)

    assert len(L) == 3
    assert L[0] != L[1] or L[0] != L[2]


@gen_cluster(client=True)
async def test_scan(c, s, a, b):
    source = Stream(asynchronous=True)
    futures = scatter(source).map(inc).scan(add)
    futures_L = futures.sink_to_list()
    L = futures.gather().sink_to_list()

    for i in range(5):
        await source.emit(i)

    assert L == [1, 3, 6, 10, 15]
    assert all(isinstance(f, Future) for f in futures_L)


@gen_cluster(client=True)
async def test_scan_state(c, s, a, b):
    source = Stream(asynchronous=True)

    def f(acc, i):
        acc = acc + i
        return acc, acc

    L = scatter(source).scan(f, returns_state=True).gather().sink_to_list()
    for i in range(3):
        await source.emit(i)

    assert L == [0, 1, 3]


@gen_cluster(client=True)
async def test_zip(c, s, a, b):
    a = Stream(asynchronous=True)
    b = Stream(asynchronous=True)
    c = scatter(a).zip(scatter(b))

    L = c.gather().sink_to_list()

    await a.emit(1)
    await b.emit('a')
    await a.emit(2)
    await b.emit('b')

    assert L == [(1, 'a'), (2, 'b')]


@gen_cluster(client=True)
async def test_accumulate(c, s, a, b):
    source = Stream(asynchronous=True)
    L = source.scatter().accumulate(lambda acc, x: acc + x, with_state=True).gather().sink_to_list()
    for i in range(3):
        await source.emit(i)
    assert L[-1][1] == 3


def test_sync(loop):  # noqa: F811
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:  # noqa: F841
            source = Stream()
            L = source.scatter().map(inc).gather().sink_to_list()

            async def f():
                for i in range(10):
                    await source.emit(i, asynchronous=True)

            sync(loop, f)

            assert L == list(map(inc, range(10)))


def test_sync_2(loop):  # noqa: F811
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop):  # noqa: F841
            source = Stream()
            L = source.scatter().map(inc).gather().sink_to_list()

            for i in range(10):
                source.emit(i)
                assert len(L) == i + 1

            assert L == list(map(inc, range(10)))


@gen_cluster(client=True, nthreads=[('127.0.0.1', 1)] * 2)
async def test_buffer(c, s, a, b):
    source = Stream(asynchronous=True)
    L = source.scatter().map(slowinc, delay=0.5).buffer(5).gather().sink_to_list()

    start = time.time()
    for i in range(5):
        await source.emit(i)
    end = time.time()
    assert end - start < 0.5

    for i in range(5, 10):
        await source.emit(i)

    end2 = time.time()
    assert end2 - start > (0.5 / 3)

    while len(L) < 10:
        await gen.sleep(0.01)
        assert time.time() - start < 5

    assert L == list(map(inc, range(10)))

    assert source.loop == c.loop


def test_buffer_sync(loop):  # noqa: F811
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:  # noqa: F841
            source = Stream()
            buff = source.scatter().map(slowinc, delay=0.5).buffer(5)
            L = buff.gather().sink_to_list()

            start = time.time()
            for i in range(5):
                source.emit(i)
            end = time.time()
            assert end - start < 0.5

            for i in range(5, 10):
                source.emit(i)

            while len(L) < 10:
                time.sleep(0.01)
                assert time.time() - start < 5

            assert L == list(map(inc, range(10)))


@pytest.mark.xfail(reason='')
async def test_stream_shares_client_loop(loop):  # noqa: F811
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:  # noqa: F841
            source = Stream()
            d = source.timed_window('20ms').scatter()  # noqa: F841
            assert source.loop is client.loop


@gen_cluster(client=True)
async def test_starmap(c, s, a, b):
    def add(x, y, z=0):
        return x + y + z

    source = Stream(asynchronous=True)
    L = source.scatter().starmap(add, z=10).gather().sink_to_list()

    for i in range(5):
        await source.emit((i, i))

    assert L == [10, 12, 14, 16, 18]
