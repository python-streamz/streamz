from operator import add
import time

import pytest
pytest.importorskip('dask.distributed')

from tornado import gen

from streamz.dask import scatter
from streamz import Stream

from distributed import Future, Client
from distributed.utils import sync
from distributed.utils_test import gen_cluster, inc, cluster, loop, slowinc  # flake8: noqa


@gen_cluster(client=True)
def test_map(c, s, a, b):
    source = Stream(asynchronous=True)
    futures = scatter(source).map(inc)
    futures_L = futures.sink_to_list()
    L = futures.gather().sink_to_list()

    for i in range(5):
        yield source.emit(i)

    assert L == [1, 2, 3, 4, 5]
    assert all(isinstance(f, Future) for f in futures_L)


@gen_cluster(client=True)
def test_scan(c, s, a, b):
    source = Stream(asynchronous=True)
    futures = scatter(source).map(inc).scan(add)
    futures_L = futures.sink_to_list()
    L = futures.gather().sink_to_list()

    for i in range(5):
        yield source.emit(i)

    assert L == [1, 3, 6, 10, 15]
    assert all(isinstance(f, Future) for f in futures_L)


@gen_cluster(client=True)
def test_scan_state(c, s, a, b):
    source = Stream(asynchronous=True)

    def f(acc, i):
        acc = acc + i
        return acc, acc

    L = scatter(source).scan(f, returns_state=True).gather().sink_to_list()
    for i in range(3):
        yield source.emit(i)

    assert L == [0, 1, 3]


@gen_cluster(client=True)
def test_zip(c, s, a, b):
    a = Stream(asynchronous=True)
    b = Stream(asynchronous=True)
    c = scatter(a).zip(scatter(b))

    L = c.gather().sink_to_list()

    yield a.emit(1)
    yield b.emit('a')
    yield a.emit(2)
    yield b.emit('b')

    assert L == [(1, 'a'), (2, 'b')]


@pytest.mark.slow
def test_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:  # flake8: noqa
            source = Stream()
            L = source.scatter().map(inc).gather().sink_to_list()

            @gen.coroutine
            def f():
                for i in range(10):
                    yield source.emit(i, asynchronous=True)

            sync(loop, f)

            assert L == list(map(inc, range(10)))


@pytest.mark.slow
def test_sync_2(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop):  # flake8: noqa
            source = Stream()
            L = source.scatter().map(inc).gather().sink_to_list()

            for i in range(10):
                source.emit(i)
                assert len(L) == i + 1

            assert L == list(map(inc, range(10)))


@pytest.mark.slow
@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_buffer(c, s, a, b):
    source = Stream(asynchronous=True)
    L = source.scatter().map(slowinc, delay=0.5).buffer(5).gather().sink_to_list()

    start = time.time()
    for i in range(5):
        yield source.emit(i)
    end = time.time()
    assert end - start < 0.5

    for i in range(5, 10):
        yield source.emit(i)

    end2 = time.time()
    assert end2 - start > (0.5 / 3)

    while len(L) < 10:
        yield gen.sleep(0.01)
        assert time.time() - start < 5

    assert L == list(map(inc, range(10)))

    assert source.loop == c.loop


@pytest.mark.slow
def test_buffer_sync(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:  # flake8: noqa
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
            end2 = time.time()

            while len(L) < 10:
                time.sleep(0.01)
                assert time.time() - start < 5

            assert L == list(map(inc, range(10)))


@pytest.mark.xfail(reason='')
@pytest.mark.slow
def test_stream_shares_client_loop(loop):
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as client:  # flake8: noqa
            source = Stream()
            d = source.timed_window('20ms').scatter()
            assert source.loop is client.loop


@gen_cluster(client=True)
def test_starmap(c, s, a, b):
    def add(x, y, z=0):
        return x + y + z

    source = Stream(asynchronous=True)
    L = source.scatter().starmap(add, z=10).gather().sink_to_list()

    for i in range(5):
        yield source.emit((i, i))

    assert L == [10, 12, 14, 16, 18]
