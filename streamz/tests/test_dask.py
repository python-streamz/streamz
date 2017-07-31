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

from dask.base import normalize_token


@gen_cluster(client=True)
def test_map(c, s, a, b):
    source = Stream()
    futures = scatter(source).map(inc)
    futures_L = futures.sink_to_list()
    L = futures.gather().sink_to_list()

    for i in range(5):
        yield source.emit(i)

    assert L == [1, 2, 3, 4, 5]
    assert all(isinstance(f, Future) for f in futures_L)


@gen_cluster(client=True)
def test_scan(c, s, a, b):
    source = Stream()
    futures = scatter(source).map(inc).scan(add)
    futures_L = futures.sink_to_list()
    L = futures.gather().sink_to_list()

    for i in range(5):
        yield source.emit(i)

    assert L == [1, 3, 6, 10, 15]
    assert all(isinstance(f, Future) for f in futures_L)


@gen_cluster(client=True)
def test_scan_state(c, s, a, b):
    source = Stream()

    def f(acc, i):
        acc = acc + i
        return acc, acc

    L = scatter(source).scan(f, returns_state=True).gather().sink_to_list()
    for i in range(3):
        yield source.emit(i)

    assert L == [0, 1, 3]


@gen_cluster(client=True)
def test_zip(c, s, a, b):
    a = Stream()
    b = Stream()
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
        with Client(s['address'], loop=loop):  # flake8: noqa
            source = Stream()
            L = source.scatter().map(inc).gather().sink_to_list()

            @gen.coroutine
            def f():
                for i in range(10):
                    yield source.emit(i)

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

            assert L == list(map(inc, range(10)))


@pytest.mark.slow
@gen_cluster(client=True, ncores=[('127.0.0.1', 1)] * 2)
def test_buffer(c, s, a, b):
    source = Stream()
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
                print(i)
            end = time.time()
            assert end - start < 0.5

            for i in range(5, 10):
                source.emit(i)
            end2 = time.time()

            while len(L) < 10:
                time.sleep(0.01)
                assert time.time() - start < 5

            assert L == list(map(inc, range(10)))

@gen_cluster(client=True)
def test_obj(c, s, a, b):
    ''' Test pickling of an arbitrary object, as well as caching.
        Some requirements :
            1. the object constructor must either be locally importable or
            provided. (Here it's provided)

        This test attempts to test the following two ideas:
            1. An arbitrary object is picklable
            2. An arbitrary object with a defined token (normalize_token) is
            cached (default is not to cache, each future is assigned a random
            hash)
    '''
    class myObj:
        def __init__(self, *args):
            if len(args) == 1:
                obj = args[0]
                self.a = obj.a
                self.b = obj.b
            elif len(args) == 2:
                a, b = args[0], args[1]
                self.a = a
                self.b = b
            else:
                raise ValueError("Num args must be 1 or 2")

    @normalize_token.register(myObj)
    def tokenize_myObj(obj):
        ''' just tokenize a, ignore b.
            This is meant to test that we can cache same object based on a
        '''
        return obj.a,

    def inca(tt):
        ''' increment member a'''
        cls, obj = tt
        # make a copy
        obj2 = cls(obj)
        obj2.a = obj.a + 1
        return cls, obj2


    def incb(tt):
        ''' increment member b'''
        cls, obj = tt
        # make a copy
        obj2 = cls(obj)
        obj2.b = obj.b + 1
        return cls, obj2

    source = Stream()
    futures = scatter(source).map(inca).map(incb)
    # cache results
    futures_L = futures.sink_to_list()
    L = futures.gather().sink_to_list()

    obj1 = myObj, myObj(1,2)
    obj2 = myObj, myObj(1,4)
    obj3 = myObj, myObj(2,4)
    yield source.emit(obj1)
    yield source.emit(obj2)
    yield source.emit(obj3)
    time.sleep(.1)

    assert L[0][1].b == 3
    # should have used cached result, not actual
    assert L[1][1].b == 3
    assert L[2][1].b == 5
