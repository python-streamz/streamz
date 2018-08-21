from concurrent.futures import Future
from operator import add

from distributed.utils_test import inc  # flake8: noqa
from streamz import Stream
from streamz.thread import thread_scatter as scatter


# from tornado import gen


def test_map():
    source = Stream(asynchronous=True)
    futures = scatter(source).map(inc)
    futures_L = futures.sink_to_list()
    L = futures.gather().sink_to_list()

    for i in range(5):
        source.emit(i)

    assert L == [1, 2, 3, 4, 5]
    assert all(isinstance(f, Future) for f in futures_L)


def test_scan():
    source = Stream(asynchronous=True)
    futures = scatter(source).map(inc).scan(add)
    futures_L = futures.sink_to_list()
    L = futures.gather().sink_to_list()

    for i in range(5):
        source.emit(i)

    assert L == [1, 3, 6, 10, 15]
    assert all(isinstance(f, Future) for f in futures_L)


def test_scan_state():
    source = Stream(asynchronous=True)

    def f(acc, i):
        acc = acc + i
        return acc, acc

    L = scatter(source).scan(f, returns_state=True).gather().sink_to_list()
    for i in range(3):
        source.emit(i)

    assert L == [0, 1, 3]


def test_zip():
    a = Stream(asynchronous=True)
    b = Stream(asynchronous=True)
    c = scatter(a).zip(scatter(b))

    L = c.gather().sink_to_list()

    a.emit(1)
    b.emit('a')
    a.emit(2)
    b.emit('b')

    assert L == [(1, 'a'), (2, 'b')]


def test_starmap():
    def add(x, y, z=0):
        return x + y + z

    source = Stream(asynchronous=True)
    L = source.thread_scatter().starmap(add, z=10).gather().sink_to_list()

    for i in range(5):
        source.emit((i, i))

    assert L == [10, 12, 14, 16, 18]
