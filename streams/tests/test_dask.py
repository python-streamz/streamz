from operator import add

from streams.dask import scatter
from streams import Stream

from distributed import Future
from distributed.utils_test import gen_cluster, inc


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

    assert L == [3, 6, 10, 15]
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

    assert L == [1, 3]


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
