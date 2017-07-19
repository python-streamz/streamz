from streams.dask import scatter
from streams import Stream

from distributed import Future
from distributed.utils_test import gen_cluster, inc, dec


@gen_cluster(client=True)
def test_core(c, s, a, b):
    source = Stream()
    futures = scatter(source).map(inc)
    futures_L = futures.sink_to_list()
    L = futures.gather().sink_to_list()

    for i in range(5):
        yield source.emit(i)

    assert L == [1, 2, 3, 4, 5]
    assert all(isinstance(f, Future) for f in futures_L)


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
