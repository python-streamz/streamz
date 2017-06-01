from operator import add

from distributed.client import Future
from distributed.utils_test import inc, gen_cluster
from tornado import gen

from streams import Stream, Batch
import streams.dask as ds


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


@gen_cluster(client=True)
def test_batches(c, s, a, b):
    source = Stream()
    remote_batches = ds.scatter(source.partition(2).map(Batch))
    local_batches = ds.gather(remote_batches.map(inc))
    L = local_batches.flatten().sink_to_list()

    for i in range(10):
        source.emit(i)

    while len(L) < 3:
        yield gen.sleep(0.01)

    yield remote_batches.flush()
    yield local_batches.flush()

    assert L == list(map(inc, range(10)))


@gen_cluster(client=True)
def test_batch_future(c, s, a, b):
    source = Stream()
    L = source.map(inc).sink_to_list()
    [future] = yield c._scatter([Batch([1, 2, 3])])

    source.emit(future)

    assert isinstance(L[0], Future)
    result = yield c._gather(L[0])
    assert result == (2, 3, 4)


@gen_cluster(client=True)
def test_future_batch(c, s, a, b):
    source = Stream()
    L = source.map(inc).sink_to_list()
    futures = yield c._scatter([1, 2, 3])
    batch = Batch(futures)

    source.emit(batch)

    assert isinstance(L[0], Batch)
    assert isinstance(L[0][0], Future)
    result = yield c._gather(list(L[0]))
    assert result == [2, 3, 4]
