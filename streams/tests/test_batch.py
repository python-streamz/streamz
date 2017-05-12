from operator import add

import toolz

from streams import Stream, Batch


def inc(x):
    return x + 1


def test_map():
    source = Stream()
    L = source.partition(3).map(Batch).map(inc).sink_to_list()

    for i in range(10):
        source.emit(i)

    assert L == [(1, 2, 3), (4, 5, 6), (7, 8, 9)]


def test_accumulate():
    source = Stream()
    L1 = source.partition(3).map(Batch).accumulate(add).sink_to_list()
    L2 = source.accumulate(add).sink_to_list()

    for i in range(9):
        source.emit(i)

    assert all(isinstance(x, tuple) for x in L1)

    assert list(toolz.concat(L1))== L2
