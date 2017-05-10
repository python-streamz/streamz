from streams import Stream, Batch
from operator import add


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
    L = source.partition(3).map(Batch).accumulate(add).sink_to_list()

    for i in range(10):
        source.emit(i)

    assert L == [(1, 2), (3, 4, 6), (7, 8, 9)]
