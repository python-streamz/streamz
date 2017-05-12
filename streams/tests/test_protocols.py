from operator import add

from streams import Stream
from streams.core import no_default


def inc(x):
    return x + 1


class Foo(object):
    def __init__(self, data):
        self.data = data

    def __stream_map__(self, func):
        return Foo(func(self.data))

    def __stream_reduce__(self, func, accumulator):
        if accumulator is no_default:
            return self, no_default
        else:
            result = Foo(func(accumulator.data, self.data))
            return result, result

    def __stream_merge__(self, *others):
        return Foo((self.data,) + tuple(o.data for o in others))


def test_map():
    source = Stream()
    L = source.map(inc).sink_to_list()

    source.emit(Foo(1))

    assert isinstance(L[0], Foo)
    assert L[0].data == 2


def test_map_raw():
    source = Stream()
    L = source.map(lambda x: (x, x), raw=True).sink_to_list()

    f = Foo(1)
    source.emit(f)

    assert isinstance(L[0], tuple)
    assert len(L[0]) == 2
    assert L[0][0] is f
    assert L[0][1] is f


def test_scan():
    source = Stream()
    L = source.scan(add).sink_to_list()

    source.emit(Foo(1))
    source.emit(Foo(2))
    source.emit(Foo(3))

    assert isinstance(L[1], Foo)
    assert L[1].data == 6


def test_zip():
    a = Stream()
    b = Stream()

    L = a.zip(b).sink_to_list()

    a.emit(Foo(1))
    b.emit(Foo(2))

    assert L[0].data == (1, 2)
