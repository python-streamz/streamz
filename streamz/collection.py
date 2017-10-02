import operator

from streamz import Stream, core

_subtypes = []


class Streaming(object):
    _subtype = object

    def __init__(self, stream=None, example=None):
        assert example is not None
        self.example = example
        assert isinstance(self.example, self._subtype)
        self.stream = stream or Stream()

    def map_partitions(self, func, *args, **kwargs):
        example = kwargs.pop('example', None)
        if example is None:
            example = func(self.example, *args, **kwargs)
        stream = self.stream.map(func, *args, **kwargs)

        for typ, stream_type in _subtypes:
            if isinstance(example, typ):
                return stream_type(stream, example)
        return Streaming(stream, example)

    def accumulate_partitions(self, func, *args, **kwargs):
        start = kwargs.pop('start', core.no_default)
        returns_state = kwargs.pop('returns_state', False)
        example = kwargs.pop('example', None)
        if example is None:
            example = func(start, self.example, *args, **kwargs)
        if returns_state:
            _, example = example
        stream = self.stream.accumulate(func, *args, start=start,
                returns_state=returns_state, **kwargs)

        for typ, stream_type in _subtypes:
            if isinstance(example, typ):
                return stream_type(stream, example)
        return Streaming(stream, example)

    def __repr__(self):
        example = self.example
        if hasattr(example, 'head'):
            example = example.head(2)
        return "%s - elements like:\n%r" % (type(self).__name__, example)

    def _repr_html_(self):
        example = self.example
        if hasattr(example, 'head'):
            example = example.head(2)
        try:
            body = example._repr_html_()
        except AttributeError:
            body = repr(example)

        return "<h5>%s - elements like<h5>\n%s" % (type(self).__name__, body)

    def __add__(self, other):
        return self.map_partitions(operator.add, other)

    def __mul__(self, other):
        return self.map_partitions(operator.mul, other)

    def __mod__(self, other):
        return self.map_partitions(operator.mod, other)

    def __truediv__(self, other):
        return self.map_partitions(operator.truediv, other)

    def __floordiv__(self, other):
        return self.map_partitions(operator.floordiv, other)

    def emit(self, x):
        self.verify(x)
        self.stream.emit(x)

    def verify(self, x):
        if not isinstance(x, self._subtype):
            raise TypeError("Expected type %s, got type %s" %
                            (self._subtype, type(x)))
