import operator
import types

from streamz import Stream, core

_stream_types = {'streaming': [], 'updating': []}


def map_partitions(func, *args, **kwargs):
    """ Map a function across all batch elements of this stream

    The output stream type will be determined by the action of that
    function on the example

    See Also
    --------
    Streaming.accumulate_partitions
    """
    example = kwargs.pop('example', None)
    if example is None:
        example = func(*[getattr(arg, 'example', arg) for arg in args], **kwargs)

    streams = [arg for arg in args if isinstance(arg, Streaming)]
    if 'stream_type' in kwargs:
        stream_type = kwargs['stream_type']
    else:
        stream_type = ('streaming'
                       if any(s._stream_type == 'streaming' for s in streams)
                       else 'updating')

    if len(streams) > 1:
        stream = type(streams[0].stream).zip(*[getattr(arg, 'stream', arg) for arg in args])
        stream = stream.map(apply_args, func, kwargs)

    else:
        s = streams[0]

        if isinstance(args[0], Streaming):
            stream = s.stream.map(func, *args[1:], **kwargs)
        else:
            other = [(i, arg) for i, arg in enumerate(args)
                     if not isinstance(arg, Streaming)]
            stream = s.stream.map(partial_by_order, function=func, other=other,
                                  **kwargs)
    s_type = get_stream_type(example, stream_type)
    if s_type:
        return s_type(stream, example)
    return Streaming(stream, example, stream_type=stream_type)


class OperatorMixin(object):
    def __abs__(self):
        return self.map_partitions(operator.abs, self)

    def __add__(self, other):
        return self.map_partitions(operator.add, self, other)

    def __radd__(self, other):
        return self.map_partitions(operator.add, other, self)

    def __and__(self, other):
        return self.map_partitions(operator.and_, self, other)

    def __rand__(self, other):
        return self.map_partitions(operator.and_, other, self)

    def __eq__(self, other):
        return self.map_partitions(operator.eq, self, other)

    def __floordiv__(self, other):
        return self.map_partitions(operator.floordiv, self, other)

    def __rfloordiv__(self, other):
        return self.map_partitions(operator.floordiv, other, self)

    def __ge__(self, other):
        return self.map_partitions(operator.ge, self, other)

    def __gt__(self, other):
        return self.map_partitions(operator.gt, self, other)

    def __inv__(self):
        return self.map_partitions(operator.inv, self)

    def __invert__(self):
        return self.map_partitions(operator.invert, self)

    def __le__(self, other):
        return self.map_partitions(operator.le, self, other)

    def __lshift__(self, other):
        return self.map_partitions(operator.lshift, self, other)

    def __rlshift__(self, other):
        return self.map_partitions(operator.lshift, other, self)

    def __lt__(self, other):
        return self.map_partitions(operator.lt, self, other)

    def __mod__(self, other):
        return self.map_partitions(operator.mod, self, other)

    def __rmod__(self, other):
        return self.map_partitions(operator.mod, other, self)

    def __mul__(self, other):
        return self.map_partitions(operator.mul, self, other)

    def __rmul__(self, other):
        return self.map_partitions(operator.mul, other, self)

    def __ne__(self, other):
        return self.map_partitions(operator.ne, self, other)

    def __neg__(self):
        return self.map_partitions(operator.neg, self)

    def __or__(self, other):
        return self.map_partitions(operator.or_, self, other)

    def __ror__(self, other):
        return self.map_partitions(operator.or_, other, self)

    def __pow__(self, other):
        return self.map_partitions(operator.pow, self, other)

    def __rpow__(self, other):
        return self.map_partitions(operator.pow, other, self)

    def __rshift__(self, other):
        return self.map_partitions(operator.rshift, self, other)

    def __rrshift__(self, other):
        return self.map_partitions(operator.rshift, other, self)

    def __sub__(self, other):
        return self.map_partitions(operator.sub, self, other)

    def __rsub__(self, other):
        return self.map_partitions(operator.sub, other, self)

    def __truediv__(self, other):
        return self.map_partitions(operator.truediv, self, other)

    def __rtruediv__(self, other):
        return self.map_partitions(operator.truediv, other, self)

    def __xor__(self, other):
        return self.map_partitions(operator.xor, self, other)

    def __rxor__(self, other):
        return self.map_partitions(operator.xor, other, self)


class Streaming(OperatorMixin, core.APIRegisterMixin):
    """
    Superclass for streaming collections

    Do not create this class directly, use one of the subclasses instead.

    Parameters
    ----------
    stream: streamz.Stream
    example: object
        An object to represent an example element of this stream

    See also
    --------
    streamz.dataframe.StreamingDataFrame
    streamz.dataframe.StreamingBatch
    """
    _subtype = object
    _stream_type = 'streaming'
    map_partitions = staticmethod(map_partitions)

    def __init__(self, stream=None, example=None, stream_type=None):
        assert example is not None
        self.example = example
        if not isinstance(self.example, self._subtype):
            self.example = self._subtype(example)
        assert isinstance(self.example, self._subtype)
        self.stream = stream or Stream()
        if stream_type:
            if stream_type not in ['streaming', 'updating']:
                raise Exception()
            self._stream_type = stream_type

    def accumulate_partitions(self, func, *args, **kwargs):
        """ Accumulate a function with state across batch elements

        See Also
        --------
        Streaming.map_partitions
        """
        start = kwargs.pop('start', core.no_default)
        returns_state = kwargs.pop('returns_state', False)
        example = kwargs.pop('example', None)
        stream_type = kwargs.pop('stream_type', self._stream_type)
        if example is None:
            example = func(start, self.example, *args, **kwargs)
        if returns_state:
            _, example = example
        stream = self.stream.accumulate(func, *args, start=start,
                                        returns_state=returns_state, **kwargs)

        s_type = get_stream_type(example, stream_type)
        if s_type:
            return s_type(stream, example)
        return Streaming(stream, example, stream_type=stream_type)

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

    @property
    def current_value(self):
        return self.stream.current_value

    def start(self):
        self.stream.start()

    def stop(self):
        self.stream.stop()

    def _ipython_display_(self, **kwargs):
        try:
            from ipywidgets import Output  # noqa: F401
            return self.stream.latest().rate_limit(
                0.5).gather()._ipython_display_(**kwargs)
        except ImportError:
            # since this function is only called by jupyter, this import must succeed
            from IPython.display import display, HTML
            if hasattr(self, '_repr_html_'):
                return display(HTML(self._repr_html_()))
            else:
                return display(self.__repr__())

    def emit(self, x):
        self.verify(x)
        self.stream.emit(x)

    def verify(self, x):
        """ Verify elements that pass through this stream """
        if not isinstance(x, self._subtype):
            raise TypeError("Expected type %s, got type %s" %
                            (self._subtype, type(x)))


def get_stream_type(example, stream_type='streaming'):
    for typ, s_type in _stream_types[stream_type]:
        if isinstance(typ, types.FunctionType):
            """For Frame like objects we use utility functions to check type.
               i.e, DataFrame like objects are checked using is_dataframe_like."""
            if typ(example):
                return s_type
        elif isinstance(example, typ):
            return s_type
    return None


def partial_by_order(*args, **kwargs):
    """

    >>> from operator import add
    >>> partial_by_order(5, function=add, other=[(1, 10)])
    15
    """
    function = kwargs.pop('function')
    other = kwargs.pop('other')
    args2 = list(args)
    for i, arg in other:
        args2.insert(i, arg)
    return function(*args2, **kwargs)


def apply_args(args, func, kwargs):
    return func(*args, **kwargs)
