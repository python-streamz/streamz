from tornado import gen

from streamz import Stream

_global_sinks = set()


class Sink(Stream):

    _graphviz_shape = 'trapezium'

    def __init__(self, upstream, **kwargs):
        super().__init__(upstream, **kwargs)
        _global_sinks.add(self)


@Stream.register_api()
class sink(Sink):
    """ Apply a function on every element

    Examples
    --------
    >>> source = Stream()
    >>> L = list()
    >>> source.sink(L.append)
    >>> source.sink(print)
    >>> source.sink(print)
    >>> source.emit(123)
    123
    123
    >>> L
    [123]

    See Also
    --------
    map
    Stream.sink_to_list
    """

    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        # take the stream specific kwargs out
        stream_name = kwargs.pop("stream_name", None)
        self.kwargs = kwargs
        self.args = args
        super().__init__(upstream, stream_name=stream_name)

    def update(self, x, who=None, metadata=None):
        result = self.func(x, *self.args, **self.kwargs)
        if gen.isawaitable(result):
            return result
        else:
            return []


@Stream.register_api()
class sink_to_textfile(Sink):
    """ Write elements to a plain text file, one element per line. Type of elements
        must be ``str``.

        Arguments
        ---------
        file: str or file-like
            File to write the elements to. ``str`` is treated as a file name to open.
            If file-like, descriptor must be open in text mode.
            Note that the file descriptor will be closed when sink is destroyed.
        end: str, optional
            This value will be written to the file after each element.
            Defaults to newline character.
        mode: str, optional
            If file is ``str``, file will be opened in this mode. Defaults to ``"a"``
            (append mode).

        Examples
        --------
        >>> source = Stream()
        >>> source.map(str).sink_to_file("test.txt")
        >>> source.emit(0)
        >>> source.emit(1)
        >>> print(open("test.txt", "r").read())
        0
        1
    """
    def __init__(self, upstream, file, end="\n", mode="a", **kwargs):
        self._fp = open(file, mode=mode, buffering=1) if isinstance(file, str) else file
        self._end = end
        super().__init__(upstream, ensure_io_loop=True, **kwargs)

    def __del__(self):
        self._fp.close()

    @gen.coroutine
    def update(self, x, who=None, metadata=None):
        yield self.loop.run_in_executor(None, self._fp.write, x)
        yield self.loop.run_in_executor(None, self._fp.write, self._end)
