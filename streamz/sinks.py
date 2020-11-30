import inspect
import weakref

from tornado import gen

from streamz import Stream

# sinks add themselves here to avoid being garbage-collected
_global_sinks = set()


class Sink(Stream):

    _graphviz_shape = 'trapezium'

    def __init__(self, upstream, **kwargs):
        super().__init__(upstream, **kwargs)
        _global_sinks.add(self)

    def destroy(self):
        super().destroy()
        _global_sinks.remove(self)


@Stream.register_api()
class sink(Sink):
    """ Apply a function on every element

    Parameters
    ----------
    func: callable
        A function that will be applied on every element.
    args:
        Positional arguments that will be passed to ``func`` after the incoming element.
    kwargs:
        Stream-specific arguments will be passed to ``Stream.__init__``, the rest of
        them will be passed to ``func``.

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
        sig = set(inspect.signature(Stream).parameters)
        stream_kwargs = {k: v for (k, v) in kwargs.items() if k in sig}
        self.kwargs = {k: v for (k, v) in kwargs.items() if k not in sig}
        self.args = args
        super().__init__(upstream, **stream_kwargs)

    def update(self, x, who=None, metadata=None):
        result = self.func(x, *self.args, **self.kwargs)
        if gen.isawaitable(result):
            return result
        else:
            return []


@Stream.register_api()
class sink_to_textfile(Sink):
    """ Write elements to a plain text file, one element per line.

        Type of elements must be ``str``.

        Parameters
        ----------
        file: str or file-like
            File to write the elements to. ``str`` is treated as a file name to open.
            If file-like, descriptor must be open in text mode. Note that the file
            descriptor will be closed when this sink is destroyed.
        end: str, optional
            This value will be written to the file after each element.
            Defaults to newline character.
        mode: str, optional
            If file is ``str``, file will be opened in this mode. Defaults to ``"a"``
            (append mode).

        Examples
        --------
        >>> source = Stream()
        >>> source.map(str).sink_to_textfile("test.txt")
        >>> source.emit(0)
        >>> source.emit(1)
        >>> print(open("test.txt", "r").read())
        0
        1
    """
    def __init__(self, upstream, file, end="\n", mode="a", **kwargs):
        self._end = end
        self._fp = open(file, mode=mode) if isinstance(file, str) else file
        weakref.finalize(self, self._fp.close)
        super().__init__(upstream, **kwargs)

    def __del__(self):
        self._fp.close()

    def update(self, x, who=None, metadata=None):
        self._fp.write(x + self._end)
