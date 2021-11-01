import asyncio
from collections import deque, defaultdict
from datetime import timedelta
import functools
import logging
import six
import sys
import threading
from time import time
from typing import Any, Callable, Hashable, Union
import weakref

import toolz
from tornado import gen
from tornado.locks import Condition
from tornado.ioloop import IOLoop
from tornado.queues import Queue

try:
    from distributed.client import default_client as _dask_default_client
except ImportError:  # pragma: no cover
    _dask_default_client = None

from collections.abc import Iterable

from threading import get_ident as get_thread_identity
from .orderedweakset import OrderedWeakrefSet

no_default = '--no-default--'

_html_update_streams = set()

thread_state = threading.local()

logger = logging.getLogger(__name__)


_io_loops = []


def get_io_loop(asynchronous=None):
    if asynchronous:
        return IOLoop.current()

    if _dask_default_client is not None:
        try:
            client = _dask_default_client()
        except ValueError:
            # No dask client found; continue
            pass
        else:
            return client.loop

    if not _io_loops:
        loop = IOLoop()
        thread = threading.Thread(target=loop.start)
        thread.daemon = True
        thread.start()
        _io_loops.append(loop)

    return _io_loops[-1]


def identity(x):
    return x


class RefCounter:
    """ A counter to track references to data

        This class is used to track how many nodes in the DAG are referencing
        a particular element in the pipeline. When the count reaches zero,
        then parties interested in knowing if data is done being processed are
        notified

        Parameters
        ----------
        initial: int, optional
            The initial value of the reference counter
        cb: callable
            The function to use a callback when the reference count reaches zero
        loop: tornado.ioloop.IOLoop
            The loop on which to create a callback when the reference count
            reaches zero
    """
    def __init__(self, initial=0, cb=None, loop=None):
        self.loop = loop if loop else get_io_loop()
        self.count = initial
        self.cb = cb

    def retain(self, n=1):
        """Retain the reference

        Parameters
        ----------
        n: The number of times to retain the reference
        """
        self.count += n

    def release(self, n=1):
        """Release the reference

        If the reference count is equal to or less than zero, the callback, if
        provided will added to the provided loop or default loop

        Parameters
        ----------
        n: The number of references to release
        """
        self.count -= n
        if self.count <= 0 and self.cb:
            self.loop.add_callback(self.cb)

    def __str__(self):
        return '<RefCounter count={}>'.format(self.count)

    __repr__ = __str__


class APIRegisterMixin(object):

    @classmethod
    def register_api(cls, modifier=identity, attribute_name=None):
        """ Add callable to Stream API

        This allows you to register a new method onto this class.  You can use
        it as a decorator.::

            >>> @Stream.register_api()
            ... class foo(Stream):
            ...     ...

            >>> Stream().foo(...)  # this works now

        It attaches the callable as a normal attribute to the class object.  In
        doing so it respects inheritance (all subclasses of Stream will also
        get the foo attribute).

        By default callables are assumed to be instance methods.  If you like
        you can include modifiers to apply before attaching to the class as in
        the following case where we construct a ``staticmethod``.

            >>> @Stream.register_api(staticmethod)
            ... class foo(Stream):
            ...     ...

            >>> Stream.foo(...)  # Foo operates as a static method

        You can also provide an optional ``attribute_name`` argument to control
        the name of the attribute your callable will be attached as.

            >>> @Stream.register_api(attribute_name="bar")
            ... class foo(Stream):
            ...     ...

            >> Stream().bar(...)  # foo was actually attached as bar
        """
        def _(func):
            @functools.wraps(func)
            def wrapped(*args, **kwargs):
                return func(*args, **kwargs)
            name = attribute_name if attribute_name else func.__name__
            setattr(cls, name, modifier(wrapped))
            return func
        return _

    @classmethod
    def register_plugin_entry_point(cls, entry_point, modifier=identity):
        if hasattr(cls, entry_point.name):
            raise ValueError(
                f"Can't add {entry_point.name} from {entry_point.module_name} "
                f"to {cls.__name__}: duplicate method name."
            )

        def stub(*args, **kwargs):
            """ Entrypoints-based streamz plugin. Will be loaded on first call. """
            node = entry_point.load()
            if not issubclass(node, Stream):
                raise TypeError(
                    f"Error loading {entry_point.name} "
                    f"from module {entry_point.module_name}: "
                    f"{node.__class__.__name__} must be a subclass of Stream"
                )
            if getattr(cls, entry_point.name).__name__ == "stub":
                cls.register_api(
                    modifier=modifier, attribute_name=entry_point.name
                )(node)
            return node(*args, **kwargs)
        cls.register_api(modifier=modifier, attribute_name=entry_point.name)(stub)


class Stream(APIRegisterMixin):
    """ A Stream is an infinite sequence of data.

    Streams subscribe to each other passing and transforming data between them.
    A Stream object listens for updates from upstream, reacts to these updates,
    and then emits more data to flow downstream to all Stream objects that
    subscribe to it.  Downstream Stream objects may connect at any point of a
    Stream graph to get a full view of the data coming off of that point to do
    with as they will.

    Parameters
    ----------
    stream_name: str or None
        This is the name of the stream.
    asynchronous: boolean or None
        Whether or not this stream will be used in asynchronous functions or
        normal Python functions.  Leave as None if you don't know.
        True will cause operations like emit to return awaitable Futures
        False will use an Event loop in another thread (starts it if necessary)
    ensure_io_loop: boolean
        Ensure that some IOLoop will be created.  If asynchronous is None or
        False then this will be in a separate thread, otherwise it will be
        IOLoop.current

    Examples
    --------
    >>> def inc(x):
    ...     return x + 1

    >>> source = Stream()  # Create a stream object
    >>> s = source.map(inc).map(str)  # Subscribe to make new streams
    >>> s.sink(print)  # take an action whenever an element reaches the end

    >>> L = list()
    >>> s.sink(L.append)  # or take multiple actions (streams can branch)

    >>> for i in range(5):
    ...     source.emit(i)  # push data in at the source
    '1'
    '2'
    '3'
    '4'
    '5'
    >>> L  # and the actions happen at the sinks
    ['1', '2', '3', '4', '5']
    """
    _graphviz_shape = 'ellipse'
    _graphviz_style = 'rounded,filled'
    _graphviz_fillcolor = 'white'
    _graphviz_orientation = 0

    str_list = ['func', 'predicate', 'n', 'interval']

    def __init__(self, upstream=None, upstreams=None, stream_name=None,
                 loop=None, asynchronous=None, ensure_io_loop=False):
        self.name = stream_name
        self.downstreams = OrderedWeakrefSet()
        self.current_value = None
        self.current_metadata = None
        if upstreams is not None:
            self.upstreams = list(upstreams)
        elif upstream is not None:
            self.upstreams = [upstream]
        else:
            self.upstreams = []

        self._set_asynchronous(asynchronous)
        self._set_loop(loop)
        if ensure_io_loop and not self.loop:
            self._set_asynchronous(False)
        if self.loop is None and self.asynchronous is not None:
            self._set_loop(get_io_loop(self.asynchronous))

        for upstream in self.upstreams:
            if upstream:
                upstream.downstreams.add(self)

    def _set_loop(self, loop):
        self.loop = None
        if loop is not None:
            self._inform_loop(loop)
        else:
            for upstream in self.upstreams:
                if upstream and upstream.loop:
                    self.loop = upstream.loop
                    break

    def _inform_loop(self, loop):
        """
        Percolate information about an event loop to the rest of the stream
        """
        if self.loop is not None:
            if self.loop is not loop:
                raise ValueError("Two different event loops active")
        else:
            self.loop = loop
            for upstream in self.upstreams:
                if upstream:
                    upstream._inform_loop(loop)
            for downstream in self.downstreams:
                if downstream:
                    downstream._inform_loop(loop)

    def _set_asynchronous(self, asynchronous):
        self.asynchronous = None
        if asynchronous is not None:
            self._inform_asynchronous(asynchronous)
        else:
            for upstream in self.upstreams:
                if upstream and upstream.asynchronous:
                    self.asynchronous = upstream.asynchronous
                    break

    def _inform_asynchronous(self, asynchronous):
        """
        Percolate information about an event loop to the rest of the stream
        """
        if self.asynchronous is not None:
            if self.asynchronous is not asynchronous:
                raise ValueError("Stream has both asynchronous and synchronous elements")
        else:
            self.asynchronous = asynchronous
            for upstream in self.upstreams:
                if upstream:
                    upstream._inform_asynchronous(asynchronous)
            for downstream in self.downstreams:
                if downstream:
                    downstream._inform_asynchronous(asynchronous)

    def _add_upstream(self, upstream):
        """Add upstream to current upstreams, this method is overridden for
        classes which handle stream specific buffers/caches"""
        self.upstreams.append(upstream)

    def _add_downstream(self, downstream):
        """Add downstream to current downstreams"""
        self.downstreams.add(downstream)

    def _remove_downstream(self, downstream):
        """Remove downstream from current downstreams"""
        self.downstreams.remove(downstream)

    def _remove_upstream(self, upstream):
        """Remove upstream from current upstreams, this method is overridden for
        classes which handle stream specific buffers/caches"""
        self.upstreams.remove(upstream)

    def start(self):
        """ Start any upstream sources """
        for upstream in self.upstreams:
            upstream.start()

    def stop(self):
        """ Stop upstream sources """
        for upstream in self.upstreams:
            upstream.stop()

    def __str__(self):
        s_list = []
        if self.name:
            s_list.append('{}; {}'.format(self.name, self.__class__.__name__))
        else:
            s_list.append(self.__class__.__name__)

        for m in self.str_list:
            s = ''
            at = getattr(self, m, None)
            if at:
                if not callable(at):
                    s = str(at)
                elif hasattr(at, '__name__'):
                    s = getattr(self, m).__name__
                else:
                    s = None
            if s:
                s_list.append('{}={}'.format(m, s))
        if len(s_list) <= 2:
            s_list = [term.split('=')[-1] for term in s_list]

        text = "<"
        text += s_list[0]
        if len(s_list) > 1:
            text += ': '
            text += ', '.join(s_list[1:])
        text += '>'
        return text

    __repr__ = __str__

    def _ipython_display_(self, **kwargs):  # pragma: no cover
        try:
            import ipywidgets
            from IPython.core.interactiveshell import InteractiveShell
            output = ipywidgets.Output(_view_count=0)
        except ImportError:
            # since this function is only called by jupyter, this import must succeed
            from IPython.display import display, HTML
            if hasattr(self, '_repr_html_'):
                return display(HTML(self._repr_html_()))
            else:
                return display(self.__repr__())
        output_ref = weakref.ref(output)

        def update_cell(val):
            output = output_ref()
            if output is None:
                return
            with output:
                content, *_ = InteractiveShell.instance().display_formatter.format(val)
                output.outputs = ({'output_type': 'display_data',
                      'data': content,
                      'metadata': {}},)

        s = self.map(update_cell)
        _html_update_streams.add(s)

        self.output_ref = output_ref
        s_ref = weakref.ref(s)

        def remove_stream(change):
            output = output_ref()
            if output is None:
                return

            if output._view_count == 0:
                ss = s_ref()
                ss.destroy()
                _html_update_streams.remove(ss)  # trigger gc

        output.observe(remove_stream, '_view_count')

        return output._ipython_display_(**kwargs)

    def _emit(self, x, metadata=None):
        """
        Push data into the stream at this point

        Parameters
        ----------
        x: any
            an element of data
        metadata: list[dict], optional
            Various types of metadata associated with the data element in `x`.

            ref: RefCounter
            A reference counter used to check when data is done

        """
        self.current_value = x
        self.current_metadata = metadata
        if metadata:
            self._retain_refs(metadata, len(self.downstreams))
        else:
            metadata = []

        result = []
        for downstream in list(self.downstreams):
            r = downstream.update(x, who=self, metadata=metadata)

            if type(r) is list:
                result.extend(r)
            else:
                result.append(r)

            self._release_refs(metadata)

        return [element for element in result if element is not None]

    def emit(self, x, asynchronous=False, metadata=None):
        """ Push data into the stream at this point

        This is typically done only at source Streams but can theoretically be
        done at any point

        Parameters
        ----------
        x: any
            an element of data
        asynchronous:
            emit asynchronously
        metadata: list[dict], optional
            Various types of metadata associated with the data element in `x`.

            ref: RefCounter
            A reference counter used to check when data is done
        """
        ts_async = getattr(thread_state, 'asynchronous', False)
        if self.loop is None or asynchronous or self.asynchronous or ts_async:
            if not ts_async:
                thread_state.asynchronous = True
            try:
                result = self._emit(x, metadata=metadata)
                if self.loop:
                    return gen.convert_yielded(result)
            finally:
                thread_state.asynchronous = ts_async
        else:
            async def _():
                thread_state.asynchronous = True
                try:
                    result = await asyncio.gather(*self._emit(x, metadata=metadata))
                finally:
                    del thread_state.asynchronous
                return result

            sync(self.loop, _)

    def update(self, x, who=None, metadata=None):
        return self._emit(x, metadata=metadata)

    def gather(self):
        """ This is a no-op for core streamz

        This allows gather to be used in both dask and core streams
        """
        return self

    def connect(self, downstream):
        """ Connect this stream to a downstream element.

        Parameters
        ----------
        downstream: Stream
            The downstream stream to connect to
        """
        self._add_downstream(downstream)
        downstream._add_upstream(self)

    def disconnect(self, downstream):
        """ Disconnect this stream to a downstream element.

        Parameters
        ----------
        downstream: Stream
            The downstream stream to disconnect from
        """
        self._remove_downstream(downstream)

        downstream._remove_upstream(self)

    @property
    def upstream(self):
        if len(self.upstreams) > 1:
            raise ValueError("Stream has multiple upstreams")
        elif len(self.upstreams) == 0:
            return None
        else:
            return self.upstreams[0]

    def destroy(self, streams=None):
        """
        Disconnect this stream from any upstream sources
        """
        if streams is None:
            streams = self.upstreams
        for upstream in list(streams):
            upstream._remove_downstream(self)
            self._remove_upstream(upstream)

    def scatter(self, **kwargs):
        from .dask import scatter
        return scatter(self, **kwargs)

    def remove(self, predicate):
        """ Only pass through elements for which the predicate returns False """
        return self.filter(lambda x: not predicate(x))

    @property
    def scan(self):
        return self.accumulate

    @property
    def concat(self):
        return self.flatten

    def sink_to_list(self):
        """ Append all elements of a stream to a list as they come in

        Examples
        --------
        >>> source = Stream()
        >>> L = source.map(lambda x: 10 * x).sink_to_list()
        >>> for i in range(5):
        ...     source.emit(i)
        >>> L
        [0, 10, 20, 30, 40]
        """
        L = []
        self.sink(L.append)
        return L

    def frequencies(self, **kwargs):
        """ Count occurrences of elements """
        def update_frequencies(last, x):
            return toolz.assoc(last, x, last.get(x, 0) + 1)

        return self.scan(update_frequencies, start={}, **kwargs)

    def visualize(self, filename='mystream.png', **kwargs):
        """Render the computation of this object's task graph using graphviz.

        Requires ``graphviz`` and ``networkx`` to be installed.

        Parameters
        ----------
        filename : str, optional
            The name of the file to write to disk.
        kwargs:
            Graph attributes to pass to graphviz like ``rankdir="LR"``
        """
        from .graph import visualize
        return visualize(self, filename, **kwargs)

    def to_dataframe(self, example):
        """ Convert a stream of Pandas dataframes to a DataFrame

        Examples
        --------
        >>> source = Stream()
        >>> sdf = source.to_dataframe()
        >>> L = sdf.groupby(sdf.x).y.mean().stream.sink_to_list()
        >>> source.emit(pd.DataFrame(...))  # doctest: +SKIP
        >>> source.emit(pd.DataFrame(...))  # doctest: +SKIP
        >>> source.emit(pd.DataFrame(...))  # doctest: +SKIP
        """
        from .dataframe import DataFrame
        return DataFrame(stream=self, example=example)

    def to_batch(self, **kwargs):
        """ Convert a stream of lists to a Batch

        All elements of the stream are assumed to be lists or tuples

        Examples
        --------
        >>> source = Stream()
        >>> batches = source.to_batch()
        >>> L = batches.pluck('value').map(inc).sum().stream.sink_to_list()
        >>> source.emit([{'name': 'Alice', 'value': 1},
        ...              {'name': 'Bob', 'value': 2},
        ...              {'name': 'Charlie', 'value': 3}])
        >>> source.emit([{'name': 'Alice', 'value': 4},
        ...              {'name': 'Bob', 'value': 5},
        ...              {'name': 'Charlie', 'value': 6}])
        """
        from .batch import Batch
        return Batch(stream=self, **kwargs)

    def _retain_refs(self, metadata, n=1):
        """ Retain all references in the provided metadata `n` number of times

        Parameters
        ----------
        metadata: list[dict], optional
            Various types of metadata associated with the data element in `x`.

            ref: RefCounter
            A reference counter used to check when data is done
        n: The number of times to retain the provided references

        """
        for m in metadata:
            if 'ref' in m:
                m['ref'].retain(n)

    def _release_refs(self, metadata, n=1):
        """ Release all references in the provided metadata `n` number of times

        Parameters
        ----------
        metadata: list[dict], optional
            Various types of metadata associated with the data element in `x`.

            ref: RefCounter
            A reference counter used to check when data is done
        n: The number of times to retain the provided references

        """
        for m in metadata:
            if 'ref' in m:
                m['ref'].release(n)


@Stream.register_api()
class map(Stream):
    """ Apply a function to every element in the stream

    Parameters
    ----------
    func: callable
    *args :
        The arguments to pass to the function.
    **kwargs:
        Keyword arguments to pass to func

    Examples
    --------
    >>> source = Stream()
    >>> source.map(lambda x: 2*x).sink(print)
    >>> for i in range(5):
    ...     source.emit(i)
    0
    2
    4
    6
    8
    """
    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        # this is one of a few stream specific kwargs
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None, metadata=None):
        try:
            result = self.func(x, *self.args, **self.kwargs)
        except Exception as e:
            logger.exception(e)
            raise
        else:
            return self._emit(result, metadata=metadata)


@Stream.register_api()
class starmap(Stream):
    """ Apply a function to every element in the stream, splayed out

    See ``itertools.starmap``

    Parameters
    ----------
    func: callable
    *args :
        The arguments to pass to the function.
    **kwargs:
        Keyword arguments to pass to func

    Examples
    --------
    >>> source = Stream()
    >>> source.starmap(lambda a, b: a + b).sink(print)
    >>> for i in range(5):
    ...     source.emit((i, i))
    0
    2
    4
    6
    8
    """
    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        # this is one of a few stream specific kwargs
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None, metadata=None):
        y = x + self.args
        try:
            result = self.func(*y, **self.kwargs)
        except Exception as e:
            logger.exception(e)
            raise
        else:
            return self._emit(result, metadata=metadata)


def _truthy(x):
    return not not x


@Stream.register_api()
class filter(Stream):
    """ Only pass through elements that satisfy the predicate

    Parameters
    ----------
    predicate : function
        The predicate. Should return True or False, where
        True means that the predicate is satisfied.
    *args :
        The arguments to pass to the predicate.
    **kwargs:
        Keyword arguments to pass to predicate

    Examples
    --------
    >>> source = Stream()
    >>> source.filter(lambda x: x % 2 == 0).sink(print)
    >>> for i in range(5):
    ...     source.emit(i)
    0
    2
    4
    """

    def __init__(self, upstream, predicate, *args, **kwargs):
        if predicate is None:
            predicate = _truthy
        self.predicate = predicate
        stream_name = kwargs.pop("stream_name", None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None, metadata=None):
        if self.predicate(x, *self.args, **self.kwargs):
            return self._emit(x, metadata=metadata)


@Stream.register_api()
class accumulate(Stream):
    """ Accumulate results with previous state

    This performs running or cumulative reductions, applying the function
    to the previous total and the new element.  The function should take
    two arguments, the previous accumulated state and the next element and
    it should return a new accumulated state,
    - ``state = func(previous_state, new_value)`` (returns_state=False)
    - ``state, result = func(previous_state, new_value)`` (returns_state=True)

    where the new_state is passed to the next invocation. The state or result
    is emitted downstream for the two cases.

    Parameters
    ----------
    func: callable
    start: object
        Initial value, passed as the value of ``previous_state`` on the first
        invocation. Defaults to the first submitted element
    returns_state: boolean
        If true then func should return both the state and the value to emit
        If false then both values are the same, and func returns one value
    **kwargs:
        Keyword arguments to pass to func

    Examples
    --------
    A running total, producing triangular numbers

    >>> source = Stream()
    >>> source.accumulate(lambda acc, x: acc + x).sink(print)
    >>> for i in range(5):
    ...     source.emit(i)
    0
    1
    3
    6
    10

    A count of number of events (including the current one)

    >>> source = Stream()
    >>> source.accumulate(lambda acc, x: acc + 1, start=0).sink(print)
    >>> for _ in range(5):
    ...     source.emit(0)
    1
    2
    3
    4
    5

    Like the builtin "enumerate".

    >>> source = Stream()
    >>> source.accumulate(lambda acc, x: ((acc[0] + 1, x), (acc[0], x)),
    ...                   start=(0, 0), returns_state=True
    ...                   ).sink(print)
    >>> for i in range(3):
    ...     source.emit(0)
    (0, 0)
    (1, 0)
    (2, 0)
    """
    _graphviz_shape = 'box'

    def __init__(self, upstream, func, start=no_default, returns_state=False,
                 **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.state = start
        self.returns_state = returns_state
        # this is one of a few stream specific kwargs
        stream_name = kwargs.pop('stream_name', None)
        self.with_state = kwargs.pop('with_state', False)
        Stream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None, metadata=None):
        if self.state is no_default:
            self.state = x
            if self.with_state:
                return self._emit((self.state, x), metadata=metadata)
            else:
                return self._emit(x, metadata=metadata)
        else:
            try:
                result = self.func(self.state, x, **self.kwargs)
            except Exception as e:
                logger.exception(e)
                raise
            if self.returns_state:
                state, result = result
            else:
                state = result
            self.state = state
            if self.with_state:
                return self._emit((self.state, result), metadata=metadata)
            else:
                return self._emit(result, metadata=metadata)


@Stream.register_api()
class slice(Stream):
    """
    Get only some events in a stream by position. Works like list[] syntax.

    Parameters
    ----------
    start : int
        First event to use. If None, start from the beginnning
    end : int
        Last event to use (non-inclusive). If None, continue without stopping.
        Does not support negative indexing.
    step : int
        Pass on every Nth event. If None, pass every one.

    Examples
    --------
    >>> source = Stream()
    >>> source.slice(2, 6, 2).sink(print)
    >>> for i in range(5):
    ...     source.emit(0)
    2
    4
    """

    def __init__(self, upstream, start=None, end=None, step=None, **kwargs):
        self.state = 0
        self.star = start or 0
        self.end = end
        self.step = step or 1
        if any((_ or 0) < 0 for _ in [start, end, step]):
            raise ValueError("Negative indices not supported by slice")
        stream_name = kwargs.pop('stream_name', None)
        Stream.__init__(self, upstream, stream_name=stream_name)
        self._check_end()

    def update(self, x, who=None, metadata=None):
        if self.state >= self.star and self.state % self.step == 0:
            self.emit(x, metadata=metadata)
        self.state += 1
        self._check_end()

    def _check_end(self):
        if self.end and self.state >= self.end:
            # we're done
            for upstream in self.upstreams:
                upstream._remove_downstream(self)


@Stream.register_api()
class partition(Stream):
    """ Partition stream into tuples of equal size

    Parameters
    ----------
    n: int
        Maximum partition size
    timeout: int or float, optional
        Number of seconds after which a partition will be emitted,
        even if its size is less than ``n``. If ``None`` (default),
        a partition will be emitted only when its size reaches ``n``.
    key: hashable or callable, optional
        Emit items with the same key together as a separate partition.
        If ``key`` is callable, partition will be identified by ``key(x)``,
        otherwise by ``x[key]``. Defaults to ``None``.

    Examples
    --------
    >>> source = Stream()
    >>> source.partition(3).sink(print)
    >>> for i in range(10):
    ...     source.emit(i)
    (0, 1, 2)
    (3, 4, 5)
    (6, 7, 8)

    >>> source = Stream()
    >>> source.partition(2, key=lambda x: x % 2).sink(print)
    >>> for i in range(4):
    ...     source.emit(i)
    (0, 2)
    (1, 3)

    >>> from time import sleep
    >>> source = Stream()
    >>> source.partition(5, timeout=1).sink(print)
    >>> for i in range(3):
    ...     source.emit(i)
    >>> sleep(1)
    (0, 1, 2)
    """
    _graphviz_shape = 'diamond'

    def __init__(self, upstream, n, timeout=None, key=None, **kwargs):
        self.n = n
        self._timeout = timeout
        self._key = key
        self._buffer = defaultdict(lambda: [])
        self._metadata_buffer = defaultdict(lambda: [])
        self._callbacks = {}
        kwargs["ensure_io_loop"] = True
        Stream.__init__(self, upstream, **kwargs)

    def _get_key(self, x):
        if self._key is None:
            return None
        if callable(self._key):
            return self._key(x)
        return x[self._key]

    @gen.coroutine
    def _flush(self, key):
        result, self._buffer[key] = self._buffer[key], []
        metadata_result, self._metadata_buffer[key] = self._metadata_buffer[key], []
        yield self._emit(tuple(result), list(metadata_result))
        self._release_refs(metadata_result)

    @gen.coroutine
    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        key = self._get_key(x)
        buffer = self._buffer[key]
        metadata_buffer = self._metadata_buffer[key]
        buffer.append(x)
        if isinstance(metadata, list):
            metadata_buffer.extend(metadata)
        else:
            metadata_buffer.append(metadata)
        if len(buffer) == self.n:
            if self._timeout is not None and self.n > 1:
                self._callbacks[key].cancel()
            yield self._flush(key)
            return
        if len(buffer) == 1 and self._timeout is not None:
            self._callbacks[key] = self.loop.call_later(
                self._timeout, self._flush, key
            )


@Stream.register_api()
class partition_unique(Stream):
    """
    Partition stream elements into groups of equal size with unique keys only.

    Parameters
    ----------
    n: int
        Number of (unique) elements to pass through as a group.
    key: Union[Hashable, Callable[[Any], Hashable]]
        Callable that accepts a stream element and returns a unique, hashable
        representation of the incoming data (``key(x)``), or a hashable that gets
        the corresponding value of a stream element (``x[key]``). For example,
        ``key=lambda x: x["a"]`` would allow only elements with unique ``"a"`` values
        to pass through.

        .. note:: By default, we simply use the element object itself as the key,
            so that object must be hashable. If that's not the case, a non-default
            key must be provided.

    keep: str
        Which element to keep in the case that a unique key is already found
        in the group. If "first", keep element from the first occurrence of a given
        key; if "last", keep element from the most recent occurrence. Note that
        relative ordering of *elements* is preserved in the data passed through,
        and not ordering of *keys*.
    **kwargs

    Examples
    --------
    >>> source = Stream()
    >>> stream = source.partition_unique(n=3, keep="first").sink(print)
    >>> eles = [1, 2, 1, 3, 1, 3, 3, 2]
    >>> for ele in eles:
    ...     source.emit(ele)
    (1, 2, 3)
    (1, 3, 2)

    >>> source = Stream()
    >>> stream = source.partition_unique(n=3, keep="last").sink(print)
    >>> eles = [1, 2, 1, 3, 1, 3, 3, 2]
    >>> for ele in eles:
    ...     source.emit(ele)
    (2, 1, 3)
    (1, 3, 2)

    >>> source = Stream()
    >>> stream = source.partition_unique(n=3, key=lambda x: len(x), keep="last").sink(print)
    >>> eles = ["f", "fo", "f", "foo", "f", "foo", "foo", "fo"]
    >>> for ele in eles:
    ...     source.emit(ele)
    ('fo', 'f', 'foo')
    ('f', 'foo', 'fo')
    """
    _graphviz_shape = "diamond"

    def __init__(
        self,
        upstream,
        n: int,
        key: Union[Hashable, Callable[[Any], Hashable]] = identity,
        keep: str = "first",  # Literal["first", "last"]
        **kwargs
    ):
        self.n = n
        self.key = key
        self.keep = keep
        self._buffer = {}
        self._metadata_buffer = {}
        Stream.__init__(self, upstream, **kwargs)

    def _get_key(self, x):
        if callable(self.key):
            return self.key(x)
        else:
            return x[self.key]

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        y = self._get_key(x)
        if self.keep == "last":
            # remove key if already present so that emitted value
            # will reflect elements' actual relative ordering
            self._buffer.pop(y, None)
            self._metadata_buffer.pop(y, None)
            self._buffer[y] = x
            self._metadata_buffer[y] = metadata
        else:  # self.keep == "first"
            if y not in self._buffer:
                self._buffer[y] = x
                self._metadata_buffer[y] = metadata
        if len(self._buffer) == self.n:
            result, self._buffer = tuple(self._buffer.values()), {}
            metadata_result, self._metadata_buffer = list(self._metadata_buffer.values()), {}
            ret = self._emit(result, metadata_result)
            self._release_refs(metadata_result)
            return ret
        else:
            return []


@Stream.register_api()
class sliding_window(Stream):
    """ Produce overlapping tuples of size n

    Parameters
    ----------
    return_partial : bool
        If True, yield tuples as soon as any events come in, each tuple being
        smaller or equal to the window size. If False, only start yielding
        tuples once a full window has accrued.

    Examples
    --------
    >>> source = Stream()
    >>> source.sliding_window(3, return_partial=False).sink(print)
    >>> for i in range(8):
    ...     source.emit(i)
    (0, 1, 2)
    (1, 2, 3)
    (2, 3, 4)
    (3, 4, 5)
    (4, 5, 6)
    (5, 6, 7)
    """
    _graphviz_shape = 'diamond'

    def __init__(self, upstream, n, return_partial=True, **kwargs):
        self.n = n
        self._buffer = deque(maxlen=n)
        self.metadata_buffer = deque(maxlen=n)
        self.partial = return_partial
        Stream.__init__(self, upstream, **kwargs)

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        self._buffer.append(x)
        if not isinstance(metadata, list):
            metadata = [metadata]
        self.metadata_buffer.append(metadata)
        if self.partial or len(self._buffer) == self.n:
            flat_metadata = [m for ml in self.metadata_buffer for m in ml]
            ret = self._emit(tuple(self._buffer), flat_metadata)
            if len(self.metadata_buffer) == self.n:
                completed = self.metadata_buffer.popleft()
                self._release_refs(completed)
            return ret
        else:
            return []


def convert_interval(interval):
    if isinstance(interval, str):
        import pandas as pd
        interval = pd.Timedelta(interval).total_seconds()
    return interval


@Stream.register_api()
class timed_window(Stream):
    """ Emit a tuple of collected results every interval

    Every ``interval`` seconds this emits a tuple of all of the results
    seen so far.  This can help to batch data coming off of a high-volume
    stream.
    """
    _graphviz_shape = 'octagon'

    def __init__(self, upstream, interval, **kwargs):
        self.interval = convert_interval(interval)
        self._buffer = []
        self.metadata_buffer = []
        self.last = gen.moment

        kwargs["ensure_io_loop"] = True
        Stream.__init__(self, upstream, **kwargs)

        self.loop.add_callback(self.cb)

    def update(self, x, who=None, metadata=None):
        self._buffer.append(x)
        self._retain_refs(metadata)
        self.metadata_buffer.append(metadata)
        return self.last

    @gen.coroutine
    def cb(self):
        while True:
            L, self._buffer = self._buffer, []
            metadata, self.metadata_buffer = self.metadata_buffer, []
            m = [m for ml in metadata for m in ml]
            self.last = self._emit(L, m)
            self._release_refs(m)
            yield self.last
            yield gen.sleep(self.interval)


@Stream.register_api()
class timed_window_unique(Stream):
    """
    Emit a group of elements with unique keys every ``interval`` seconds.

    Parameters
    ----------
    interval: Union[int, str]
        Number of seconds over which to group elements, or a ``pandas``-style
        duration string that can be converted into seconds.
    key: Union[Hashable, Callable[[Any], Hashable]]
        Callable that accepts a stream element and returns a unique, hashable
        representation of the incoming data (``key(x)``), or a hashable that gets
        the corresponding value of a stream element (``x[key]``). For example, both
        ``key=lambda x: x["a"]`` and ``key="a"`` would allow only elements with unique
        ``"a"`` values to pass through.

        .. note:: By default, we simply use the element object itself as the key,
            so that object must be hashable. If that's not the case, a non-default
            key must be provided.

    keep: str
        Which element to keep in the case that a unique key is already found
        in the group. If "first", keep element from the first occurrence of a given
        key; if "last", keep element from the most recent occurrence. Note that
        relative ordering of *elements* is preserved in the data passed through,
        and not ordering of *keys*.

    Examples
    --------
    >>> source = Stream()

    Get unique hashable elements in a window, keeping just the first occurrence:
    >>> stream = source.timed_window_unique(interval=1.0, keep="first").sink(print)
    >>> for ele in [1, 2, 3, 3, 2, 1]:
    ...     source.emit(ele)
    ()
    (1, 2, 3)
    ()

    Get unique hashable elements in a window, keeping just the last occurrence:
    >>> stream = source.timed_window_unique(interval=1.0, keep="last").sink(print)
    >>> for ele in [1, 2, 3, 3, 2, 1]:
    ...     source.emit(ele)
    ()
    (3, 2, 1)
    ()

    Get unique elements in a window by (string) length, keeping just the first occurrence:
    >>> stream = source.timed_window_unique(interval=1.0, key=len, keep="first")
    >>> for ele in ["f", "b", "fo", "ba", "foo", "bar"]:
    ...     source.emit(ele)
    ()
    ('f', 'fo', 'foo')
    ()

    Get unique elements in a window by (string) length, keeping just the last occurrence:
    >>> stream = source.timed_window_unique(interval=1.0, key=len, keep="last")
    >>> for ele in ["f", "b", "fo", "ba", "foo", "bar"]:
    ...     source.emit(ele)
    ()
    ('b', 'ba', 'bar')
    ()
    """
    _graphviz_shape = "octagon"

    def __init__(
        self,
        upstream,
        interval: Union[int, str],
        key: Union[Hashable, Callable[[Any], Hashable]] = identity,
        keep: str = "first",  # Literal["first", "last"]
        **kwargs
    ):
        self.interval = convert_interval(interval)
        self.key = key
        self.keep = keep
        self._buffer = {}
        self._metadata_buffer = {}
        self.last = gen.moment
        kwargs["ensure_io_loop"] = True
        Stream.__init__(self, upstream, **kwargs)
        self.loop.add_callback(self.cb)

    def _get_key(self, x):
        if callable(self.key):
            return self.key(x)
        else:
            return x[self.key]

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        y = self._get_key(x)
        if self.keep == "last":
            # remove key if already present so that emitted value
            # will reflect elements' actual relative ordering
            self._buffer.pop(y, None)
            self._metadata_buffer.pop(y, None)
            self._buffer[y] = x
            self._metadata_buffer[y] = metadata
        else:  # self.keep == "first"
            if y not in self._buffer:
                self._buffer[y] = x
                self._metadata_buffer[y] = metadata
        return self.last

    @gen.coroutine
    def cb(self):
        while True:
            result, self._buffer = tuple(self._buffer.values()), {}
            metadata_result, self._metadata_buffer = list(self._metadata_buffer.values()), {}
            # TODO: figure out why metadata_result is handled differently here...
            m = [m for ml in metadata_result for m in ml]
            self.last = self._emit(result, m)
            self._release_refs(m)
            yield self.last
            yield gen.sleep(self.interval)


@Stream.register_api()
class delay(Stream):
    """ Add a time delay to results """
    _graphviz_shape = 'octagon'

    def __init__(self, upstream, interval, **kwargs):
        self.interval = convert_interval(interval)
        self.queue = Queue()

        kwargs["ensure_io_loop"] = True
        Stream.__init__(self, upstream,**kwargs)

        self.loop.add_callback(self.cb)

    @gen.coroutine
    def cb(self):
        while True:
            last = time()
            x, metadata = yield self.queue.get()
            yield self._emit(x, metadata=metadata)
            self._release_refs(metadata)
            duration = self.interval - (time() - last)
            if duration > 0:
                yield gen.sleep(duration)

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        return self.queue.put((x, metadata))


@Stream.register_api()
class rate_limit(Stream):
    """ Limit the flow of data

    This stops two elements of streaming through in an interval shorter
    than the provided value.

    Parameters
    ----------
    interval: float
        Time in seconds
    """
    _graphviz_shape = 'octagon'

    def __init__(self, upstream, interval, **kwargs):
        self.interval = convert_interval(interval)
        self.next = 0

        kwargs["ensure_io_loop"] = True
        Stream.__init__(self, upstream, **kwargs)

    @gen.coroutine
    def update(self, x, who=None, metadata=None):
        now = time()
        old_next = self.next
        self.next = max(now, self.next) + self.interval
        if now < old_next:
            yield gen.sleep(old_next - now)
        yield self._emit(x, metadata=metadata)


@Stream.register_api()
class buffer(Stream):
    """ Allow results to pile up at this point in the stream

    This allows results to buffer in place at various points in the stream.
    This can help to smooth flow through the system when backpressure is
    applied.
    """
    _graphviz_shape = 'diamond'

    def __init__(self, upstream, n, **kwargs):
        self.queue = Queue(maxsize=n)

        kwargs["ensure_io_loop"] = True
        Stream.__init__(self, upstream, **kwargs)

        self.loop.add_callback(self.cb)

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        return self.queue.put((x, metadata))

    @gen.coroutine
    def cb(self):
        while True:
            x, metadata = yield self.queue.get()
            yield self._emit(x, metadata=metadata)
            self._release_refs(metadata)


@Stream.register_api()
class zip(Stream):
    """ Combine streams together into a stream of tuples

    We emit a new tuple once all streams have produce a new tuple.

    See also
    --------
    combine_latest
    zip_latest
    """
    _graphviz_orientation = 270
    _graphviz_shape = 'triangle'

    def __init__(self, *upstreams, **kwargs):
        self.maxsize = kwargs.pop('maxsize', 10)
        self.condition = Condition()
        self.literals = [(i, val) for i, val in enumerate(upstreams)
                         if not isinstance(val, Stream)]

        self.buffers = {upstream: deque()
                        for upstream in upstreams
                        if isinstance(upstream, Stream)}

        upstreams2 = [upstream for upstream in upstreams if isinstance(upstream, Stream)]

        Stream.__init__(self, upstreams=upstreams2, **kwargs)

    def _add_upstream(self, upstream):
        # Override method to handle setup of buffer for new stream
        self.buffers[upstream] = deque()
        super(zip, self)._add_upstream(upstream)

    def _remove_upstream(self, upstream):
        # Override method to handle removal of buffer for stream
        self.buffers.pop(upstream)
        super(zip, self)._remove_upstream(upstream)

    def pack_literals(self, tup):
        """ Fill buffers for literals whenever we empty them """
        inp = list(tup)[::-1]
        out = []
        for i, val in self.literals:
            while len(out) < i:
                out.append(inp.pop())
            out.append(val)

        while inp:
            out.append(inp.pop())

        return tuple(out)

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        L = self.buffers[who]  # get buffer for stream
        L.append((x, metadata))
        if len(L) == 1 and all(self.buffers.values()):
            vals = [self.buffers[up][0] for up in self.upstreams]
            tup, md = __builtins__['zip'](*vals)
            for buf in self.buffers.values():
                buf.popleft()
            self.condition.notify_all()
            if self.literals:
                tup = self.pack_literals(tup)
            md = [m for ml in md for m in ml]
            ret = self._emit(tup, md)
            self._release_refs(md)
            return ret
        elif len(L) > self.maxsize:
            return self.condition.wait()


@Stream.register_api()
class combine_latest(Stream):
    """ Combine multiple streams together to a stream of tuples

    This will emit a new tuple of all of the most recent elements seen from
    any stream.

    Parameters
    ----------
    emit_on : stream or list of streams or None
        only emit upon update of the streams listed.
        If None, emit on update from any stream

    See Also
    --------
    zip
    """
    _graphviz_orientation = 270
    _graphviz_shape = 'triangle'

    def __init__(self, *upstreams, **kwargs):
        emit_on = kwargs.pop('emit_on', None)
        self._initial_emit_on = emit_on

        self.last = [None for _ in upstreams]
        self.metadata = [None for _ in upstreams]
        self.missing = set(upstreams)
        if emit_on is not None:
            if not isinstance(emit_on, Iterable):
                emit_on = (emit_on, )
            emit_on = tuple(
                upstreams[x] if isinstance(x, int) else x for x in emit_on)
            self.emit_on = emit_on
        else:
            self.emit_on = upstreams
        Stream.__init__(self, upstreams=upstreams, **kwargs)

    def _add_upstream(self, upstream):
        # Override method to handle setup of last and missing for new stream
        self.last.append(None)
        self.metadata.append(None)
        self.missing.update([upstream])
        super(combine_latest, self)._add_upstream(upstream)
        if self._initial_emit_on is None:
            self.emit_on = self.upstreams

    def _remove_upstream(self, upstream):
        # Override method to handle removal of last and missing for stream
        if self.emit_on == upstream:
            raise RuntimeError("Can't remove the ``emit_on`` stream since that"
                               "would cause no data to be emitted. "
                               "Consider adding an ``emit_on`` first by "
                               "running ``node.emit_on=(upstream,)`` to add "
                               "a new ``emit_on`` or running "
                               "``node.emit_on=tuple(node.upstreams)`` to "
                               "emit on all incoming data")
        self.last.pop(self.upstreams.index(upstream))
        self.metadata.pop(self.upstreams.index(upstream))
        self.missing.remove(upstream)
        super(combine_latest, self)._remove_upstream(upstream)
        if self._initial_emit_on is None:
            self.emit_on = self.upstreams

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        idx = self.upstreams.index(who)
        if self.metadata[idx]:
            self._release_refs(self.metadata[idx])
        self.metadata[idx] = metadata

        if self.missing and who in self.missing:
            self.missing.remove(who)

        self.last[idx] = x
        if not self.missing and who in self.emit_on:
            tup = tuple(self.last)
            md = [m for ml in self.metadata for m in ml]
            return self._emit(tup, md)


@Stream.register_api()
class flatten(Stream):
    """ Flatten streams of lists or iterables into a stream of elements

    Examples
    --------
    >>> source = Stream()
    >>> source.flatten().sink(print)
    >>> for x in [[1, 2, 3], [4, 5], [6, 7, 7]]:
    ...     source.emit(x)
    1
    2
    3
    4
    5
    6
    7

    See Also
    --------
    partition
    """
    def update(self, x, who=None, metadata=None):
        L = []
        for i, item in enumerate(x):
            if i == len(x) - 1:
                y = self._emit(item, metadata=metadata)
            else:
                y = self._emit(item)
            if type(y) is list:
                L.extend(y)
            else:
                L.append(y)
        return L


@Stream.register_api()
class unique(Stream):
    """ Avoid sending through repeated elements

    This deduplicates a stream so that only new elements pass through.
    You can control how much of a history is stored with the ``maxsize=``
    parameter.  For example setting ``maxsize=1`` avoids sending through
    elements when one is repeated right after the other.

    Parameters
    ----------
    maxsize: int or None, optional
        number of stored unique values to check against
    key : function, optional
        Function which returns a representation of the incoming data.
        For example ``key=lambda x: x['a']`` could be used to allow only
        pieces of data with unique ``'a'`` values to pass through.
    hashable : bool, optional
        If True then data is assumed to be hashable, else it is not. This is
        used for determining how to cache the history, if hashable then
        either dicts or LRU caches are used, otherwise a deque is used.
        Defaults to True.

    Examples
    --------
    >>> source = Stream()
    >>> source.unique(maxsize=1).sink(print)
    >>> for x in [1, 1, 2, 2, 2, 1, 3]:
    ...     source.emit(x)
    1
    2
    1
    3
    """
    def __init__(self, upstream, maxsize=None, key=identity, hashable=True,
                 **kwargs):
        self.key = key
        self.maxsize = maxsize
        if hashable:
            self.seen = dict()
            if self.maxsize:
                from zict import LRU
                self.seen = LRU(self.maxsize, self.seen)
        else:
            self.seen = []

        Stream.__init__(self, upstream, **kwargs)

    def update(self, x, who=None, metadata=None):
        y = self.key(x)
        emit = True
        if isinstance(self.seen, list):
            if y in self.seen:
                self.seen.remove(y)
                emit = False
            self.seen.insert(0, y)
            if self.maxsize:
                del self.seen[self.maxsize:]
            if emit:
                return self._emit(x, metadata=metadata)
        else:
            if self.seen.get(y, '~~not_seen~~') == '~~not_seen~~':
                self.seen[y] = 1
                return self._emit(x, metadata=metadata)


@Stream.register_api()
class union(Stream):
    """ Combine multiple streams into one

    Every element from any of the upstreams streams will immediately flow
    into the output stream.  They will not be combined with elements from
    other streams.

    See also
    --------
    Stream.zip
    Stream.combine_latest
    """
    def __init__(self, *upstreams, **kwargs):
        super(union, self).__init__(upstreams=upstreams, **kwargs)

    def update(self, x, who=None, metadata=None):
        return self._emit(x, metadata=metadata)


@Stream.register_api()
class pluck(Stream):
    """ Select elements from elements in the stream.

    Parameters
    ----------
    pluck : object, list
        The element(s) to pick from the incoming element in the stream
        If an instance of list, will pick multiple elements.

    Examples
    --------
    >>> source = Stream()
    >>> source.pluck([0, 3]).sink(print)
    >>> for x in [[1, 2, 3, 4], [4, 5, 6, 7], [8, 9, 10, 11]]:
    ...     source.emit(x)
    (1, 4)
    (4, 7)
    (8, 11)

    >>> source = Stream()
    >>> source.pluck('name').sink(print)
    >>> for x in [{'name': 'Alice', 'x': 123}, {'name': 'Bob', 'x': 456}]:
    ...     source.emit(x)
    'Alice'
    'Bob'
    """
    def __init__(self, upstream, pick, **kwargs):
        self.pick = pick
        super(pluck, self).__init__(upstream, **kwargs)

    def update(self, x, who=None, metadata=None):
        if isinstance(self.pick, list):
            return self._emit(tuple([x[ind] for ind in self.pick]),
                              metadata=metadata)
        else:
            return self._emit(x[self.pick], metadata=metadata)


@Stream.register_api()
class collect(Stream):
    """
    Hold elements in a cache and emit them as a collection when flushed.

    Examples
    --------
    >>> source1 = Stream()
    >>> source2 = Stream()
    >>> collector = collect(source1)
    >>> collector.sink(print)
    >>> source2.sink(collector.flush)
    >>> source1.emit(1)
    >>> source1.emit(2)
    >>> source2.emit('anything')  # flushes collector
    ...
    [1, 2]
    """
    def __init__(self, upstream, cache=None, metadata_cache=None, **kwargs):
        if cache is None:
            cache = deque()
        self.cache = cache

        if metadata_cache is None:
            metadata_cache = deque()
        self.metadata_cache = metadata_cache

        Stream.__init__(self, upstream, **kwargs)

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        self.cache.append(x)
        if metadata:
            if isinstance(metadata, list):
                self.metadata_cache.extend(metadata)
            else:
                self.metadata_cache.append(metadata)

    def flush(self, _=None):
        out = tuple(self.cache)
        metadata = list(self.metadata_cache)
        self._emit(out, metadata)
        self._release_refs(metadata)
        self.cache.clear()
        self.metadata_cache.clear()


@Stream.register_api()
class zip_latest(Stream):
    """Combine multiple streams together to a stream of tuples

    The stream which this is called from is lossless. All elements from
    the lossless stream are emitted reguardless of when they came in.
    This will emit a new tuple consisting of an element from the lossless
    stream paired with the latest elements from the other streams.
    Elements are only emitted when an element on the lossless stream are
    received, similar to ``combine_latest`` with the ``emit_on`` flag.

    See Also
    --------
    Stream.combine_latest
    Stream.zip
    """
    def __init__(self, lossless, *upstreams, **kwargs):
        upstreams = (lossless,) + upstreams
        self.last = [None for _ in upstreams]
        self.metadata = [None for _ in upstreams]
        self.missing = set(upstreams)
        self.lossless = lossless
        self.lossless_buffer = deque()
        Stream.__init__(self, upstreams=upstreams, **kwargs)

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        idx = self.upstreams.index(who)
        if who is self.lossless:
            self.lossless_buffer.append((x, metadata))
        elif self.metadata[idx]:
            self._release_refs(self.metadata[idx])
        self.metadata[idx] = metadata
        self.last[idx] = x
        if self.missing and who in self.missing:
            self.missing.remove(who)

        if not self.missing:
            L = []
            while self.lossless_buffer:
                self.last[0], self.metadata[0] = self.lossless_buffer.popleft()
                md = [m for ml in self.metadata for m in ml]
                L.append(self._emit(tuple(self.last), md))
                self._release_refs(self.metadata[0])
            return L


@Stream.register_api()
class latest(Stream):
    """ Drop held-up data and emit the latest result

    This allows you to skip intermediate elements in the stream if there is
    some back pressure causing a slowdown.  Use this when you only care about
    the latest elements, and are willing to lose older data.

    This passes through values without modification otherwise.

    Examples
    --------
    >>> source.map(f).latest().map(g)  # doctest: +SKIP
    """
    _graphviz_shape = 'octagon'

    def __init__(self, upstream, **kwargs):
        self.condition = Condition()
        self.next = []
        self.next_metadata = None

        kwargs["ensure_io_loop"] = True
        Stream.__init__(self, upstream, **kwargs)

        self.loop.add_callback(self.cb)

    def update(self, x, who=None, metadata=None):
        if self.next_metadata:
            self._release_refs(self.next_metadata)
        self._retain_refs(metadata)

        self.next = [x]
        self.next_metadata = metadata
        self.loop.add_callback(self.condition.notify)

    @gen.coroutine
    def cb(self):
        while True:
            yield self.condition.wait()
            [x] = self.next
            yield self._emit(x, self.next_metadata)


def sync(loop, func, *args, **kwargs):
    """
    Run coroutine in loop running in separate thread.
    """
    # This was taken from distrbuted/utils.py

    timeout = kwargs.pop('callback_timeout', None)

    e = threading.Event()
    main_tid = get_thread_identity()
    result = [None]
    error = [False]

    @gen.coroutine
    def f():
        try:
            if main_tid == get_thread_identity():
                raise RuntimeError("sync() called from thread of running loop")
            yield gen.moment
            thread_state.asynchronous = True
            future = func(*args, **kwargs)
            if timeout is not None:
                future = gen.with_timeout(timedelta(seconds=timeout), future)
            result[0] = yield future
        except Exception:
            error[0] = sys.exc_info()
        finally:
            thread_state.asynchronous = False
            e.set()

    loop.add_callback(f)
    if timeout is not None:
        if not e.wait(timeout):
            raise gen.TimeoutError("timed out after %s s." % (timeout,))
    else:
        while not e.is_set():
            e.wait(10)
    if error[0]:
        six.reraise(*error[0])
    else:
        return result[0]
