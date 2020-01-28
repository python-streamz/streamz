from __future__ import absolute_import, division, print_function

from collections import deque
from datetime import timedelta
import functools
import logging
import six
import sys
import threading
from time import time
import weakref

import toolz
from tornado import gen
from tornado.locks import Condition
from tornado.ioloop import IOLoop
from tornado.queues import Queue
try:
    from tornado.ioloop import PollIOLoop
except ImportError:
    PollIOLoop = None  # dropped in tornado 6.0

from collections import Iterable

from .compatibility import get_thread_identity
from .orderedweakset import OrderedWeakrefSet

no_default = '--no-default--'

_global_sinks = set()

_html_update_streams = set()

thread_state = threading.local()

logger = logging.getLogger(__name__)


_io_loops = []


def get_io_loop(asynchronous=None):
    if asynchronous:
        return IOLoop.current()

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
    def __init__(self, initial=0, cb=None, loop=None):
        self.loop = loop if loop else get_io_loop()
        self.count = initial
        self.cb = cb

    def retain(self, x=1):
        self.count += x

    def release(self, x=1):
        self.count -= x
        if self.count <= 0 and self.cb:
            self.loop.add_callback(self.cb)

    def __str__(self):
        return '<RefCounter count={}>'.format(self.count)

    __repr__ = __str__


class Stream(object):
    """ A Stream is an infinite sequence of data

    Streams subscribe to each other passing and transforming data between them.
    A Stream object listens for updates from upstream, reacts to these updates,
    and then emits more data to flow downstream to all Stream objects that
    subscribe to it.  Downstream Stream objects may connect at any point of a
    Stream graph to get a full view of the data coming off of that point to do
    with as they will.

    Parameters
    ----------
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
        self.downstreams = OrderedWeakrefSet()
        if upstreams is not None:
            self.upstreams = list(upstreams)
        else:
            self.upstreams = [upstream]

        self._set_asynchronous(asynchronous)
        self._set_loop(loop)
        if ensure_io_loop and not self.loop:
            self._set_asynchronous(False)
        if self.loop is None and self.asynchronous is not None:
            self._set_loop(get_io_loop(self.asynchronous))

        for upstream in self.upstreams:
            if upstream:
                upstream.downstreams.add(self)

        self.name = stream_name

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
        if self.upstreams == [None]:
            self.upstreams[0] = upstream
        else:
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
        if len(self.upstreams) == 1:
            self.upstreams[0] = [None]
        else:
            self.upstreams.remove(upstream)

    @classmethod
    def register_api(cls, modifier=identity):
        """ Add callable to Stream API

        This allows you to register a new method onto this class.  You can use
        it as a decorator.::

            >>> @Stream.register_api()
            ... class foo(Stream):
            ...     ...

            >>> Stream().foo(...)  # this works now

        It attaches the callable as a normal attribute to the class object.  In
        doing so it respsects inheritance (all subclasses of Stream will also
        get the foo attribute).

        By default callables are assumed to be instance methods.  If you like
        you can include modifiers to apply before attaching to the class as in
        the following case where we construct a ``staticmethod``.

            >>> @Stream.register_api(staticmethod)
            ... class foo(Stream):
            ...     ...

            >>> Stream.foo(...)  # Foo operates as a static method
        """
        def _(func):
            @functools.wraps(func)
            def wrapped(*args, **kwargs):
                return func(*args, **kwargs)
            setattr(cls, func.__name__, modifier(wrapped))
            return func
        return _

    def start(self):
        """ Start any upstream sources """
        for upstream in self.upstreams:
            upstream.start()

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
                elif hasattr(at.__class__, '__name__'):
                    s = getattr(self, m).__class__.__name__
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

    def _ipython_display_(self, **kwargs):
        try:
            from ipywidgets import Output
            import IPython
        except ImportError:
            if hasattr(self, '_repr_html_'):
                return self._repr_html_()
            else:
                return self.__repr__()
        output = Output(_view_count=0)
        output_ref = weakref.ref(output)

        def update_cell(val):
            output = output_ref()
            if output is None:
                return
            with output:
                IPython.display.clear_output(wait=True)
                IPython.display.display(val)

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
        if metadata is None:
            metadata = {}

        refs = []
        if 'refs' in metadata:
            refs = metadata['refs']
            if not isinstance(refs, list):
                refs = [refs]

        result = []
        downstreams = list(self.downstreams)
        for ref in refs:
            ref.retain(len(downstreams))

        for downstream in downstreams:
            r = downstream.update(x, who=self, metadata=metadata)

            if type(r) is list:
                result.extend(r)
            else:
                result.append(r)

            for ref in refs:
                ref.release()

        return [element for element in result if element is not None]

    def emit(self, x, asynchronous=False, metadata=None):
        """ Push data into the stream at this point

        This is typically done only at source Streams but can theortically be
        done at any point
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
            @gen.coroutine
            def _():
                thread_state.asynchronous = True
                try:
                    result = yield self._emit(x)
                finally:
                    del thread_state.asynchronous

                raise gen.Return(result)
            sync(self.loop, _)

    def update(self, x, who=None, metadata=None):
        self._emit(x, metadata=metadata)

    def gather(self):
        """ This is a no-op for core streamz

        This allows gather to be used in both dask and core streams
        """
        return self

    def connect(self, downstream):
        ''' Connect this stream to a downstream element.

        Parameters
        ----------
        downstream: Stream
            The downstream stream to connect to
        '''
        self._add_downstream(downstream)
        downstream._add_upstream(self)

    def disconnect(self, downstream):
        ''' Disconnect this stream to a downstream element.

        Parameters
        ----------
        downstream: Stream
            The downstream stream to disconnect from
        '''
        self._remove_downstream(downstream)

        downstream._remove_upstream(self)

    @property
    def upstream(self):
        if len(self.upstreams) != 1:
            raise ValueError("Stream has multiple upstreams")
        else:
            return self.upstreams[0]

    def destroy(self, streams=None):
        """
        Disconnect this stream from any upstream sources
        """
        if streams is None:
            streams = self.upstreams
        for upstream in list(streams):
            upstream.downstreams.remove(self)
            self.upstreams.remove(upstream)

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

    def _retain_refs(self, metadata):
        if not isinstance(metadata, list):
            metadata = [metadata]
        for m in metadata:
            if m and 'refs' in m:
                refs = m['refs']
                if not isinstance(refs, list):
                    refs = [refs]
                for ref in refs:
                    ref.retain(1)

    def _release_refs(self, metadata):
        if not isinstance(metadata, list):
            metadata = [metadata]
        for m in metadata:
            if m and 'refs' in m:
                refs = m['refs']
                if not isinstance(refs, list):
                    refs = [refs]
                for ref in refs:
                    ref.release(1)


@Stream.register_api()
class sink(Stream):
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
    _graphviz_shape = 'trapezium'

    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        # take the stream specific kwargs out
        stream_name = kwargs.pop("stream_name", None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, upstream, stream_name=stream_name)
        _global_sinks.add(self)

    def update(self, x, who=None, metadata=None):
        result = self.func(x, *self.args, **self.kwargs)
        if gen.isawaitable(result):
            return result
        else:
            return []


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
        Stream.__init__(self, upstream, stream_name=stream_name)

    def update(self, x, who=None, metadata=None):
        if self.state is no_default:
            self.state = x
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

    Examples
    --------
    >>> source = Stream()
    >>> source.partition(3).sink(print)
    >>> for i in range(10):
    ...     source.emit(i)
    (0, 1, 2)
    (3, 4, 5)
    (6, 7, 8)
    """
    _graphviz_shape = 'diamond'

    def __init__(self, upstream, n, **kwargs):
        self.n = n
        self.buffer = []
        self.metadata_buffer = []
        Stream.__init__(self, upstream, **kwargs)

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        self.buffer.append(x)
        if isinstance(metadata, list):
            self.metadata_buffer.extend(metadata)
        else:
            self.metadata_buffer.append(metadata)
        if len(self.buffer) == self.n:
            result, self.buffer = self.buffer, []
            metadata_result, self.metadata_buffer = self.metadata_buffer, []
            ret = self._emit(tuple(result), list(metadata_result))
            for metadata in metadata_result:
                self._release_refs(metadata)
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
        self.buffer = deque(maxlen=n)
        self.metadata_buffer = deque(maxlen=n)
        self.partial = return_partial
        Stream.__init__(self, upstream, **kwargs)

    def update(self, x, who=None, metadata=None):
        self._retain_refs(metadata)
        self.buffer.append(x)
        if not isinstance(metadata, list):
            metadata = [metadata]
        self.metadata_buffer.append(metadata)
        if self.partial or len(self.buffer) == self.n:
            flat_metadata = [m for l in self.metadata_buffer for m in l]
            ret = self._emit(tuple(self.buffer), flat_metadata)
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
        self.buffer = []
        self.metadata_buffer = []
        self.last = gen.moment

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)

        self.loop.add_callback(self.cb)

    def update(self, x, who=None, metadata=None):
        self.buffer.append(x)
        self._retain_refs(metadata)
        self.metadata_buffer.append(metadata)
        return self.last

    @gen.coroutine
    def cb(self):
        while True:
            L, self.buffer = self.buffer, []
            metadata, self.metadata_buffer = self.metadata_buffer, []
            self.last = self._emit(L, metadata)
            for m in metadata:
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

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)

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

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)

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

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)

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
            ret = self._emit(tup, list(md))
            for m in md:
                self._release_refs(m)
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
            return self._emit(tup, self.metadata)


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
                return self._emit(x)


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
                L.append(self._emit(tuple(self.last), metadata=self.metadata))
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

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)

        self.loop.add_callback(self.cb)

    def update(self, x, who=None, metadata=None):
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


@Stream.register_api()
class to_kafka(Stream):
    """ Writes data in the stream to Kafka

    This stream accepts a string or bytes object. Call ``flush`` to ensure all
    messages are pushed. Responses from Kafka are pushed downstream.

    Parameters
    ----------
    topic : string
        The topic which to write
    producer_config : dict
        Settings to set up the stream, see
        https://docs.confluent.io/current/clients/confluent-kafka-python/#configuration
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        Examples:
        bootstrap.servers: Connection string (host:port) to Kafka

    Examples
    --------
    >>> from streamz import Stream
    >>> ARGS = {'bootstrap.servers': 'localhost:9092'}
    >>> source = Stream()
    >>> kafka = source.map(lambda x: str(x)).to_kafka('test', ARGS)
    <to_kafka>
    >>> for i in range(10):
    ...     source.emit(i)
    >>> kafka.flush()
    """
    def __init__(self, upstream, topic, producer_config, **kwargs):
        import confluent_kafka as ck

        self.topic = topic
        self.producer = ck.Producer(producer_config)

        Stream.__init__(self, upstream, ensure_io_loop=True, **kwargs)
        self.stopped = False
        self.polltime = 0.2
        self.loop.add_callback(self.poll)
        self.futures = []

    @gen.coroutine
    def poll(self):
        while not self.stopped:
            # executes callbacks for any delivered data, in this thread
            # if no messages were sent, nothing happens
            self.producer.poll(0)
            yield gen.sleep(self.polltime)

    def update(self, x, who=None, metadata=None):
        future = gen.Future()
        self.futures.append(future)

        @gen.coroutine
        def _():
            while True:
                try:
                    # this runs asynchronously, in C-K's thread
                    self.producer.produce(self.topic, x, callback=self.cb)
                    return
                except BufferError:
                    yield gen.sleep(self.polltime)
                except Exception as e:
                    future.set_exception(e)
                    return

        self.loop.add_callback(_)
        return future

    @gen.coroutine
    def cb(self, err, msg):
        future = self.futures.pop(0)
        if msg is not None and msg.value() is not None:
            future.set_result(None)
            yield self._emit(msg.value())
        else:
            future.set_exception(err or msg.error())

    def flush(self, timeout=-1):
        self.producer.flush(timeout)


def sync(loop, func, *args, **kwargs):
    """
    Run coroutine in loop running in separate thread.
    """
    # This was taken from distrbuted/utils.py

    # Tornado's PollIOLoop doesn't raise when using closed, do it ourselves
    if PollIOLoop and ((isinstance(loop, PollIOLoop) and getattr(loop, '_closing', False))
            or (hasattr(loop, 'asyncio_loop') and loop.asyncio_loop._closed)):
        raise RuntimeError("IOLoop is closed")

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
