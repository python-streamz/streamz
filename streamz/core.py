from __future__ import absolute_import, division, print_function

from collections import deque
import functools
from time import time
import weakref

import toolz
from tornado import gen
from tornado.locks import Condition
from tornado.ioloop import IOLoop
from tornado.queues import Queue
from collections import Iterable

from .compatibility import builtins

no_default = '--no-default--'

_global_sinks = set()


def identity(x):
    return x


class Stream(object):
    """ A Stream is an infinite sequence of data

    Streams subscribe to each other passing and transforming data between them.
    A Stream object listens for updates from upstream, reacts to these updates,
    and then emits more data to flow downstream to all Stream objects that
    subscribe to it.  Downstream Stream objects may connect at any point of a
    Stream graph to get a full view of the data coming off of that point to do
    with as they will.

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

    def __init__(self, child=None, children=None, stream_name=None, **kwargs):
        self.parents = weakref.WeakSet()
        if children is not None:
            self.children = children
        else:
            self.children = [child]
        if kwargs.get('loop'):
            self._loop = kwargs.get('loop')
        for child in self.children:
            if child:
                child.parents.add(self)
        self.name = stream_name

    @classmethod
    def register_api(cls, func):
        @functools.wraps(func)
        def _(*args, **kwargs):
            return func(*args, **kwargs)
        setattr(cls, func.__name__, _)
        return func

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

    def emit(self, x):
        """ Push data into the stream at this point

        This is typically done only at source Streams but can theortically be
        done at any point
        """
        result = []
        for parent in self.parents:
            r = parent.update(x, who=self)
            if type(r) is list:
                result.extend(r)
            else:
                result.append(r)
        return [element for element in result if element is not None]

    def update(self, x, who=None):
        self.emit(x)

    def connect(self, parent):
        ''' Connect this stream to a downstream element.

        Parameters
        ----------
        parent: Stream
            The parent stream (downstream element) to connect to
        '''
        # Note : parents go downstream and children go upstream.
        self.parents.add(parent)

        if parent.children == [None]:
            parent.children = [self]
        else:
            parent.children.append(self)

    def disconnect(self, parent):
        ''' Disconnect this stream to a downstream element.

        Parameters
        ----------
        parent: Stream
            The parent stream (downstream element) to disconnect from
        '''
        self.parents.remove(parent)

        parent.children.remove(self)

    @property
    def child(self):
        if len(self.children) != 1:
            raise ValueError("Stream has multiple children")
        else:
            return self.children[0]

    @property
    def loop(self):
        try:
            return self._loop
        except AttributeError:
            pass
        for child in self.children:
            if child:
                loop = self.child.loop
                if loop:
                    self._loop = loop
                    return loop
        self._loop = IOLoop.current()
        return self._loop

    def scatter(self):
        from .dask import scatter
        return scatter(self)

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

    def frequencies(self):
        """ Count occurrences of elements """
        def update_frequencies(last, x):
            return toolz.assoc(last, x, last.get(x, 0) + 1)

        return self.scan(update_frequencies, start={})

    def visualize(self, filename='mystream.png', **kwargs):
        """Render the computation of this object's task graph using graphviz.

        Requires ``graphviz`` to be installed.

        Parameters
        ----------
        node: Stream instance
            A node in the task graph
        filename : str, optional
            The name of the file to write to disk.
        kwargs:
            Graph attributes to pass to graphviz like ``rankdir="LR"``
        """
        from .graph import visualize
        return visualize(self, filename, **kwargs)

    def to_dataframe(self, example):
        """ Convert a stream of Pandas dataframes to a StreamingDataFrame

        Examples
        --------
        >>> source = Stream()
        >>> sdf = source.to_dataframe()
        >>> L = sdf.groupby(sdf.x).y.mean().stream.sink_to_list()
        >>> source.emit(pd.DataFrame(...))  # doctest: +SKIP
        >>> source.emit(pd.DataFrame(...))  # doctest: +SKIP
        >>> source.emit(pd.DataFrame(...))  # doctest: +SKIP
        """
        from .dataframe import StreamingDataFrame
        return StreamingDataFrame(stream=self, example=example)

    def to_batch(self, **kwargs):
        """ Convert a stream of lists to a StreamingBatch

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
        from .batch import StreamingBatch
        return StreamingBatch(stream=self, **kwargs)

    @staticmethod
    def from_textfile(f, poll_interval=None):
        """ Read data from file into stream

        Parameters
        ----------
        f: file or string
            File object or filename to read
        poll_interval: number
            Number of seconds between which to poll the file
            If None then don't poll, and instead return without waiting

        Examples
        --------
        >>> source = Stream()
        >>> source.map(json.loads).pluck('value').sum()
        >>> source.from_textfile('myfile.json')

        Returns
        -------
        Nothing.  This has the side effect of emitting data into the stream.
        """
        from .sources import TextFile
        return TextFile(f, poll_interval=poll_interval)


@Stream.register_api
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

    def __init__(self, child, func):
        self.func = func

        Stream.__init__(self, child)
        _global_sinks.add(self)

    def update(self, x, who=None):
        result = self.func(x)
        if type(result) is gen.Future:
            return result
        else:
            return []


@Stream.register_api
class map(Stream):
    """ Apply a function to every element in the stream """
    def __init__(self, child, func, *args, **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, child)

    def update(self, x, who=None):
        result = self.func(x, *self.args, **self.kwargs)

        return self.emit(result)


def _truthy(x):
    return not not x


@Stream.register_api
class filter(Stream):
    """ Only pass through elements that satisfy the predicate """
    def __init__(self, child, predicate):
        if predicate is None:
            predicate = _truthy
        self.predicate = predicate

        Stream.__init__(self, child)

    def update(self, x, who=None):
        if self.predicate(x):
            return self.emit(x)


@Stream.register_api
class accumulate(Stream):
    """ Accumulate results with previous state

    This preforms running or cumulative reductions, applying the function
    to the previous total and the new element.  The function should take
    two arguments, the previous accumulated state and the next element and
    it should return a new accumulated state.

    Parameters
    ----------
    func: callable
    start: object
        Initial value.  Defaults to the first submitted element
    returns_state: boolean
        If true then func should return both the state and the value to emit
        If false then both values are the same, and func returns one value
    **kwargs:
        Keyword arguments to pass to func

    Examples
    --------
    >>> source = Stream()
    >>> source.accumulate(lambda acc, x: acc + x).sink(print)
    ...
    >>> for i in range(5):
    ...     source.emit(i)
    1
    3
    6
    10
    """
    _graphviz_shape = 'box'

    def __init__(self, child, func, start=no_default, returns_state=False,
                 **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.state = start
        self.returns_state = returns_state
        Stream.__init__(self, child)

    def update(self, x, who=None):
        if self.state is no_default:
            self.state = x
            return self.emit(x)
        else:
            result = self.func(self.state, x, **self.kwargs)
            if self.returns_state:
                state, result = result
            else:
                state = result
            self.state = state
            return self.emit(result)


@Stream.register_api
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

    def __init__(self, child, n):
        self.n = n
        self.buffer = []
        Stream.__init__(self, child)

    def update(self, x, who=None):
        self.buffer.append(x)
        if len(self.buffer) == self.n:
            result, self.buffer = self.buffer, []
            return self.emit(tuple(result))
        else:
            return []


@Stream.register_api
class sliding_window(Stream):
    """ Produce overlapping tuples of size n

    Examples
    --------
    >>> source = Stream()
    >>> source.sliding_window(3).sink(print)
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

    def __init__(self, child, n):
        self.n = n
        self.buffer = deque(maxlen=n)
        Stream.__init__(self, child)

    def update(self, x, who=None):
        self.buffer.append(x)
        if len(self.buffer) == self.n:
            return self.emit(tuple(self.buffer))
        else:
            return []


@Stream.register_api
class timed_window(Stream):
    """ Emit a tuple of collected results every interval

    Every ``interval`` seconds this emits a tuple of all of the results
    seen so far.  This can help to batch data coming off of a high-volume
    stream.
    """
    _graphviz_shape = 'octagon'

    def __init__(self, child, interval, loop=None):
        self.interval = interval
        self.buffer = []
        self.last = gen.moment

        Stream.__init__(self, child, loop=loop)

        self.loop.add_callback(self.cb)

    def update(self, x, who=None):
        self.buffer.append(x)
        return self.last

    @gen.coroutine
    def cb(self):
        while True:
            L, self.buffer = self.buffer, []
            self.last = self.emit(L)
            yield self.last
            yield gen.sleep(self.interval)


@Stream.register_api
class delay(Stream):
    """ Add a time delay to results """
    _graphviz_shape = 'octagon'

    def __init__(self, child, interval, loop=None):
        self.interval = interval
        self.queue = Queue()

        Stream.__init__(self, child, loop=loop)

        self.loop.add_callback(self.cb)

    @gen.coroutine
    def cb(self):
        while True:
            last = time()
            x = yield self.queue.get()
            yield self.emit(x)
            duration = self.interval - (time() - last)
            if duration > 0:
                yield gen.sleep(duration)

    def update(self, x, who=None):
        return self.queue.put(x)


@Stream.register_api
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

    def __init__(self, child, interval):
        self.interval = interval
        self.next = 0

        Stream.__init__(self, child)

    @gen.coroutine
    def update(self, x, who=None):
        now = time()
        old_next = self.next
        self.next = max(now, self.next) + self.interval
        if now < old_next:
            yield gen.sleep(old_next - now)
        yield self.emit(x)


@Stream.register_api
class buffer(Stream):
    """ Allow results to pile up at this point in the stream

    This allows results to buffer in place at various points in the stream.
    This can help to smooth flow through the system when backpressure is
    applied.
    """
    _graphviz_shape = 'diamond'

    def __init__(self, child, n, loop=None):
        self.queue = Queue(maxsize=n)

        Stream.__init__(self, child, loop=loop)

        self.loop.add_callback(self.cb)

    def update(self, x, who=None):
        return self.queue.put(x)

    @gen.coroutine
    def cb(self):
        while True:
            x = yield self.queue.get()
            yield self.emit(x)


@Stream.register_api
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

    def __init__(self, *children, **kwargs):
        self.maxsize = kwargs.pop('maxsize', 10)
        self.buffers = [deque() for _ in children]
        self.condition = Condition()
        self.literals = [(i, val) for i, val in enumerate(children)
                         if not isinstance(val, Stream)]
        self.pack_literals()

        self.buffers_by_stream = {child: buffer
                    for child, buffer in builtins.zip(children, self.buffers)
                    if isinstance(child, Stream)}

        children2 = [child for child in children if isinstance(child, Stream)]

        Stream.__init__(self, children=children2)

    def pack_literals(self):
        """ Fill buffers for literals whenver we empty them """
        for i, val in self.literals:
            self.buffers[i].append(val)

    def update(self, x, who=None):
        L = self.buffers_by_stream[who]  # get buffer for stream
        L.append(x)
        if len(L) == 1 and all(self.buffers):
            tup = tuple(buf.popleft() for buf in self.buffers)
            self.condition.notify_all()
            if self.literals:
                self.pack_literals()
            return self.emit(tup)
        elif len(L) > self.maxsize:
            return self.condition.wait()


@Stream.register_api
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

    def __init__(self, *children, **kwargs):
        emit_on = kwargs.pop('emit_on', None)
        self.last = [None for _ in children]
        self.missing = set(children)
        if emit_on is not None:
            if not isinstance(emit_on, Iterable):
                emit_on = (emit_on, )
            emit_on = tuple(
                children[x] if isinstance(x, int) else x for x in emit_on)
            self.emit_on = emit_on
        else:
            self.emit_on = children
        Stream.__init__(self, children=children)

    def update(self, x, who=None):
        if self.missing and who in self.missing:
            self.missing.remove(who)

        self.last[self.children.index(who)] = x
        if not self.missing and who in self.emit_on:
            tup = tuple(self.last)
            return self.emit(tup)


@Stream.register_api
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
    def update(self, x, who=None):
        L = []
        for item in x:
            y = self.emit(item)
            if type(y) is list:
                L.extend(y)
            else:
                L.append(y)
        return L


@Stream.register_api
class unique(Stream):
    """ Avoid sending through repeated elements

    This deduplicates a stream so that only new elements pass through.
    You can control how much of a history is stored with the ``history=``
    parameter.  For example setting ``history=1`` avoids sending through
    elements when one is repeated right after the other.

    Examples
    --------
    >>> source = Stream()
    >>> source.unique(history=1).sink(print)
    >>> for x in [1, 1, 2, 2, 2, 1, 3]:
    ...     source.emit(x)
    1
    2
    1
    3
    """
    def __init__(self, child, history=None, key=identity):
        self.seen = dict()
        self.key = key
        if history:
            from zict import LRU
            self.seen = LRU(history, self.seen)

        Stream.__init__(self, child)

    def update(self, x, who=None):
        y = self.key(x)
        if y not in self.seen:
            self.seen[y] = 1
            return self.emit(x)


@Stream.register_api
class union(Stream):
    """ Combine multiple streams into one

    Every element from any of the children streams will immediately flow
    into the output stream.  They will not be combined with elements from
    other streams.

    See also
    --------
    Stream.zip
    Stream.combine_latest
    """
    def __init__(self, *children, **kwargs):
        super(union, self).__init__(children=children, **kwargs)

    def update(self, x, who=None):
        return self.emit(x)


@Stream.register_api
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
    def __init__(self, child, pick):
        self.pick = pick
        super(pluck, self).__init__(child)

    def update(self, x, who=None):
        if isinstance(self.pick, list):
            return self.emit(tuple([x[ind] for ind in self.pick]))
        else:
            return self.emit(x[self.pick])


@Stream.register_api
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
    def __init__(self, child, cache=None):
        if cache is None:
            cache = deque()
        self.cache = cache

        Stream.__init__(self, child)

    def update(self, x, who=None):
        self.cache.append(x)

    def flush(self, _=None):
        out = tuple(self.cache)
        self.emit(out)
        self.cache.clear()


@Stream.register_api
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
    def __init__(self, lossless, *children):
        children = (lossless,) + children
        self.last = [None for _ in children]
        self.missing = set(children)
        self.lossless = lossless
        self.lossless_buffer = deque()
        Stream.__init__(self, children=children)

    def update(self, x, who=None):
        idx = self.children.index(who)
        if who is self.lossless:
            self.lossless_buffer.append(x)

        self.last[idx] = x
        if self.missing and who in self.missing:
            self.missing.remove(who)

        if not self.missing:
            L = []
            while self.lossless_buffer:
                self.last[0] = self.lossless_buffer.popleft()
                L.append(self.emit(tuple(self.last)))
            return L


@Stream.register_api
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

    def __init__(self, child, loop=None):
        self.condition = Condition()
        self.next = []

        Stream.__init__(self, child, loop=loop)

        self.loop.add_callback(self.cb)

    def update(self, x, who=None):
        self.next = [x]
        self.loop.add_callback(self.condition.notify)

    @gen.coroutine
    def cb(self):
        while True:
            yield self.condition.wait()
            [x] = self.next
            yield self.emit(x)
