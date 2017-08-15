from __future__ import absolute_import, division, print_function

from collections import deque
from time import time

import toolz
from tornado import gen
from tornado.locks import Condition
from tornado.ioloop import IOLoop
from tornado.queues import Queue
from collections import Iterable

no_default = '--no-default--'


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
    str_list = ['func', 'predicate', 'n', 'interval']

    def __init__(self, child=None, children=None, name=None, **kwargs):
        self.parents = []
        if children is not None:
            self.children = children
        else:
            self.children = [child]
        if kwargs.get('loop'):
            self._loop = kwargs.get('loop')
        for child in self.children:
            if child:
                child.parents.append(self)
        self.name = name

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
        s = "; ".join(s_list)
        s = "<" + s + ">"
        return s

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

    def map(self, func, *args, **kwargs):
        """ Apply a function to every element in the stream """
        return map(func, self, args=args, **kwargs)

    def filter(self, predicate):
        """ Only pass through elements that satisfy the predicate """
        return filter(predicate, self)

    def remove(self, predicate):
        """ Only pass through elements for which the predicate returns False
        """
        return filter(lambda x: not predicate(x), self)

    def accumulate(self, func, start=no_default, returns_state=False):
        """ Accumulate results with previous state

        This preforms running or cumulative reductions, applying the function
        to the previous total and the new element.  The function should take
        two arguments, the previous accumulated state and the next element and
        it should return a new accumulated state.

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
        return scan(func, self, start=start, returns_state=returns_state)

    scan = accumulate

    def partition(self, n):
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
        return partition(n, self)

    def sliding_window(self, n):
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
        return sliding_window(n, self)

    def rate_limit(self, interval):
        """ Limit the flow of data

        This stops two elements of streaming through in an interval shorter
        than the provided value.

        Parameters
        ----------
        interval: float
            Time in seconds
        """
        return rate_limit(interval, self)

    def buffer(self, n, loop=None):
        """ Allow results to pile up at this point in the stream

        This allows results to buffer in place at various points in the stream.
        This can help to smooth flow through the system when backpressure is
        applied.
        """
        return buffer(n, self, loop=loop)

    def timed_window(self, interval, loop=None):
        """ Emit a tuple of collected results every interval

        Every ``interval`` seconds this emits a tuple of all of the results
        seen so far.  This can help to batch data coming off of a high-volume
        stream.
        """
        return timed_window(interval, self, loop=loop)

    def delay(self, interval, loop=None):
        """ Add a time delay to results """
        return delay(interval, self, loop=None)

    def combine_latest(self, *others, **kwargs):
        """ Combine multiple streams together to a stream of tuples

        This will emit a new tuple of all of the most recent elements seen from
        any stream.

        Parameters
        ---------------
        emit_on : stream or list of streams or None
            only emit upon update of the streams listed.
            If None, emit on update from any stream

        """
        return combine_latest(self, *others, **kwargs)

    def concat(self):
        """ Flatten streams of lists or iterables into a stream of elements

        Examples
        --------
        >>> source = Stream()
        >>> source.concat().sink(print)
        >>> for x in [[1, 2, 3], [4, 5], [6, 7, 7]]:
        ...     source.emit(x)
        1
        2
        3
        4
        5
        6
        7
        """
        return concat(self)

    flatten = concat

    def union(self, *others):
        """ Combine multiple streams into one

        Every element from any of the children streams will immediately flow
        into the output stream.  They will not be combined with elements from
        other streams.

        See also
        --------
        Stream.zip
        Stream.combine_latest
        """
        return union(children=(self,) + others)

    def unique(self, history=None, key=identity):
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
        return unique(self, history=history, key=key)

    def collect(self, cache=None):
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
        return collect(self, cache=cache)

    def zip(self, *other):
        """ Combine two streams together into a stream of tuples """
        return zip(self, *other)

    def sink(self, func):
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
        Stream.sink_to_list
        """
        return Sink(func, self)

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
        Sink(L.append, self)
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


class Sink(Stream):
    def __init__(self, func, child):
        self.func = func

        Stream.__init__(self, child)

    def update(self, x, who=None):
        result = self.func(x)
        if type(result) is gen.Future:
            return result
        else:
            return []


class map(Stream):
    def __init__(self, func, child, args=(), **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, child)

    def update(self, x, who=None):
        result = self.func(x, *self.args, **self.kwargs)

        return self.emit(result)


class filter(Stream):
    def __init__(self, predicate, child):
        self.predicate = predicate

        Stream.__init__(self, child)

    def update(self, x, who=None):
        if self.predicate(x):
            return self.emit(x)


class scan(Stream):
    def __init__(self, func, child, start=no_default, returns_state=False):
        self.func = func
        self.state = start
        self.returns_state = returns_state
        Stream.__init__(self, child)

    def update(self, x, who=None):
        if self.state is no_default:
            self.state = x
            return self.emit(x)
        else:
            result = self.func(self.state, x)
            if self.returns_state:
                state, result = result
            else:
                state = result
            self.state = state
            return self.emit(result)


class partition(Stream):
    def __init__(self, n, child):
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


class sliding_window(Stream):
    def __init__(self, n, child):
        self.n = n
        self.buffer = deque(maxlen=n)
        Stream.__init__(self, child)

    def update(self, x, who=None):
        self.buffer.append(x)
        if len(self.buffer) == self.n:
            return self.emit(tuple(self.buffer))
        else:
            return []


class timed_window(Stream):
    def __init__(self, interval, child, loop=None):
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


class delay(Stream):
    def __init__(self, interval, child, loop=None):
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


class rate_limit(Stream):
    def __init__(self, interval, child):
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


class buffer(Stream):
    def __init__(self, n, child, loop=None):
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


class zip(Stream):
    def __init__(self, *children, **kwargs):
        self.maxsize = kwargs.pop('maxsize', 10)
        self.buffers = [deque() for _ in children]
        self.condition = Condition()
        Stream.__init__(self, children=children)

    def update(self, x, who=None):
        L = self.buffers[self.children.index(who)]
        L.append(x)
        if len(L) == 1 and all(self.buffers):
            tup = tuple(buf.popleft() for buf in self.buffers)
            self.condition.notify_all()
            return self.emit(tup)
        elif len(L) > self.maxsize:
            return self.condition.wait()


class combine_latest(Stream):
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


class concat(Stream):
    def update(self, x, who=None):
        L = []
        for item in x:
            y = self.emit(item)
            if type(y) is list:
                L.extend(y)
            else:
                L.append(y)
        return L


class unique(Stream):
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


class union(Stream):
    def update(self, x, who=None):
        return self.emit(x)


class collect(Stream):
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
