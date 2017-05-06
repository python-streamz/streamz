from dask.delayed import Delayed
from distributed.client import default_client
from tornado.locks import Condition
from tornado.queues import Queue
from tornado import gen

from . import core
from .core import Stream


class DaskStream(Stream):
    """ A Stream of Dask futures

    This follows a subset of the Stream API but operates on a cluster

    Examples
    --------
    >>> from dask.distributed import Client
    >>> client = Client()
    >>> source = Stream()
    >>> s = source.to_dask().map(func).gather()
    """
    def map(self, func, **kwargs):
        """ Apply a function on every element of the stream with Dask

        This uses ``Client.submit`` to call a function on the element
        asynchronously on a Dask cluster.  It returns a stream of futures.
        """
        return map(func, self, **kwargs)

    def compute(self, func, **kwargs):
        """ Computes a dask delayed on every element of the stream

        This uses ``Client.compute`` to trigger execution of a delayed function
        on the element asynchronously on a Dask cluster.
        It returns a stream of futures.
        """
        return compute(func, self, **kwargs)

    def scan(self, func, start=core.no_default):
        """ Accumulate results with previous state

        This uses ``Client.submit`` to call the function using a Dask client.
        It returns a stream of futures.
        """
        return scan(func, self, start=start)

    accumulate = scan

    def scatter(self, limit=10, client=None):
        """ Scatter data out to a cluster

        This returns a stream of futures
        """
        return scatter(self, limit=limit, client=client)

    def gather(self, limit=10, client=None):
        """ Gather a stream of futures back to a stream of local results """
        return gather(self, limit=limit, client=client)

    def update(self, x, who=None):
        return self.emit(x)

    def zip(self, other):
        return zip(self, other)


class scatter(DaskStream):

    def __init__(self, child, limit=10, client=None):
        self.client = client or default_client()
        self.queue = Queue(maxsize=limit)
        self.condition = Condition()

        Stream.__init__(self, child)

        self.client.loop.add_callback(self.cb)

    def update(self, x, who=None):
        return self.queue.put(x)

    @gen.coroutine
    def cb(self):
        while True:
            x = yield self.queue.get()
            L = [x]
            while not self.queue.empty():
                L.append(self.queue.get_nowait())
            futures = yield self.client._scatter(L)
            for f in futures:
                yield self.emit(f)
            if self.queue.empty():
                self.condition.notify_all()

    @gen.coroutine
    def flush(self):
        while not self.queue.empty():
            yield self.condition.wait()


class gather(Stream):

    def __init__(self, child, limit=10, client=None):
        self.client = client or default_client()
        self.queue = Queue(maxsize=limit)
        self.condition = Condition()

        Stream.__init__(self, child)

        self.client.loop.add_callback(self.cb)

    def update(self, x, who=None):
        return self.queue.put(x)

    @gen.coroutine
    def cb(self):
        while True:
            x = yield self.queue.get()
            L = [x]
            while not self.queue.empty():
                L.append(self.queue.get_nowait())
            results = yield self.client._gather(L)
            for x in results:
                yield self.emit(x)
            if self.queue.empty():
                self.condition.notify_all()

    @gen.coroutine
    def flush(self):
        while not self.queue.empty():
            yield self.condition.wait()


class map(DaskStream):

    def __init__(self, func, child, client=None, **kwargs):
        self.client = client or default_client()
        self.func = func
        self.kwargs = kwargs

        Stream.__init__(self, child)

    def update(self, x, who=None):
        return self.emit(self.client.submit(self.func, x, **self.kwargs))


class compute(DaskStream):

    def __init__(self, func, child, client=None, **kwargs):
        self.client = client or default_client()
        self.func = func
        self.kwargs = kwargs

        Stream.__init__(self, child)

    def update(self, x, who=None):
        self.emit(self.client.compute(self.func(x), **self.kwargs))


class scan(DaskStream):

    def __init__(self, func, child, start=core.no_default, client=None):
        self.client = client or default_client()
        self.func = func
        self.state = start

        Stream.__init__(self, child)

    def update(self, x, who=None):
        if self.state is core.no_default:
            self.state = x
        else:
            self.state = self.client.submit(self.func, self.state, x)
            return self.emit(self.state)


class zip(core.zip, DaskStream):
    pass
