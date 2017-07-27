from __future__ import absolute_import, division, print_function

from operator import getitem

from tornado import gen

import dask.distributed
from distributed.utils import set_thread_state
from distributed.client import default_client

from . import core


class DaskStream(core.Stream):
    def map(self, func, *args, **kwargs):
        """ Apply a function to every element in the stream """
        return map(func, self, args=args, **kwargs)

    def gather(self):
        return gather(self)

    def accumulate(self, func, start=core.no_default, returns_state=False):
        """ Accumulate results with previous state """
        return scan(func, self, start=start, returns_state=returns_state)

    scan = accumulate

    def zip(self, *other):
        """ Combine two streams together into a stream of tuples """
        return zip(self, *other)

    def buffer(self, n, loop=None):
        """ Allow results to pile up at this point in the stream

        This allows results to buffer in place at various points in the stream.
        This can help to smooth flow through the system when backpressure is
        applied.
        """
        return buffer(n, self, loop=loop)


class map(DaskStream):
    def __init__(self, func, child, args=(), **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.args = args

        DaskStream.__init__(self, child)

    def update(self, x, who=None):
        client = default_client()
        result = client.submit(self.func, x, *self.args, **self.kwargs)
        return self.emit(result)


class scan(DaskStream):
    def __init__(self, func, child, start=core.no_default, returns_state=False):
        self.func = func
        self.state = start
        self.returns_state = returns_state
        DaskStream.__init__(self, child)

    def update(self, x, who=None):
        if self.state is core.no_default:
            self.state = x
            return self.emit(self.state)
        else:
            client = default_client()
            result = client.submit(self.func, self.state, x)
            if self.returns_state:
                state = client.submit(getitem, result, 0)
                result = client.submit(getitem, result, 1)
            else:
                state = result
            self.state = state
            return self.emit(result)


class scatter(DaskStream):
    """ Convert local stream to Dask Stream

    All elements flowing through the input will be scattered out to the cluster
    """
    @gen.coroutine
    def _update(self, x, who=None):
        client = default_client()
        future = yield client.scatter(x, asynchronous=True)
        yield self.emit(future)

    def update(self, x, who=None):
        client = default_client()
        return client.sync(self._update, x, who)


class gather(core.Stream):
    """ Convert Dask stream to local Stream """
    @gen.coroutine
    def _update(self, x, who=None):
        client = default_client()
        result = yield client.gather(x, asynchronous=True)
        with set_thread_state(asynchronous=True):
            result = self.emit(result)
        yield result

    def update(self, x, who=None):
        client = default_client()
        return client.sync(self._update, x, who)


class zip(DaskStream, core.zip):
    pass


class buffer(DaskStream):
    def __init__(self, n, child, loop=None):
        client = default_client()
        self.queue = dask.distributed.Queue(maxsize=n, client=client)

        core.Stream.__init__(self, child, loop=loop or client.loop)

        self.loop.add_callback(self.cb)

    @gen.coroutine
    def cb(self):
        while True:
            x = yield self.queue.get(asynchronous=True)
            with set_thread_state(asynchronous=True):
                result = self.emit(x)
            yield result

    @gen.coroutine
    def _update(self, x, who=None):
        result = yield self.queue.put(x, asynchronous=True)
        raise gen.Return(result)

    def update(self, x, who=None):
        return self.queue.put(x)
