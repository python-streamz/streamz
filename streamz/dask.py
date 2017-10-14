from __future__ import absolute_import, division, print_function

from operator import getitem

from tornado import gen

import dask.distributed
from distributed.utils import set_thread_state
from distributed.client import default_client

from . import core


class DaskStream(core.Stream):

    @property
    def scan(self):
        return self.accumulate


@DaskStream.register_api()
class map(DaskStream):
    def __init__(self, child, func, args=(), **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.args = args

        DaskStream.__init__(self, child)

    def update(self, x, who=None):
        client = default_client()
        result = client.submit(self.func, x, *self.args, **self.kwargs)
        return self.emit(result)


@DaskStream.register_api()
class scan(DaskStream):
    def __init__(self, child, func, start=core.no_default, returns_state=False):
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


@DaskStream.register_api()
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


@DaskStream.register_api()
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


@DaskStream.register_api()
class zip(DaskStream, core.zip):
    pass


@DaskStream.register_api()
class buffer(DaskStream):
    def __init__(self, child, n, loop=None):
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
