from __future__ import absolute_import, division, print_function

from operator import getitem

from tornado import gen

from distributed.client import default_client

from .core import Stream
from . import core


class DaskStream(Stream):
    def __init__(self, *args, **kwargs):
        if 'loop' not in kwargs:
            kwargs['loop'] = default_client().loop
        super(DaskStream, self).__init__(*args, **kwargs)


@DaskStream.register_api()
class map(DaskStream):
    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.args = args

        DaskStream.__init__(self, upstream)

    def update(self, x, who=None):
        client = default_client()
        result = client.submit(self.func, x, *self.args, **self.kwargs)
        return self._emit(result)


@DaskStream.register_api()
class accumulate(DaskStream):
    def __init__(self, upstream, func, start=core.no_default,
                 returns_state=False, **kwargs):
        self.func = func
        self.state = start
        self.returns_state = returns_state
        self.kwargs = kwargs
        DaskStream.__init__(self, upstream)

    def update(self, x, who=None):
        if self.state is core.no_default:
            self.state = x
            return self._emit(self.state)
        else:
            client = default_client()
            result = client.submit(self.func, self.state, x, **self.kwargs)
            if self.returns_state:
                state = client.submit(getitem, result, 0)
                result = client.submit(getitem, result, 1)
            else:
                state = result
            self.state = state
            return self._emit(result)


@core.Stream.register_api()
@DaskStream.register_api()
class scatter(DaskStream):
    """ Convert local stream to Dask Stream

    All elements flowing through the input will be scattered out to the cluster
    """
    @gen.coroutine
    def update(self, x, who=None):
        client = default_client()
        future = yield client.scatter(x, asynchronous=True)
        f = yield self._emit(future)
        raise gen.Return(f)


@DaskStream.register_api()
class gather(core.Stream):
    """ Convert Dask stream to local Stream """
    @gen.coroutine
    def update(self, x, who=None):
        client = default_client()
        result = yield client.gather(x, asynchronous=True)
        result2 = yield self._emit(result)
        raise gen.Return(result2)


@DaskStream.register_api()
class buffer(DaskStream, core.buffer):
    pass


@DaskStream.register_api()
class combine_latest(DaskStream, core.combine_latest):
    pass


@DaskStream.register_api()
class delay(DaskStream, core.delay):
    pass


@DaskStream.register_api()
class latest(DaskStream, core.latest):
    pass


@DaskStream.register_api()
class partition(DaskStream, core.partition):
    pass


@DaskStream.register_api()
class rate_limit(DaskStream, core.rate_limit):
    pass


@DaskStream.register_api()
class sliding_window(DaskStream, core.sliding_window):
    pass


@DaskStream.register_api()
class timed_window(DaskStream, core.timed_window):
    pass


@DaskStream.register_api()
class union(DaskStream, core.union):
    pass


@DaskStream.register_api()
class zip(DaskStream, core.zip):
    pass
