from distributed.client import default_client
from tornado import gen

from .core import Stream

class scatter(Stream):
    def __init__(self, child, client=None):
        self.client = client or default_client()

        Stream.__init__(self, child)

    @gen.coroutine
    def update(self, x):
        future = yield self.client._scatter(x)
        raise gen.Return(self.emit(future))


class gather(Stream):
    def __init__(self, child, client=None):
        self.client = client or default_client()

        Stream.__init__(self, child)

    @gen.coroutine
    def update(self, x):
        result = yield self.client._gather(x)
        raise gen.Return(self.emit(result))
