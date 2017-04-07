from .core import Stream, Sink
from tornado import gen
from tornado.ioloop import IOLoop


def inc(x):
    return x + 1


class Counter(Stream):
    def __init__(self, interval, step=inc, loop=None):
        self.loop = loop or IOLoop.current()
        self.interval = interval
        self.x = 0
        self.step = step

        Stream.__init__(self)
        self.loop.add_callback(self.cb)

    @gen.coroutine
    def cb(self):
        while True:
            self.emit(self.x)
            yield gen.sleep(self.interval)
            self.x = self.step(self.x)


def sink_to_file(filename, child, mode='w', prefix='', suffix='\n', flush=False):
    file = open(filename, mode=mode)

    def write(text):
        file.write(prefix + text + suffix)
        if flush:
            file.flush()

    sink = Sink(write, child)
    return file


def sink_to_list(x):
    L = []
    Sink(L.append, x)
    return L
