from .core import Stream, Sink
from tornado import gen
import tornado.ioloop


def PeriodicCallback(callback, callback_time, **kwargs):
    source = Stream()
    def _():
        result = callback()
        source.emit(result)
    pc = tornado.ioloop.PeriodicCallback(_, callback_time, **kwargs)
    pc.start()
    return source


def sink_to_file(filename, child, mode='w', prefix='', suffix='\n', flush=False):
    file = open(filename, mode=mode)

    def write(text):
        file.write(prefix + text + suffix)
        if flush:
            file.flush()

    Sink(write, child)
    return file
