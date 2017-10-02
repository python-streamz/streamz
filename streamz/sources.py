from .core import Stream, Sink
import tornado.ioloop
from tornado import gen


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


class Source(Stream):
    _graphviz_shape = 'doubleoctagon'


class TextFile(Source):
    """ Stream data from a text file

    Parameters
    ----------
    f: file or string
    poll_interval: Number
        Interval to poll file for new data in seconds

    Example
    -------
    >>> source = Stream.from_textfile('myfile.json')  # doctest: +SKIP
    >>> js.map(json.loads).pluck('value').sum().sink(print)  # doctest: +SKIP

    >>> source.start()  # doctest: +SKIP

    Returns
    -------
    Stream
    """
    def __init__(self, f, poll_interval=0.100):
        if isinstance(f, str):
            f = open(f)
        self.file = f

        self.poll_interval = poll_interval
        super(TextFile, self).__init__()

    @gen.coroutine
    def start(self):
        while True:
            line = self.file.readline()
            if line:
                last = self.emit(line)  # TODO: we should yield on emit
            else:
                if self.poll_interval:
                    yield gen.sleep(self.poll_interval)
                    yield last
                else:
                    return
