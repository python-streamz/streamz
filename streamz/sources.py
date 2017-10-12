from glob import glob
import os

import tornado.ioloop
from tornado import gen

from .core import Stream


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

    child.sink(write)
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


@Stream.register_api
class filenames(Source):
    """ Stream over filenames in a directory

    Parameters
    ----------
    path: string
        Directory path or globstring over which to search for files
    poll_interval: Number
        Seconds between checking path

    Examples
    --------
    >>> source = Stream.filenames('path/to/dir')  # doctest: +SKIP
    >>> source = Stream.filenames('path/to/*.csv', poll_interval=0.500)  # doctest: +SKIP
    """
    def __init__(self, path, poll_interval=0.100):
        if '*' not in path:
            if os.path.isdir(path):
                if not path.endswith(os.path.sep):
                    path = path + '/'
                path = path + '*'
        self.path = path
        self.seen = set()
        self.poll_interval = poll_interval

        super(filenames, self).__init__()

    @gen.coroutine
    def start(self):
        while True:
            filenames = set(glob(self.path))
            new = filenames - self.seen
            for fn in sorted(new):
                self.seen.add(fn)
                yield self.emit(fn)
            yield gen.sleep(self.poll_interval)  # TODO: remove poll if delayed
