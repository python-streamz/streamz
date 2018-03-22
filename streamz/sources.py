from glob import glob
import os

import tornado.ioloop
from tornado import gen

from .core import Stream


def PeriodicCallback(callback, callback_time, asynchronous=False, **kwargs):
    source = Stream(asynchronous=asynchronous)

    def _():
        result = callback()
        source._emit(result)

    pc = tornado.ioloop.PeriodicCallback(_, callback_time, **kwargs)
    pc.start()
    return source


def sink_to_file(filename, upstream, mode='w', prefix='', suffix='\n', flush=False):
    file = open(filename, mode=mode)

    def write(text):
        file.write(prefix + text + suffix)
        if flush:
            file.flush()

    upstream.sink(write)
    return file


class Source(Stream):
    _graphviz_shape = 'doubleoctagon'


@Stream.register_api(staticmethod)
class from_textfile(Source):
    """ Stream data from a text file

    Parameters
    ----------
    f: file or string
    poll_interval: Number
        Interval to poll file for new data in seconds
    delimiter: str ("\n")
        Character(s) to use to split the data into parts
    start: bool (False)
        Whether to start running immediately; otherwise call stream.start()
        explicitly.

    Example
    -------
    >>> source = Stream.from_textfile('myfile.json')  # doctest: +SKIP
    >>> js.map(json.loads).pluck('value').sum().sink(print)  # doctest: +SKIP

    >>> source.start()  # doctest: +SKIP

    Returns
    -------
    Stream
    """
    def __init__(self, f, poll_interval=0.100, delimiter='\n', start=False,
                 **kwargs):
        if isinstance(f, str):
            f = open(f)
        self.file = f
        self.delimiter = delimiter

        self.poll_interval = poll_interval
        super(from_textfile, self).__init__(ensure_io_loop=True, **kwargs)
        self.stopped = True
        if start:
            self.start()

    def start(self):
        self.stopped = False
        self.loop.add_callback(self.do_poll)

    @gen.coroutine
    def do_poll(self):
        buffer = ''
        while True:
            line = self.file.read()
            if line:
                buffer = buffer + line
                if self.delimiter in buffer:
                    parts = buffer.split(self.delimiter)
                    buffer = parts.pop(-1)
                    for part in parts:
                        yield self._emit(part + self.delimiter)
            else:
                yield gen.sleep(self.poll_interval)
            if self.stopped:
                break


@Stream.register_api(staticmethod)
class filenames(Source):
    """ Stream over filenames in a directory

    Parameters
    ----------
    path: string
        Directory path or globstring over which to search for files
    poll_interval: Number
        Seconds between checking path
    start: bool (False)
        Whether to start running immediately; otherwise call stream.start()
        explicitly.

    Examples
    --------
    >>> source = Stream.filenames('path/to/dir')  # doctest: +SKIP
    >>> source = Stream.filenames('path/to/*.csv', poll_interval=0.500)  # doctest: +SKIP
    """
    def __init__(self, path, poll_interval=0.100, start=False, **kwargs):
        if '*' not in path:
            if os.path.isdir(path):
                if not path.endswith(os.path.sep):
                    path = path + '/'
                path = path + '*'
        self.path = path
        self.seen = set()
        self.poll_interval = poll_interval
        self.stopped = True
        super(filenames, self).__init__(ensure_io_loop=True)
        if start:
            self.start()

    def start(self):
        self.stopped = False
        self.loop.add_callback(self.do_poll)

    @gen.coroutine
    def do_poll(self):
        while True:
            filenames = set(glob(self.path))
            new = filenames - self.seen
            for fn in sorted(new):
                self.seen.add(fn)
                yield self._emit(fn)
            yield gen.sleep(self.poll_interval)  # TODO: remove poll if delayed
            if self.stopped:
                break


@Stream.register_api(staticmethod)
class from_kafka(Source):
    """ Accepts messages from Kafka

    Uses the confluent-kafka library,
    https://docs.confluent.io/current/clients/confluent-kafka-python/


    Parameters
    ----------
    topics: list of str
        Labels of Kafka topics to consume from
    consumer_params: dict
        Settings to set up the stream, see
        https://docs.confluent.io/current/clients/confluent-kafka-python/#configuration
        Examples:
        url: Connection string (host:port) by which to reach Kafka
        group: Identity of the consumer. If multiple sources share the same
            group, each message will be passed to only one of them.
    poll_interval: number
        Seconds that elapse between polling Kafka for new messages

    Example
    -------

    >>> source = Stream.from_kafka(['mytopic'],
    ...        dict(url='localhost:9092', group='streamz'))  # doctest: +SKIP
    """
    def __init__(self, topics, consumer_params, poll_interval=0.1):
        import confluent_kafka as ck
        self.cpars = consumer_params
        self.consumer = ck.Consumer(consumer_params)
        self.consumer.subscribe(topics)
        self.topics = topics
        self.sleep = poll_interval

        super(from_kafka, self).__init__(ensure_io_loop=True)
        self.loop.add_callback(self.poll_kafka)

    @gen.coroutine
    def poll_kafka(self):
        while True:
            msg = self.consumer.poll(0)
            if msg is None or msg.error():
                yield gen.sleep(self.sleep)
            else:
                yield self.emit(msg.value())
