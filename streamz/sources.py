from glob import glob
import os

import tornado.ioloop
from tornado.ioloop import IOLoop
from tornado import gen

from .core import Stream


def PeriodicCallback(callback, callback_time, **kwargs):
    source = Stream()

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

    Example
    -------
    >>> source = Stream.from_textfile('myfile.json')  # doctest: +SKIP
    >>> js.map(json.loads).pluck('value').sum().sink(print)  # doctest: +SKIP

    >>> source.start()  # doctest: +SKIP

    Returns
    -------
    Stream
    """
    def __init__(self, f, poll_interval=0.100, **kwargs):
        if isinstance(f, str):
            f = open(f)
        self.file = f

        self.poll_interval = poll_interval
        super(from_textfile, self).__init__(**kwargs)

    @gen.coroutine
    def start(self):
        while True:
            line = self.file.readline()
            if line:
                yield self._emit(line)
            else:
                if self.poll_interval:
                    yield gen.sleep(self.poll_interval)
                else:
                    return


@Stream.register_api(staticmethod)
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
    def __init__(self, path, poll_interval=0.100, **kwargs):
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
                yield self._emit(fn)
            yield gen.sleep(self.poll_interval)  # TODO: remove poll if delayed


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
        IOLoop.current().add_callback(self.poll_kafka)
        self.cpars = consumer_params
        self.consumer = ck.Consumer(consumer_params)
        self.consumer.subscribe(topics)
        self.topics = topics
        self.sleep = poll_interval

        super(from_kafka, self).__init__()

    @gen.coroutine
    def poll_kafka(self):
        while True:
            msg = self.consumer.poll(0)
            if msg is None or msg.error():
                yield gen.sleep(self.sleep)
            else:
                yield self.emit(msg.value())
