from glob import glob
import os

import time
import tornado.ioloop
from tornado import gen

from .core import Stream, convert_interval, RefCounter


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

    def __init__(self, **kwargs):
        self.stopped = True
        super(Source, self).__init__(**kwargs)

    def stop(self):  # pragma: no cover
        # fallback stop method - for poll functions with while not self.stopped
        if not self.stopped:
            self.stopped = True


@Stream.register_api(staticmethod)
class from_textfile(Source):
    """ Stream data from a text file

    Parameters
    ----------
    f: file or string
        Source of the data. If string, will be opened.
    poll_interval: Number
        Interval to poll file for new data in seconds
    delimiter: str
        Character(s) to use to split the data into parts
    start: bool
        Whether to start running immediately; otherwise call stream.start()
        explicitly.
    from_end: bool
        Whether to begin streaming from the end of the file (i.e., only emit
        lines appended after the stream starts).

    Examples
    --------
    >>> source = Stream.from_textfile('myfile.json')  # doctest: +SKIP
    >>> source.map(json.loads).pluck('value').sum().sink(print)  # doctest: +SKIP
    >>> source.start()  # doctest: +SKIP

    Returns
    -------
    Stream
    """
    def __init__(self, f, poll_interval=0.100, delimiter='\n', start=False,
                 from_end=False, **kwargs):
        if isinstance(f, str):
            f = open(f)
        self.file = f
        self.from_end = from_end
        self.delimiter = delimiter

        self.poll_interval = poll_interval
        super(from_textfile, self).__init__(ensure_io_loop=True, **kwargs)
        self.stopped = True
        self.started = False
        if start:
            self.start()

    def start(self):
        self.stopped = False
        self.started = False
        self.loop.add_callback(self.do_poll)

    @gen.coroutine
    def do_poll(self):
        buffer = ''
        if self.from_end:
            # this only happens when we are ready to read
            self.file.seek(0, 2)
        while not self.stopped:
            self.started = True
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
class from_tcp(Source):
    """
    Creates events by reading from a socket using tornado TCPServer

    The stream of incoming bytes is split on a given delimiter, and the parts
    become the emitted events.

    Parameters
    ----------
    port : int
        The port to open and listen on. It only gets opened when the source
        is started, and closed upon ``stop()``
    delimiter : bytes
        The incoming data will be split on this value. The resulting events
        will still have the delimiter at the end.
    start : bool
        Whether to immediately initiate the source. You probably want to
        set up downstream nodes first.
    server_kwargs : dict or None
        If given, additional arguments to pass to TCPServer

    Examples
    --------

    >>> source = Source.from_tcp(4567)  # doctest: +SKIP
    """
    def __init__(self, port, delimiter=b'\n', start=False,
                 server_kwargs=None):
        super(from_tcp, self).__init__(ensure_io_loop=True)
        self.stopped = True
        self.server_kwargs = server_kwargs or {}
        self.port = port
        self.server = None
        self.delimiter = delimiter
        if start:  # pragma: no cover
            self.start()

    @gen.coroutine
    def _start_server(self):
        from tornado.tcpserver import TCPServer
        from tornado.iostream import StreamClosedError

        class EmitServer(TCPServer):
            source = self

            @gen.coroutine
            def handle_stream(self, stream, address):
                while True:
                    try:
                        data = yield stream.read_until(self.source.delimiter)
                        yield self.source._emit(data)
                    except StreamClosedError:
                        break

        self.server = EmitServer(**self.server_kwargs)
        self.server.listen(self.port)

    def start(self):
        if self.stopped:
            self.loop.add_callback(self._start_server)
            self.stopped = False

    def stop(self):
        if not self.stopped:
            self.server.stop()
            self.server = None
            self.stopped = True


@Stream.register_api(staticmethod)
class from_http_server(Source):
    """Listen for HTTP POSTs on given port

    Each connection will emit one event, containing the body data of
    the request

    Parameters
    ----------
    port : int
        The port to listen on
    path : str
        Specific path to listen on. Can be regex, but content is not used.
    start : bool
        Whether to immediately startup the server. Usually you want to connect
        downstream nodes first, and then call ``.start()``.
    server_kwargs : dict or None
        If given, set of further parameters to pass on to HTTPServer

    Examples
    --------

    >>> source = Source.from_http_server(4567)  # doctest: +SKIP

    """

    def __init__(self, port, path='/.*', start=False, server_kwargs=None):
        self.port = port
        self.path = path
        self.server_kwargs = server_kwargs or {}
        super(from_http_server, self).__init__(ensure_io_loop=True)
        self.stopped = True
        self.server = None
        if start:  # pragma: no cover
            self.start()

    def _start_server(self):
        from tornado.web import Application, RequestHandler
        from tornado.httpserver import HTTPServer

        class Handler(RequestHandler):
            source = self

            @gen.coroutine
            def post(self):
                yield self.source._emit(self.request.body)
                self.write('OK')

        application = Application([
            (self.path, Handler),
        ])
        self.server = HTTPServer(application, **self.server_kwargs)
        self.server.listen(self.port)

    def start(self):
        """Start HTTP server and listen"""
        if self.stopped:
            self.loop.add_callback(self._start_server)
            self.stopped = False

    def stop(self):
        """Shutdown HTTP server"""
        if not self.stopped:
            self.server.stop()
            self.server = None
            self.stopped = True


@Stream.register_api(staticmethod)
class from_process(Source):
    """Messages from a running external process

    This doesn't work on Windows

    Parameters
    ----------
    cmd : list of str or str
        Command to run: program name, followed by arguments
    open_kwargs : dict
        To pass on the the process open function, see ``subprocess.Popen``.
    with_stderr : bool
        Whether to include the process STDERR in the stream
    start : bool
        Whether to immediately startup the process. Usually you want to connect
        downstream nodes first, and then call ``.start()``.

    Example
    -------
    >>> source = Source.from_process(['ping', 'localhost'])  # doctest: +SKIP
    """

    def __init__(self, cmd, open_kwargs=None, with_stderr=False, start=False):
        self.cmd = cmd
        self.open_kwargs = open_kwargs or {}
        self.with_stderr = with_stderr
        super(from_process, self).__init__(ensure_io_loop=True)
        self.stopped = True
        self.process = None
        if start:  # pragma: no cover
            self.start()

    @gen.coroutine
    def _start_process(self):
        # should be done in asyncio (py3 only)? Apparently can handle Windows
        # with appropriate config.
        from tornado.process import Subprocess
        from tornado.iostream import StreamClosedError
        import subprocess
        stderr = subprocess.STDOUT if self.with_stderr else subprocess.PIPE
        process = Subprocess(self.cmd, stdout=Subprocess.STREAM,
                             stderr=stderr, **self.open_kwargs)
        while not self.stopped:
            try:
                out = yield process.stdout.read_until(b'\n')
            except StreamClosedError:
                # process exited
                break
            yield self._emit(out)
        yield process.stdout.close()
        process.proc.terminate()

    def start(self):
        """Start external process"""
        if self.stopped:
            self.loop.add_callback(self._start_process)
            self.stopped = False

    def stop(self):
        """Shutdown external process"""
        if not self.stopped:
            self.stopped = True


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
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        Examples:
        bootstrap.servers, Connection string(s) (host:port) by which to reach
        Kafka;
        group.id, Identity of the consumer. If multiple sources share the same
        group, each message will be passed to only one of them.
    poll_interval: number
        Seconds that elapse between polling Kafka for new messages
    start: bool (False)
        Whether to start polling upon instantiation

    Examples
    --------

    >>> source = Stream.from_kafka(['mytopic'],
    ...           {'bootstrap.servers': 'localhost:9092',
    ...            'group.id': 'streamz'})  # doctest: +SKIP

    """
    def __init__(self, topics, consumer_params, poll_interval=0.1, start=False, **kwargs):
        self.cpars = consumer_params
        self.consumer = None
        self.topics = topics
        self.poll_interval = poll_interval
        super(from_kafka, self).__init__(ensure_io_loop=True, **kwargs)
        self.stopped = True
        if start:
            self.start()

    def do_poll(self):
        if self.consumer is not None:
            msg = self.consumer.poll(0)
            if msg and msg.value() and msg.error() is None:
                return msg.value()

    @gen.coroutine
    def poll_kafka(self):
        while True:
            val = self.do_poll()
            if val:
                yield self._emit(val)
            else:
                yield gen.sleep(self.poll_interval)
            if self.stopped:
                break
        self._close_consumer()

    def start(self):
        import confluent_kafka as ck
        if self.stopped:
            self.stopped = False
            self.consumer = ck.Consumer(self.cpars)
            self.consumer.subscribe(self.topics)
            tp = ck.TopicPartition(self.topics[0], 0, 0)

            # blocks for consumer thread to come up
            self.consumer.get_watermark_offsets(tp)
            self.loop.add_callback(self.poll_kafka)

    def _close_consumer(self):
        if self.consumer is not None:
            consumer = self.consumer
            self.consumer = None
            consumer.unsubscribe()
            consumer.close()
        self.stopped = True


class FromKafkaBatched(Stream):
    """Base class for both local and cluster-based batched kafka processing"""
    def __init__(self, topic, consumer_params, poll_interval='1s',
                 npartitions=1, keys=False, **kwargs):
        self.consumer_params = consumer_params
        self.topic = topic
        self.npartitions = npartitions
        self.positions = [0] * npartitions
        self.poll_interval = convert_interval(poll_interval)
        self.keys = keys
        self.stopped = True

        super(FromKafkaBatched, self).__init__(ensure_io_loop=True, **kwargs)

    @gen.coroutine
    def poll_kafka(self):
        import confluent_kafka as ck

        def commit(_part):
            topic, part_no, _, _, offset = _part[1:]
            _tp = ck.TopicPartition(topic, part_no, offset + 1)
            self.consumer.commit(offsets=[_tp], asynchronous=True)

        @gen.coroutine
        def checkpoint_emit(_part):
            ref = RefCounter(cb=lambda: commit(_part))
            yield self._emit(_part, metadata={'refs': ref})

        tps = []
        for partition in range(self.npartitions):
            tps.append(ck.TopicPartition(self.topic, partition))

        while True:
            try:
                committed = self.consumer.committed(tps, timeout=1)
            except ck.KafkaException:
                pass
            else:
                for tp in committed:
                    self.positions[tp.partition] = tp.offset
                break

        try:
            while not self.stopped:
                out = []
                for partition in range(self.npartitions):
                    tp = ck.TopicPartition(self.topic, partition, 0)
                    try:
                        low, high = self.consumer.get_watermark_offsets(
                            tp, timeout=0.1)
                    except (RuntimeError, ck.KafkaException):
                        continue
                    current_position = self.positions[partition]
                    lowest = max(current_position, low)
                    if high > lowest:
                        out.append((self.consumer_params, self.topic, partition,
                                    self.keys, lowest, high - 1))
                        self.positions[partition] = high

                for part in out:
                    self.loop.add_callback(checkpoint_emit, part)

                else:
                    yield gen.sleep(self.poll_interval)
        finally:
            self.consumer.unsubscribe()
            self.consumer.close()

    def start(self):
        import confluent_kafka as ck
        if self.stopped:
            self.consumer = ck.Consumer(self.consumer_params)
            self.stopped = False
            tp = ck.TopicPartition(self.topic, 0, 0)

            # blocks for consumer thread to come up
            self.consumer.get_watermark_offsets(tp)
            self.loop.add_callback(self.poll_kafka)


@Stream.register_api(staticmethod)
def from_kafka_batched(topic, consumer_params, poll_interval='1s',
                       npartitions=1, start=False, dask=False, keys=False, **kwargs):
    """ Get messages and keys (optional) from Kafka in batches

    Uses the confluent-kafka library,
    https://docs.confluent.io/current/clients/confluent-kafka-python/

    This source will emit lists of messages for each partition of a single given
    topic per time interval, if there is new data. If using dask, one future
    will be produced per partition per time-step, if there is data.

    Parameters
    ----------
    topic: str
        Kafka topic to consume from
    consumer_params: dict
        Settings to set up the stream, see
        https://docs.confluent.io/current/clients/confluent-kafka-python/#configuration
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        Examples:
        bootstrap.servers: Connection string(s) (host:port) by which to reach Kafka
        group.id: Identity of the consumer. If multiple sources share the same
            group, each message will be passed to only one of them.
    poll_interval: number
        Seconds that elapse between polling Kafka for new messages
    npartitions: int
        Number of partitions in the topic
    start: bool (False)
        Whether to start polling upon instantiation
    keys: bool (False)
        Whether to extract keys along with the messages. If True, this will yield each message as a dict:
        {'key':msg.key(), 'value':msg.value()}

    Examples
    --------

    >>> source = Stream.from_kafka_batched('mytopic',
    ...           {'bootstrap.servers': 'localhost:9092',
    ...            'group.id': 'streamz'}, npartitions=4)  # doctest: +SKIP

    """
    if dask:
        from distributed.client import default_client
        kwargs['loop'] = default_client().loop
    source = FromKafkaBatched(topic, consumer_params,
                              poll_interval=poll_interval,
                              npartitions=npartitions, keys=keys,
                              **kwargs)
    if dask:
        source = source.scatter()

    if start:
        source.start()

    return source.starmap(get_message_batch)


def get_message_batch(kafka_params, topic, partition, keys, low, high, timeout=None):
    """Fetch a batch of kafka messages (keys & values) in given topic/partition

    This will block until messages are available, or timeout is reached.
    """
    import confluent_kafka as ck
    t0 = time.time()
    consumer = ck.Consumer(kafka_params)
    tp = ck.TopicPartition(topic, partition, low)
    consumer.assign([tp])
    out = []
    try:
        while True:
            msg = consumer.poll(0)
            if msg and msg.value() and msg.error() is None:
                if high >= msg.offset():
                    if keys:
                        out.append({'key':msg.key(), 'value':msg.value()})
                    else:
                        out.append(msg.value())
                if high <= msg.offset():
                    break
            else:
                time.sleep(0.1)
                if timeout is not None and time.time() - t0 > timeout:
                    break
    finally:
        consumer.close()
    return out
