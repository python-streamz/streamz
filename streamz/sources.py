from glob import glob
import os
import weakref

import time
import tornado.ioloop
from tornado import gen

from .core import Stream, convert_interval


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
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        Examples:
        bootstrap.servers: Connection string(s) (host:port) by which to reach Kafka
        group.id: Identity of the consumer. If multiple sources share the same
            group, each message will be passed to only one of them.
    poll_interval: number
        Seconds that elapse between polling Kafka for new messages
    start: bool (False)
        Whether to start polling upon instantiation

    Example
    -------

    >>> source = Stream.from_kafka(['mytopic'],
    ...           {'bootstrap.servers': 'localhost:9092',
    ...            'group.id': 'streamz'})  # doctest: +SKIP
    """
    def __init__(self, topics, consumer_params, poll_interval=0.1, start=False,
                 **kwargs):
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
            if msg and msg.value():
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

    def start(self):
        import confluent_kafka as ck
        import distributed
        if self.stopped:
            finalize = distributed.compatibility.finalize
            self.stopped = False
            self.loop.add_callback(self.poll_kafka)
            self.consumer = ck.Consumer(self.cpars)
            self.consumer.subscribe(self.topics)

            def close(ref):
                ob = ref()
                if ob is not None and ob.consumer is not None:
                    consumer = ob.consumer
                    ob.consumer = None
                    consumer.unsubscribe()
                    consumer.close()  # may raise with latest ck, that's OK

            finalize(self, close, weakref.ref(self))

    def _close_consumer(self):
        if self.consumer is not None:
            consumer = self.consumer
            self.consumer = None
            consumer.unsubscribe()
            consumer.close()
        self.stopped = True


class ConsumerFactory:
    def __init__(self, consumer_params, topic):
        self.consumer_params = self._fix_params(consumer_params)
        self.topic = topic
        self.consumers = {}
        self.ck = None

    def _fix_params(self, consumer_params):
        if 'group.id' not in consumer_params:
            import warnings
            import uuid
            group_id = "consumer-{}".format(uuid.uuid4())
            warnings.warn("Consumer group id is not specified, Job restarts consume messages from offset 0.\n" +
                          "Use group.id: {} for restarting this job from a check point in future.".format(group_id))
            consumer_params['group.id'] = group_id

        consumer_params['enable.auto.commit'] = 'false'
        # consumer_params['max_poll_interval_ms'] = 20*60*1000
        return consumer_params

    def get_consumer(self, partition, low):
        topic_partition = self._topic_partition(partition, low)
        if topic_partition not in self.consumers:
            return self._fully_configured_consumer(topic_partition)
        else:
            consumer = self.consumers[topic_partition]
            try:
                consumer_offset = consumer.committed([topic_partition])[0].offset
                if consumer_offset != topic_partition.offset:
                    consumer.seek(topic_partition)
            except RuntimeError:
                return self._fully_configured_consumer(topic_partition)
            return consumer

    def _fully_configured_consumer(self, topic_partition):
        ck = self.get_ck_module()
        consumer = ck.Consumer(self.consumer_params)
        consumer.assign([topic_partition])
        self.consumers[topic_partition] = consumer
        return consumer

    def _topic_partition(self, partition, low):
        ck = self.get_ck_module()
        return ck.TopicPartition(self.topic, partition, low)

    def get_ck_module(self):
        if not self.ck:
            import confluent_kafka as ck
            self.ck = ck
        return self.ck


class FromKafkaBatched(Stream):
    """Base class for both local and cluster-based batched kafka processing"""
    def __init__(self, topic, consumer_params, poll_interval='1s',
                 npartitions=1, **kwargs):
        self.consumer_params = consumer_params
        self.topic = topic
        self.npartitions = npartitions
        self.positions = [-1] * npartitions
        self.poll_interval = convert_interval(poll_interval)
        self.stopped = True
        self.consumer_factory = ConsumerFactory(consumer_params, topic)

        super(FromKafkaBatched, self).__init__(ensure_io_loop=True, **kwargs)

    @gen.coroutine
    def poll_kafka(self):
        ck = self.consumer_factory.get_ck_module()
        consumer = ck.Consumer(self.consumer_params)

        try:
            while not self.stopped:
                out = []

                for partition in range(self.npartitions):
                    tp = ck.TopicPartition(self.topic, partition, 0)
                    try:
                        low, high = consumer.get_watermark_offsets(tp,
                                                                   timeout=0.1)
                    except (RuntimeError, ck.KafkaException):
                        continue
                    current_position = self.positions[partition]
                    if current_position == -1:
                        last_committed_offset = consumer.committed([tp])[0].offset
                        if last_committed_offset >= 0:
                            current_position = last_committed_offset
                    lowest = max(current_position, low)
                    out.append((self.consumer_factory, partition, lowest, high - 1))
                    self.positions[partition] = high

                for part in out:
                    yield self._emit(part)

                else:
                    yield gen.sleep(self.poll_interval)
        finally:
            consumer.close()

    def start(self):
        self.stopped = False
        self.loop.add_callback(self.poll_kafka)


@Stream.register_api(staticmethod)
def from_kafka_batched(topic, consumer_params, poll_interval='1s',
                       npartitions=1, start=False, dask=False, **kwargs):
    """ Get messages from Kafka in batches

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

    Example
    -------

    >>> source = Stream.from_kafka_batched('mytopic',
    ...           {'bootstrap.servers': 'localhost:9092',
    ...            'group.id': 'streamz'}, npartitions=4)  # doctest: +SKIP
    """
    if dask:
        from distributed.client import default_client
        kwargs['loop'] = default_client().loop
    source = FromKafkaBatched(topic, consumer_params,
                              poll_interval=poll_interval,
                              npartitions=npartitions, **kwargs)
    if dask:
        source = source.scatter()

    if start:
        source.start()

    return source.starmap(get_message_batch)


def get_message_batch(kc_factory, partition, low, high, timeout=None):
    """Fetch a batch of kafka messages in given topic/partition

    This will block until messages are available, or timeout is reached.
    """
    t0 = time.time()
    consumer = kc_factory.get_consumer(partition, low)
    out = []
    while True:
        msg = consumer.poll(0)
        if msg and msg.value():
            if high >= msg.offset():
                out.append(msg.value())
            if high <= msg.offset():
                break
        else:
            time.sleep(0.1)
            if timeout is not None and time.time() - t0 > timeout:
                break

    if low <= high:
        try:
            consumer.commit(asynchronous=False)
        except:
            pass
    return out
