import inspect
import weakref

from tornado import gen

from streamz import Stream
from streamz.core import sync

# sinks add themselves here to avoid being garbage-collected
_global_sinks = set()


class Sink(Stream):

    _graphviz_shape = 'trapezium'

    def __init__(self, upstream, **kwargs):
        super().__init__(upstream, **kwargs)
        _global_sinks.add(self)

    def destroy(self):
        super().destroy()
        _global_sinks.remove(self)


@Stream.register_api()
class sink(Sink):
    """ Apply a function on every element

    Parameters
    ----------
    func: callable
        A function that will be applied on every element.
    args:
        Positional arguments that will be passed to ``func`` after the incoming element.
    kwargs:
        Stream-specific arguments will be passed to ``Stream.__init__``, the rest of
        them will be passed to ``func``.

    Examples
    --------
    >>> source = Stream()
    >>> L = list()
    >>> source.sink(L.append)
    >>> source.sink(print)
    >>> source.sink(print)
    >>> source.emit(123)
    123
    123
    >>> L
    [123]

    See Also
    --------
    map
    Stream.sink_to_list
    """

    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        # take the stream specific kwargs out
        sig = set(inspect.signature(Stream).parameters)
        stream_kwargs = {k: v for (k, v) in kwargs.items() if k in sig}
        self.kwargs = {k: v for (k, v) in kwargs.items() if k not in sig}
        self.args = args
        super().__init__(upstream, **stream_kwargs)

    def update(self, x, who=None, metadata=None):
        result = self.func(x, *self.args, **self.kwargs)
        if gen.isawaitable(result):
            return result
        else:
            return []


@Stream.register_api()
class sink_to_textfile(Sink):
    """ Write elements to a plain text file, one element per line.

        Type of elements must be ``str``.

        Parameters
        ----------
        file: str or file-like
            File to write the elements to. ``str`` is treated as a file name to open.
            If file-like, descriptor must be open in text mode. Note that the file
            descriptor will be closed when this sink is destroyed.
        end: str, optional
            This value will be written to the file after each element.
            Defaults to newline character.
        mode: str, optional
            If file is ``str``, file will be opened in this mode. Defaults to ``"a"``
            (append mode).

        Examples
        --------
        >>> source = Stream()
        >>> source.map(str).sink_to_textfile("test.txt")
        >>> source.emit(0)
        >>> source.emit(1)
        >>> print(open("test.txt", "r").read())
        0
        1
    """
    def __init__(self, upstream, file, end="\n", mode="a", **kwargs):
        self._end = end
        self._fp = open(file, mode=mode) if isinstance(file, str) else file
        weakref.finalize(self, self._fp.close)
        super().__init__(upstream, **kwargs)

    def update(self, x, who=None, metadata=None):
        self._fp.write(x + self._end)


@Stream.register_api()
class to_kafka(Stream):
    """ Writes data in the stream to Kafka

    This stream accepts a string or bytes object. Call ``flush`` to ensure all
    messages are pushed. Responses from Kafka are pushed downstream.

    Parameters
    ----------
    topic : string
        The topic which to write
    producer_config : dict
        Settings to set up the stream, see
        https://docs.confluent.io/current/clients/confluent-kafka-python/#configuration
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        Examples:
        bootstrap.servers: Connection string (host:port) to Kafka

    Examples
    --------
    >>> from streamz import Stream
    >>> ARGS = {'bootstrap.servers': 'localhost:9092'}
    >>> source = Stream()
    >>> kafka = source.map(lambda x: str(x)).to_kafka('test', ARGS)
    <to_kafka>
    >>> for i in range(10):
    ...     source.emit(i)
    >>> kafka.flush()
    """
    def __init__(self, upstream, topic, producer_config, **kwargs):
        import confluent_kafka as ck

        self.topic = topic
        self.producer = ck.Producer(producer_config)

        kwargs["ensure_io_loop"] = True
        Stream.__init__(self, upstream, **kwargs)
        self.stopped = False
        self.polltime = 0.2
        self.loop.add_callback(self.poll)
        self.futures = []

    @gen.coroutine
    def poll(self):
        while not self.stopped:
            # executes callbacks for any delivered data, in this thread
            # if no messages were sent, nothing happens
            self.producer.poll(0)
            yield gen.sleep(self.polltime)

    def update(self, x, who=None, metadata=None):
        future = gen.Future()
        self.futures.append(future)

        @gen.coroutine
        def _():
            while True:
                try:
                    # this runs asynchronously, in C-K's thread
                    self.producer.produce(self.topic, x, callback=self.cb)
                    return
                except BufferError:
                    yield gen.sleep(self.polltime)
                except Exception as e:
                    future.set_exception(e)
                    return

        self.loop.add_callback(_)
        return future

    @gen.coroutine
    def cb(self, err, msg):
        future = self.futures.pop(0)
        if msg is not None and msg.value() is not None:
            future.set_result(None)
            yield self._emit(msg.value())
        else:
            future.set_exception(err or msg.error())

    def flush(self, timeout=-1):
        self.producer.flush(timeout)


@Stream.register_api()
class to_websocket(Sink):
    """Write bytes data to websocket

    The websocket will be opened on first call, and kept open. Should
    it close at some point, future writes will fail.

    Requires the ``websockets`` package.

    :param uri: str
        Something like "ws://host:port". Use "wss:" to allow TLS.
    :param ws_kwargs: dict
        Further kwargs to pass to ``websockets.connect``, please
        read its documentation.
    :param kwargs:
        Passed to superclass
    """

    def __init__(self, upstream, uri, ws_kwargs=None, **kwargs):
        self.uri = uri
        self.ws_kw = ws_kwargs
        self.ws = None
        super().__init__(upstream, ensure_io_loop=True, **kwargs)

    async def update(self, x, who=None, metadata=None):
        import websockets
        if self.ws is None:
            self.ws = await websockets.connect(self.uri, **(self.ws_kw or {}))
        await self.ws.send(x)

    def destroy(self):
        super().destroy()
        if self.ws is not None:
            sync(self.loop, self.ws.protocol.close)
        self.ws = None


@Stream.register_api()
class to_mqtt(Sink):
    """
    Send data to MQTT broker

    See also ``sources.from_mqtt``.

    Requires ``paho.mqtt``

    :param host: str
    :param port: int
    :param topic: str
    :param keepalive: int
        See mqtt docs - to keep the channel alive
    :param client_kwargs:
        Passed to the client's ``connect()`` method
    """
    def __init__(self, upstream, host, port, topic, keepalive=60, client_kwargs=None,
                 **kwargs):
        self.host = host
        self.port = port
        self.c_kw = client_kwargs or {}
        self.client = None
        self.topic = topic
        self.keepalive = keepalive
        super().__init__(upstream, ensure_io_loop=True, **kwargs)

    def update(self, x, who=None, metadata=None):
        import paho.mqtt.client as mqtt
        if self.client is None:
            self.client = mqtt.Client()
            self.client.connect(self.host, self.port, self.keepalive, **self.c_kw)
        # TODO: wait on successful delivery
        self.client.publish(self.topic, x)

    def destroy(self):
        self.client.disconnect()
        self.client = None
        super().destroy()
