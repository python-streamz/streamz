from flaky import flaky
import pytest
from streamz import Source, Stream
from streamz.utils_test import wait_for, await_for, gen_test
import socket


@flaky(max_runs=3, min_passes=1)
def test_tcp():
    port = 9876
    s = Source.from_tcp(port)
    out = s.sink_to_list()
    s.start()
    wait_for(lambda: s.server is not None, 2, period=0.02)

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", port))
        sock.send(b'data\n')
        sock.close()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", port))
        sock.send(b'data\n')

        sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock2.connect(("localhost", port))
        sock2.send(b'data2\n')
        wait_for(lambda: out == [b'data\n', b'data\n', b'data2\n'], 2,
                 period=0.01)
    finally:
        s.stop()
        sock.close()
        sock2.close()


@flaky(max_runs=3, min_passes=1)
@gen_test(timeout=60)
def test_tcp_async():
    port = 9876
    s = Source.from_tcp(port)
    out = s.sink_to_list()
    s.start()
    yield await_for(lambda: s.server is not None, 2, period=0.02)

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", port))
        sock.send(b'data\n')
        sock.close()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", port))
        sock.send(b'data\n')

        sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock2.connect(("localhost", port))
        sock2.send(b'data2\n')
        yield await_for(lambda: out == [b'data\n', b'data\n', b'data2\n'], 2,
                        period=0.01)
    finally:
        s.stop()
        sock.close()
        sock2.close()


def test_http():
    requests = pytest.importorskip('requests')
    port = 9875
    s = Source.from_http_server(port)
    out = s.sink_to_list()
    s.start()
    wait_for(lambda: s.server is not None, 2, period=0.02)

    r = requests.post('http://localhost:%i/' % port, data=b'data')
    wait_for(lambda: out == [b'data'], 2, period=0.01)
    assert r.ok

    r = requests.post('http://localhost:%i/other' % port, data=b'data2')
    wait_for(lambda: out == [b'data', b'data2'], 2, period=0.01)
    assert r.ok

    s.stop()

    with pytest.raises(requests.exceptions.RequestException):
        requests.post('http://localhost:%i/other' % port, data=b'data2')


@flaky(max_runs=3, min_passes=1)
@gen_test(timeout=60)
def test_process():
    cmd = ["python", "-c", "for i in range(4): print(i)"]
    s = Source.from_process(cmd)
    out = s.sink_to_list()
    s.start()
    yield await_for(lambda: out == [b'0\n', b'1\n', b'2\n', b'3\n'], timeout=5)
    s.stop()


def test_backpressure_connect_empty_stream():
    from tornado import gen

    @Stream.register_api()
    class from_list(Stream):

        def __init__(self, source, **kwargs):
            self.data = source
            self.stopped = True
            super().__init__(ensure_io_loop=True, **kwargs)

        def start(self):
            self.stopped = False
            self.loop.add_callback(self.run)

        @gen.coroutine
        def run(self):
            while not self.stopped and len(self.data) > 0:
                yield self._emit(self.data.pop(0))
            self.stopped = True

    source_list = [0, 1, 2, 3, 4]

    source = Stream.from_list(source_list)
    L = source.rate_limit(1).sink_to_list()

    sout = Stream()
    L2 = sout.sink_to_list()
    source.connect(sout)
    source.start()

    wait_for(lambda: L == [0], timeout=2)
    assert len(source_list) > 0
