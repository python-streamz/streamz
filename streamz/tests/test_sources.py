import asyncio
import sys

from flaky import flaky
import pytest
from streamz import Source
from streamz.utils_test import wait_for, await_for, gen_test
import socket


def test_periodic():
    s = Source.from_periodic(lambda: True)
    l = s.sink_to_list()
    assert s.stopped
    s.start()
    wait_for(lambda: l, 0.3, period=0.01)
    wait_for(lambda: len(l) > 1, 0.3, period=0.01)
    assert all(l)


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


@gen_test(timeout=60)
def test_process():
    cmd = ["python", "-c", "for i in range(4): print(i, end='')"]
    s = Source.from_process(cmd, with_end=True)
    if sys.platform != "win32":
        # don't know why - something with pytest and new processes
        policy = asyncio.get_event_loop_policy()
        watcher = asyncio.SafeChildWatcher()
        policy.set_child_watcher(watcher)
        watcher.attach_loop(s.loop.asyncio_loop)
    out = s.sink_to_list()
    s.start()
    yield await_for(lambda: out == [b'0123'], timeout=5)
    s.stop()


@gen_test(timeout=60)
def test_process_str():
    cmd = 'python -c "for i in range(4): print(i)"'
    s = Source.from_process(cmd)
    if sys.platform != "win32":
        # don't know why - something with pytest and new processes
        policy = asyncio.get_event_loop_policy()
        watcher = asyncio.SafeChildWatcher()
        policy.set_child_watcher(watcher)
        watcher.attach_loop(s.loop.asyncio_loop)
    out = s.sink_to_list()
    s.start()
    yield await_for(lambda: out == [b'0\n', b'1\n', b'2\n', b'3\n'], timeout=5)
    s.stop()


def test_from_iterable():
    source = Source.from_iterable(range(3))
    L = source.sink_to_list()
    source.start()
    wait_for(lambda: L == [0, 1, 2], 0.1)


def test_from_iterable_backpressure():
    it = iter(range(5))
    source = Source.from_iterable(it)
    L = source.rate_limit(0.1).sink_to_list()
    source.start()

    wait_for(lambda: L == [0], 1, period=0.01)
    assert next(it) == 2  # 1 is in blocked _emit


def test_from_iterable_stop():
    from _pytest.outcomes import Failed

    source = Source.from_iterable(range(5))
    L = source.rate_limit(0.01).sink_to_list()
    source.start()

    wait_for(lambda: L == [0], 1)
    source.stop()

    assert source.stopped
    with pytest.raises(Failed):
        wait_for(lambda: L == [0, 1, 2], 0.1)
