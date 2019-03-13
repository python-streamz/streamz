from streamz import Source
import socket
import time


def test_socket():
    port = 9876
    s = Source.from_socket(port)
    l = s.sink_to_list()
    s.start()

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", port))
        sock.send(b'data')
        time.sleep(0.02)
        assert l == []
        sock.send(b'\n')
        time.sleep(0.02)
        assert l == [b'data\n']
        sock.send(b'\nmore\ndata')
        time.sleep(0.02)
    finally:
        s.stop()
        sock.close()  # no error

    assert l == [b'data\n', b'\n', b'more\n']


def test_socket_multiple_connection():
    port = 9876
    s = Source.from_socket(port)
    l = s.sink_to_list()
    s.start()

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", port))
        sock.send(b'data\n')
        time.sleep(0.02)
        sock.close()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("localhost", port))
        sock.send(b'data\n')
        time.sleep(0.02)

        sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock2.connect(("localhost", port))
        sock2.send(b'data2\n')
        time.sleep(0.02)
    finally:
        s.stop()
        sock2.close()
        sock.close()

    assert l == [b'data\n', b'data\n', b'data2\n']
