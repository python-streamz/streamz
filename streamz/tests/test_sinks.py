import weakref

import pytest
from streamz import Stream
from streamz.sinks import _global_sinks, Sink
from streamz.utils_test import tmpfile, wait_for


def test_sink_with_args_and_kwargs():
    L = dict()

    def mycustomsink(elem, key, prefix=""):
        key = prefix + key
        if key not in L:
            L[key] = list()
        L[key].append(elem)

    s = Stream()
    sink = s.sink(mycustomsink, "cat", "super", stream_name="test")
    s.emit(1)
    s.emit(2)

    assert L['supercat'] == [1, 2]
    assert sink.name == "test"


def test_sink_to_textfile_fp():
    source = Stream()
    with tmpfile() as filename, open(filename, "w") as fp:
        source.map(str).sink_to_textfile(fp)
        source.emit(0)
        source.emit(1)

        fp.flush()

        assert open(filename, "r").read() == "0\n1\n"


def test_sink_to_textfile_named():
    source = Stream()
    with tmpfile() as filename:
        _sink = source.map(str).sink_to_textfile(filename)
        source.emit(0)
        source.emit(1)

        _sink._fp.flush()

        assert open(filename, "r").read() == "0\n1\n"


def test_sink_to_textfile_closes():
    source = Stream()
    with tmpfile() as filename:
        sink = source.sink_to_textfile(filename)
        fp = sink._fp
        _global_sinks.remove(sink)
        del sink

        with pytest.raises(ValueError, match=r"I/O operation on closed file\."):
            fp.write(".")


def test_sink_destroy():
    source = Stream()
    sink = Sink(source)
    ref = weakref.ref(sink)
    sink.destroy()

    assert sink not in _global_sinks

    del sink

    assert ref() is None


def test_ws_roundtrip():
    pytest.importorskip("websockets")
    s0 = Stream.from_websocket("localhost", 8989, start=True)
    l = s0.sink_to_list()

    data = [b'0123'] * 4
    s = Stream.from_iterable(data)
    s.to_websocket("ws://localhost:8989")
    s.start()

    wait_for(lambda: data == l, timeout=1)
    s.stop()
    s0.stop()


def test_mqtt_roundtrip():
    pytest.importorskip("paho.mqtt.client")
    s0 = Stream.from_mqtt("mqtt.eclipseprojects.io", 1883, "streamz/sensor/temperature")
    l = s0.map(lambda msg: msg.payload).sink_to_list()
    s0.start()

    data = [b'0123'] * 4
    s = Stream.from_iterable(data)
    s.to_mqtt("mqtt.eclipseprojects.io", 1883, "streamz/sensor/temperature")
    s.start()

    wait_for(lambda: data == l, timeout=1)
    s.stop()
    s0.stop()

