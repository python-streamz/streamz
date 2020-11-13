from time import sleep

import pytest
from streamz import Stream
from streamz.sinks import _global_sinks
from streamz.utils_test import tmpfile


def test_sink_with_args_and_kwargs():
    L = dict()

    def mycustomsink(elem, key, prefix=""):
        key = prefix + key
        if key not in L:
            L[key] = list()
        L[key].append(elem)

    s = Stream()
    s.sink(mycustomsink, "cat", "super")

    s.emit(1)
    s.emit(2)
    assert L['supercat'] == [1, 2]


def test_sink_to_textfile_fp():
    source = Stream()
    with tmpfile() as filename, open(filename, "w", buffering=1) as fp:
        source.map(str).sink_to_textfile(fp)
        source.emit(0)
        source.emit(1)

        sleep(0.01)

        assert open(filename, "r").read() == "0\n1\n"


def test_sink_to_textfile_named():
    source = Stream()
    with tmpfile() as filename:
        source.map(str).sink_to_textfile(filename)
        source.emit(0)
        source.emit(1)

        sleep(0.01)

        assert open(filename, "r").read() == "0\n1\n"


def test_sink_to_textfile_closes():
    source = Stream()
    with tmpfile() as filename:
        sink = source.sink_to_textfile(filename)
        fp = sink._fp
        _global_sinks.remove(sink)
        del sink

        with pytest.raises(ValueError):  # I/O operation on closed file
            fp.write(".")
