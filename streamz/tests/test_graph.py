from operator import add, mul
import os

import matplotlib.pyplot as plt
import pytest
nx = pytest.importorskip('networkx')

from streamz import Stream, create_graph, visualize
from streamz.utils_test import tmpfile

from ..graph import _clean_text, node_style, run_vis


def test_create_graph():
    source1 = Stream(stream_name='source1')
    source2 = Stream(stream_name='source2')

    n1 = source1.zip(source2)
    n2 = n1.map(add)
    n2.sink(source1.emit)

    g = nx.DiGraph()
    create_graph(n2, g)
    for t in [hash(a) for a in [source1, source2, n1, n2]]:
        assert t in g


def test_create_file():
    source1 = Stream(stream_name='source1')
    source2 = Stream(stream_name='source2')

    n1 = source1.zip(source2)
    n2 = n1.map(add).scan(mul).map(lambda x : x + 1)
    n2.sink(source1.emit)

    with tmpfile(extension='png') as fn:
        visualize(n1, filename=fn)
        assert os.path.exists(fn)

    with tmpfile(extension='svg') as fn:
        n1.visualize(filename=fn, rankdir="LR")
        assert os.path.exists(fn)

    with tmpfile(extension='dot') as fn:
        n1.visualize(filename=fn, rankdir="LR")
        with open(fn) as f:
            text = f.read()

        for word in ['rankdir', 'source1', 'source2', 'zip', 'map', 'add',
                     'shape=box', 'shape=ellipse']:
            assert word in text


def test_cleantext():
    text = "JFDSM*(@&$:FFDS:;;"
    expected_text = "JFDSM ;FFDS; "
    cleaned_text = _clean_text(text)
    assert cleaned_text == expected_text


def test_run_vis_smoke():
    source = Stream()

    def sleep_inc(x):
        if x == 5:
            raise RuntimeError()
        return x + 1

    def print_sleep(x):
        print(x)

    b = source.map(sleep_inc)
    b.sink(print_sleep)
    b.sink(print_sleep)
    gv = run_vis(
        source,
        source_node=True,
        edge_style={"color": "k"},
        node_label_style={"font_size": 10},
        edge_label_style=lambda x: {"label": x["label"], "font_size": 15},
        node_style=node_style,
        force_draw=True,
    )
    plt.pause(.1)
    for i in range(10):
        try:
            source.emit(i)
            plt.pause(.1)
        except RuntimeError:
            pass
