from operator import add
import os

from dask.utils import tmpfile
import networkx as nx

from dask_streams import Stream, create_graph, visualize


def test_create_graph():
    source1 = Stream(name='source1')
    source2 = Stream(name='source2')

    n1 = source1.zip(source2)
    n2 = n1.map(add)
    n2.sink(source1.emit)

    g = nx.DiGraph()
    create_graph(n2, g)
    for t in [hash(a) for a in [source1, source2, n1, n2]]:
        assert t in g


def test_create_file():
    source1 = Stream(name='source1')
    source2 = Stream(name='source2')

    n1 = source1.zip(source2)
    n2 = n1.map(add)
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

        for word in ['rankdir', 'source1', 'source2', 'zip', 'map', 'add']:
            assert word in text
