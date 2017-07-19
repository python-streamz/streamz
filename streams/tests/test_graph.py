from operator import add

import networkx as nx

from ..core import Stream
from ..graph import create_graph


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
