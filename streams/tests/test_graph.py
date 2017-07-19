from datetime import timedelta
import operator
from operator import add
from time import time

import pytest

from tornado import gen
from tornado.queues import Queue
from tornado.ioloop import IOLoop

import streams as s

from ..core import Stream
from ..graph import create_graph
from streams.sources import sink_to_file, Counter
from streams.utils_test import inc, double, gen_test, tmpfile

import networkx as nx


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
