from streams import Stream
from operator import add

import networkx as nx
from streams.graph import plot_graph
source1 = Stream(name='source1')
source2 = Stream(name='source2')
source3 = Stream(name='awsome source')

n1 = source1.zip(source2)
n2 = n1.map(add)
n3 = n2.zip(source3)
n3.sink(source1.emit)

plot_graph(n2, file='example_graph.png')
plot_graph(n2, nx.spring_layout)
