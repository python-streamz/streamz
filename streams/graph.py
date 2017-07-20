"""Graphing utilities for EventStreams"""
import networkx as nx


def create_graph(node, graph, prior_node=None, pc=None):
    """Create graph from a single node, searching up and down the chain

    Parameters
    ----------
    node: EventStream instance
    graph: networkx.DiGraph instance
    """
    if node is None:
        return
    t = hash(node)
    graph.add_node(t, str=str(node))
    if prior_node:
        tt = hash(prior_node)
        if graph.has_edge(t, tt):
            return
        if pc == 'parent':
            graph.add_edge(tt, t)
        else:
            graph.add_edge(t, tt)

    for nodes, pc in zip([list(node.parents), list(node.children)],
                         ['parent', 'children']):
        for node2 in nodes:
            if node2 is not None:
                create_graph(node2, graph, node, pc=pc)


def visualize(node, filename='mystream.png'):
    """Render the computation of this object's task graph using graphviz.

    Requires ``graphviz`` to be installed.

    Parameters
    ----------
    node: Stream instance
        A node in the task graph
    filename : str, optional
        The name of the file to write to disk.
    """

    g = nx.DiGraph()
    create_graph(node, g)
    mapping = {k: '{}'.format(g.node[k]['str']) for k in g}
    idx_mapping = {}
    for k, v in mapping.items():
        if v in idx_mapping.keys():
            idx_mapping[v] += 1
            mapping[k] += '-{}'.format(idx_mapping[v])
        else:
            idx_mapping[v] = 0

    gg = {k: v for k, v in mapping.items()}
    rg = nx.relabel_nodes(g, gg, copy=True)
    a = nx.nx_agraph.to_agraph(rg)
    a.layout('dot')
    extension = os.path.splitext(filename)[1]
    a.draw(filename, format=extension)
