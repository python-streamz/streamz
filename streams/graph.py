"""Graphing utilities for EventStreams"""
import networkx as nx
import matplotlib.pyplot as plt
from networkx.drawing.nx_agraph import graphviz_layout


def get_all_nodes(node, l):
    if node is None:
        return
    t = hash(node)
    if t in l:
        return
    l.append(t)
    nodes = list(node.parents) + list(node.children)
    for node2 in nodes:
        tt = hash(node2)
        if tt not in l:
            get_all_nodes(node2, l)


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


def plot_graph(node, layout=graphviz_layout, file=None):
    g = nx.DiGraph()
    create_graph(node, g)
    create_graph(node, g)
    p = layout(g)
    mapping = {k: '{}'.format(g.node[k]['str']) for k in g}
    idx_mapping = {}
    for k, v in mapping.items():
        if v in idx_mapping.keys():
            idx_mapping[v] += 1
            mapping[k] += ' {}'.format(idx_mapping[v])
        else:
            idx_mapping[v] = 0

    gg = {k: v for k, v in mapping.items()}
    rg = nx.relabel_nodes(g, gg, copy=True)
    if file:
        a = nx.nx_agraph.to_agraph(rg)
        a.layout('dot')
        a.draw(file)
    else:
        nx.draw_networkx_labels(g, p, labels={k: g.node[k]['str'] for k in g})
        nx.draw_networkx_nodes(g, p, alpha=.3)
        nx.draw_networkx_edges(g, p)
        plt.show()
    return g, gg
