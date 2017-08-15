"""Graphing utilities for EventStreams"""
from __future__ import absolute_import, division, print_function

from functools import partial
import os


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


def readable_graph(node):
    """Create human readable version of this object's task graph.

    Parameters
    ----------
    node: Stream instance
        A node in the task graph
    """
    import networkx as nx
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
    return rg


def to_graphviz(graph, **graph_attr):
    import graphviz
    gvz = graphviz.Digraph(graph_attr=graph_attr)
    for node in graph.nodes():
        gvz.node(node)
    gvz.edges(graph.edges())
    return gvz


def visualize(node, filename='mystream.png', **kwargs):
    """
    Render a task graph using dot.

    If `filename` is not None, write a file to disk with that name in the
    format specified by `format`.  `filename` should not include an extension.

    Parameters
    ----------
    dsk : dict
        The graph to display.
    filename : str or None, optional
        The name (without an extension) of the file to write to disk.  If
        `filename` is None, no file will be written, and we communicate with
        dot using only pipes.  Default is 'mydask'.
    format : {'png', 'pdf', 'dot', 'svg', 'jpeg', 'jpg'}, optional
        Format in which to write output file.  Default is 'png'.

    Returns
    -------
    result : None or IPython.display.Image or IPython.display.SVG  (See below.)

    Notes
    -----
    If IPython is installed, we return an IPython.display object in the
    requested format.  If IPython is not installed, we just return None.

    We always return None if format is 'pdf' or 'dot', because IPython can't
    display these formats natively. Passing these formats with filename=None
    will not produce any useful output.

    See Also
    --------
    streams.graph.readable_graph
    """
    rg = readable_graph(node)
    g = to_graphviz(rg, **kwargs)

    fmts = ['.png', '.pdf', '.dot', '.svg', '.jpeg', '.jpg']
    if filename is None:
        format = 'png'

    elif any(filename.lower().endswith(fmt) for fmt in fmts):
        filename, format = os.path.splitext(filename)
        format = format[1:].lower()

    else:
        format = 'png'

    data = g.pipe(format=format)
    if not data:
        raise RuntimeError("Graphviz failed to properly produce an image. "
                           "This probably means your installation of graphviz "
                           "is missing png support. See: "
                           "https://github.com/ContinuumIO/anaconda-issues/"
                           "issues/485 for more information.")

    display_cls = _get_display_cls(format)

    if not filename:
        return display_cls(data=data)

    full_filename = '.'.join([filename, format])
    with open(full_filename, 'wb') as f:
        f.write(data)

    return display_cls(filename=full_filename)


IPYTHON_IMAGE_FORMATS = frozenset(['jpeg', 'png'])
IPYTHON_NO_DISPLAY_FORMATS = frozenset(['dot', 'pdf'])


def _get_display_cls(format):
    """
    Get the appropriate IPython display class for `format`.

    Returns `IPython.display.SVG` if format=='svg', otherwise
    `IPython.display.Image`.

    If IPython is not importable, return dummy function that swallows its
    arguments and returns None.
    """
    dummy = lambda *args, **kwargs: None
    try:
        import IPython.display as display
    except ImportError:
        # Can't return a display object if no IPython.
        return dummy

    if format in IPYTHON_NO_DISPLAY_FORMATS:
        # IPython can't display this format natively, so just return None.
        return dummy
    elif format in IPYTHON_IMAGE_FORMATS:
        # Partially apply `format` so that `Image` and `SVG` supply a uniform
        # interface to the caller.
        return partial(display.Image, format=format)
    elif format == 'svg':
        return display.SVG
    else:
        raise ValueError("Unknown format '%s' passed to `dot_graph`" % format)
