"""Graphing utilities for EventStreams"""
from __future__ import absolute_import, division, print_function

import os
import re
from functools import partial
from weakref import ref

import matplotlib.pyplot as plt

from .core import Stream


def _clean_text(text, match=None):
    ''' Clean text, remove forbidden characters.
    '''
    # all non alpha numeric characters, except for _ and :
    # replace them with space
    # the + condenses a group of consecutive characters all into one space
    # (rather than assigning a space to each)
    if match is None:
        match = '[^a-zA-Z0-9_:]+'
    text = re.sub(match, ' ', text)
    # now replace the colon with semicolon
    text = re.sub(":", ";", text)
    return text


def create_graph(node, graph, prior_node=None, pc=None):
    """Create graph from a single node, searching up and down the chain

    Parameters
    ----------
    node: Stream instance
    graph: networkx.DiGraph instance
    """
    if node is None:
        return
    t = hash(node)
    graph.add_node(t,
                   label=_clean_text(str(node)),
                   shape=node._graphviz_shape,
                   orientation=str(node._graphviz_orientation),
                   style=node._graphviz_style,
                   fillcolor=node._graphviz_fillcolor)
    if prior_node:
        tt = hash(prior_node)
        if graph.has_edge(t, tt):
            return
        if pc == 'downstream':
            graph.add_edge(tt, t)
        else:
            graph.add_edge(t, tt)

    for nodes, pc in zip([list(node.downstreams), list(node.upstreams)],
                         ['downstream', 'upstreams']):
        for node2 in nodes:
            if node2 is not None:
                create_graph(node2, graph, node, pc=pc)


def create_edge_label_graph(node, graph, prior_node=None, pc=None, i=None):
    """Create graph from a single node, searching up and down the chain

    Parameters
    ----------
    node: Stream instance
    graph: networkx.DiGraph instance
    """
    if node is None:
        return
    t = hash(node)
    graph.add_node(t,
                   label=_clean_text(str(node)),
                   shape=node._graphviz_shape,
                   orientation=str(node._graphviz_orientation),
                   style=node._graphviz_style,
                   fillcolor=node._graphviz_fillcolor)
    if prior_node:
        tt = hash(prior_node)
        if graph.has_edge(t, tt):
            return
        if i is None:
            i = ''
        if pc == 'downstream':
            graph.add_edge(tt, t, label=str(i))
        else:
            graph.add_edge(t, tt)

    for nodes, pc in zip([list(node.downstreams), list(node.upstreams)],
                         ['downstream', 'upstreams']):
        for i, node2 in enumerate(nodes):
            if node2 is not None:
                if len(nodes) > 1:
                    create_edge_label_graph(node2, graph, node, pc=pc, i=i)
                else:
                    create_edge_label_graph(node2, graph, node, pc=pc)


def create_graph_nodes(node, graph, prior_node=None, pc=None):
    """Create graph from a single node, searching up and down the chain
    with weakrefs to nodes in the graph nodes

    Parameters
    ----------
    node: Stream instance
    graph: networkx.DiGraph instance
    """
    if node is None:
        return
    t = hash(node)
    graph.add_node(
        t,
        label=_clean_text(str(node)),
        shape=node._graphviz_shape,
        orientation=str(node._graphviz_orientation),
        style=node._graphviz_style,
        fillcolor=node._graphviz_fillcolor,
        node=ref(node),
    )
    if prior_node:
        tt = hash(prior_node)
        # If we emit on something other than all the upstreams vis it
        if graph.has_edge(t, tt):
            return
        if pc == "downstream":
            graph.add_edge(tt, t)
        else:
            graph.add_edge(t, tt)

    for nodes, pc in zip(
        [list(node.downstreams), list(node.upstreams)],
        ["downstream", "upstreams"],
    ):
        for node2 in nodes:
            if node2 is not None:
                create_graph_nodes(node2, graph, node, pc=pc)


def readable_graph(node, source_node=False):
    """Create human readable version of this object's task graph.

    Parameters
    ----------
    node: Stream instance
        A node in the task graph
    """
    import networkx as nx
    g = nx.DiGraph()
    if source_node:
        create_edge_label_graph(node, g)
    else:
        create_graph(node, g)
    mapping = {k: '{}'.format(g.node[k]['label']) for k in g}
    idx_mapping = {}
    for k, v in mapping.items():
        if v in idx_mapping.keys():
            idx_mapping[v] += 1
            mapping[k] += '-{}'.format(idx_mapping[v])
        else:
            idx_mapping[v] = 0

    gg = {k: v for k, v in mapping.items()}
    rg = nx.relabel_nodes(g, gg, copy=True)
    return rg, gg


def to_graphviz(graph, **graph_attr):
    import graphviz
    gvz = graphviz.Digraph(graph_attr=graph_attr)
    for node, attrs in graph.node.items():
        gvz.node(node, **attrs)
    for edge, attrs in graph.edges().items():
        gvz.edge(edge[0], edge[1], **attrs)
    return gvz


def visualize(node, filename='mystream.png', source_node=False, **kwargs):
    """
    Render a task graph using dot.

    If `filename` is not None, write a file to disk with that name in the
    format specified by `format`.  `filename` should not include an extension.

    Parameters
    ----------
    node : Stream instance
        The stream to display.
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
    rg, gg = readable_graph(node, source_node=source_node)
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


class LiveGraphPlot(object):
    """Live plotting of the streamz graph status"""

    def __init__(
        self,
        graph,
        layout="spectral",
        node_style=None,
        edge_style=None,
        node_label_style=None,
        edge_label_style=None,
        ax=None,
        force_draw=False,
    ):
        """

        Parameters
        ----------
        graph : nx.Graph
            The graph to be plotted
        layout : string or callable, optional, default: "spectral"
            Specifies the type of layout to use for plotting.
            It must be one of "spring", "circular", "random", "kamada_kawai",
            "shell", "spectral", or a callable.
        node_style : dict or callable, optional
            The style parameters for nodes, if callable must return a dict
        edge_style : dict or callable, optional
            The style parameters for edges, if callable must return a dict
        node_label_style : dict or callable, optional
            The style parameters for node labels, if callable must return a dict
        edge_label_style : dict or callable, optional
            The style parameters for edge labels, if callable must return a dict
        ax : matplotlib axis object, optional
            The axis to plot on. If not provided produce fig and ax internally.
        force_draw : bool, optional
            If True force drawing every time graph is updated, else only draw
            when idle. Defaults to False
        """
        from grave import plot_network
        self.force_draw = force_draw
        if edge_label_style is None:
            edge_label_style = {}
        if node_label_style is None:
            node_label_style = {}
        if edge_style is None:
            edge_style = {}
        if node_style is None:
            node_style = {}
        self.node_style = node_style
        self.edge_style = edge_style
        self.node_label_style = node_label_style
        self.edge_label_style = edge_label_style
        self.layout = layout
        self.graph = graph
        if not ax:
            fig, ax = plt.subplots()
        self.ax = ax
        self.art = plot_network(
            self.graph,
            node_style=self.node_style,
            edge_style=self.edge_style,
            node_label_style=self.node_label_style,
            edge_label_style=self.edge_label_style,
            layout=self.layout,
            ax=self.ax,
        )
        self.update()

    def update(self):
        """Update the graph plot"""
        # TODO: reuse the current node positions (if no new nodes added)
        self.art._reprocess()
        if self.force_draw:
            plt.draw()
        else:
            self.ax.figure.canvas.draw_idle()


def decorate_nodes(graph, update_decorator=None, emit_decorator=None):
    """Decorate node methods for nodes in a graph

    Parameters
    ----------
    graph : nx.Graph instance
        The graph who's nodes are to be updated
    update_decorator : callable, optional
        The function to wrap the update method. If None no decorator is applied.
    emit_decorator : callable, optional
        The function to wrap the _emit method. If None no decorator is applied.

    Returns
    -------

    """
    for n, attrs in graph.nodes.items():
        nn = attrs["node"]()
        if nn.__class__ != Stream:
            if update_decorator:
                nn.update = update_decorator(attrs["node"]().update)
            if emit_decorator:
                nn._emit = emit_decorator(attrs["node"]()._emit)


status_color_map = {"running": "yellow", "waiting": "green", "error": "red"}


def node_style(node_attrs):
    d = {
        "size": 2000,
        "color": status_color_map.get(node_attrs.get("status", "NA"), "k"),
    }
    return d


def run_vis(node, source_node=False, **kwargs):
    """Start the visualization of a pipeline

    Parameters
    ----------
    node : Stream instance
        A node in the pipeline
    source_node : bool
        If True the input node is the source node and numbers the
        graph edges accordingly, defaults to False
    kwargs : Any
        kwargs passed to LiveGraphPlot

    Returns
    -------

    """
    g, gg = readable_graph(node, source_node=source_node)
    fig, ax = plt.subplots()
    gv = LiveGraphPlot(g, ax=ax, **kwargs)

    def update_decorator(func):
        node = hash(func.__self__)
        node_name = gg[node]

        # @wraps
        def wrapps(*args, **kwargs):
            g.nodes[node_name]["status"] = "running"
            gv.update()
            try:
                ret = func(*args, **kwargs)
            except Exception as e:
                g.nodes[node_name]["status"] = "error"
                gv.update()
                raise e
            else:
                g.nodes[node_name]["status"] = "waiting"
                gv.update()
                return ret

        return wrapps

    def emit_decorator(func):
        node = hash(func.__self__)
        node_name = gg[node]

        def wrapps(*args, **kwargs):
            g.nodes[node_name]["status"] = "waiting"
            gv.update()
            try:
                ret = func(*args, **kwargs)
            except Exception as e:
                g.nodes[node_name]["status"] = "error"
                gv.update()
                raise e
            else:
                return ret

        return wrapps

    import networkx as nx
    node_g = nx.DiGraph()
    create_graph_nodes(node, node_g)
    decorate_nodes(node_g, update_decorator, emit_decorator)
    return gv