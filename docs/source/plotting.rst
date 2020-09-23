Visualizing streamz
===================

A variety of tools are available to help you understand, debug,
visualize your streaming objects:

- Most Streamz objects automatically display themselves in Jupyter
  notebooks, periodically updating their visual representation as text
  or tables by registering events with the Tornado IOLoop used by Jupyter
- The network graph underlying a stream can be visualized using `dot` to
  render a PNG using `Stream.visualize(filename)`
- Streaming data can be visualized using the optional separate packages
  hvPlot, HoloViews, and Panel (see below)


hvplot.streamz
--------------

hvPlot is a separate plotting library providing Bokeh-based plots for
Pandas dataframes and a variety of other object types, including
streamz DataFrame and Series objects.

See `hvplot.holoviz.org <https://hvplot.holoviz.org>`_ for
instructions on how to install hvplot.  Once it is installed, you can
use the Pandas .plot() API to get a dynamically updating plot in
Jupyter or in Bokeh/Panel Server:

.. code-block:: python

   import hvplot.streamz
   from streamz.dataframe import Random
   
   df = Random()
   df.hvplot(backlog=100)

See the `streaming section
<https://hvplot.holoviz.org/user_guide/Streaming.html>`_ of the hvPlot
user guide for more details, and the `dataframes.ipynb` example that
comes with streamz for a simple runnable example.


HoloViews
---------

hvPlot is built on HoloViews, and you can also use HoloViews directly
if you want more control over events and how they are processed.  See
the `HoloViews user guide
<http://holoviews.org/user_guide/Streaming_Data.html>`_ for more
details.


Panel
-----

Panel is a general purpose dashboard and app framework, supporting a
wide variety of displayable objects as "Panes". Panel provides a
`streamz Pane
<https://panel.holoviz.org/reference/panes/Streamz.html>`_ for
rendering arbitrary streamz objects, and streamz DataFrames are
handled by the Panel `DataFrame Pane
<https://panel.holoviz.org/reference/panes/DataFrame.html>`_.
