Streamz
=======

Streamz helps you build pipelines to manage continuous streams of data.  It is
simple to use in simple cases, but also supports complex pipelines that involve
branching, joining, flow control, feedback, back pressure, and so on.

Optionally, Streamz can also work with Pandas dataframes to provide sensible
streaming operations on continuous tabular data.

To learn more about how to use streams, visit :doc:`Core documentation <core>`.


Motivation
----------

Continuous data streams arise in many applications like the following:

1.  Log processing from web servers
2.  Scientific instrument data like telemetry or image processing pipelines
3.  Financial time series
4.  Machine learning pipelines for real-time and on-line learning
5.  ...

Sometimes these pipelines are very simple, with a linear sequence of processing
steps:

.. image:: images/simple.svg
   :alt: a simple streamz pipeline

And sometimes these pipelines are more complex, involving branching, look-back
periods, feedback into earlier stages, and more.

.. image:: images/complex.svg
   :alt: a more complex streamz pipeline

Streamz endeavors to be simple in simple cases, while also being powerful
enough to let you define custom and powerful pipelines for your application.

Why not Python generator expressions?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Python users often manage continuous sequences of data with iterators or
generator expressions.

.. code-block:: python

    def fib():
        a, b = 0, 1
        while True:
            yield a
            a, b = b, a + b

    sequence = (f(n) for n in fib())

However iterators become challenging when you want to fork them or control the
flow of data.  Typically people rely on tools like ``itertools.tee``, and
``zip``.

.. code-block:: python

    x1, x2 = itertools.tee(x, 2)
    y1 = map(f, x1)
    y2 = map(g, x2)

However this quickly become cumbersome, especially when building complex
pipelines.


Related Work
------------

Streamz is similar to reactive
programming systems like `RxPY <https://github.com/ReactiveX/RxPY>`_ or big
data streaming systems like `Apache Flink <https://flink.apache.org/>`_,
`Apache Beam <https://beam.apache.org/get-started/quickstart-py/>`_ or
`Apache Spark Streaming <https://beam.apache.org/get-started/quickstart-py/>`_.


.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Contents

   core.rst
   dataframes.rst
   dask.rst
   collections.rst
   api.rst
   collections-api.rst
   dataframe-aggregations.rst
   async.rst
