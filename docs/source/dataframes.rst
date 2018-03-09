DataFrames
==========

When handling large volumes of streaming tabular data it is often more
efficient to pass around larger Pandas dataframes with many rows each rather
than pass around individual Python tuples or dicts.  Handling and computing on
data with Pandas can be much faster than operating on Python objects.

So one could imagine building streaming dataframe pipelines using the ``.map``
and ``.accumulate`` streaming operators with functions that consume and produce
Pandas dataframes as in the following example:

.. code-block:: python

   from streamz import Stream

   def query(df):
       return df[df.name == 'Alice']

   def aggregate(acc, df):
       return acc + df.amount.sum()

   stream = Stream()
   stream.map(query).accumulate(aggregate, start=0)

This is fine, and straightforward to do if you understand ``streamz.core`` ,
Pandas, and have some skill with developing algorithms.


Streaming Dataframes
--------------------

The ``streamz.dataframe`` module provides a streaming dataframe object that
implements many of these algorithms for you.  It provides a Pandas-like
interface on streaming data.  Our example above is rewritten below using
streaming dataframes:

.. code-block:: python

   import pandas as pd
   from streamz.dataframe import DataFrame

   example = pd.DataFrame({'name': [], 'amount': []})
   sdf = DataFrame(stream, example=example)

   sdf[sdf.name == 'Alice'].amount.sum()

The two examples are identical in terms of performance and execution.  The
resulting streaming dataframe contains a ``.stream`` attribute which is
equivalent to the ``stream`` produced in the first example.  Streaming
dataframes are only syntactic sugar on core streams.


Supported Operations
--------------------

Streaming dataframes support the following classes of operations

-  Elementwise operations like ``df.x + 1``
-  Filtering like ``df[df.name == 'Alice']``
-  Column addition like ``df['z'] = df.x + df.y``
-  Reductions like ``df.amount.mean()``
-  Groupby-aggregations like ``df.groupby(df.name).amount.mean()``
-  Windowed aggregations (fixed length) like ``df.window(n=100).amount.sum()``
-  Windowed aggregations (index valued) like ``df.window(value='2h').amount.sum()``
-  Windowed groupby aggregations like ``df.window(value='2h').groupby('name').amount.sum()``


DataFrame Aggregations
----------------------

Dataframe aggregations are composed of an aggregation (like sum, mean, ...) and
a windowing scheme (fixed sized windows, index-valued, all time, ...)

Aggregations
++++++++++++

Streaming Dataframe aggregations are built from three methods

-  ``initial``: Creates initial state given an empty example dataframe
-  ``on_new``: Updates state and produces new result to emit given new data
-  ``on_old``: Updates state and produces new result to emit given decayed data

So a simple implementation of ``sum`` as an aggregation might look like the
following:

.. code-block:: python

   from streamz.dataframe import Aggregation

   class Mean(Aggregation):
       def initial(self, new):
           state = new.iloc[:0].sum(), new.iloc[:0].count()
           return state

       def on_new(self, state, new):
           total, count = state
           total = total + new.sum()
           count = count + new.count()
           new_state = (total, count)
           new_value = total / count
           return new_state, new_value

       def on_old(self, state, old):
           total, count = state
           total = total - new.sum()   # switch + for - here
           count = count - new.count() # switch + for - here
           new_state = (total, count)
           new_value = total / count
           return new_state, new_value

These aggregations can then used in a variety of different windowing schemes
with the ``aggregate`` method as follows:

.. code-block:: python

    df.aggregate(Mean())

    df.window(n=100).aggregate(Mean())

    df.window(value='60s').aggregate(Mean())

whose job it is to deliver new and old data to your aggregation for processing.


Windowing Schemes
+++++++++++++++++

Different windowing schemes like fixed sized windows (last 100 elements) or
value-indexed windows (last two hours of data) will track newly arrived and
decaying data and call these methods accordingly.  The mechanism to track data
arriving and leaving is kept orthogonal from the aggregations themselves.
These windowing schemes include the following:

1.  All previous data.  Only ``initial`` and ``on_new`` are called, ``on_old``
    is never called.

    .. code-block:: python

       >>> df.sum()

2.  The previous ``n`` elements

    .. code-block:: python

       >>> df.window(n=100).sum()

3.  An index range, like a time range for a datetime index

    .. code-block:: python

       >>> df.window(value='2h').sum()

    Although this can be done for any range on any type of index, time is just
    a common case.

Windowing schemes generally maintain a deque of historical values within
accumulated state.  As new data comes in they inspect that state and eject data
that no longer falls within the window.


Grouping
++++++++

Groupby aggregations also maintain historical data on the grouper and perform a
parallel aggregation on the number of times any key has been seen, removing
that key once it is no longer present.


Dask
----

In all cases, dataframe operations are only implemented with the ``.map`` and
``.accumulate`` operators, and so are equally compatible with core ``Stream``
and ``DaskStream`` objects.


Not Yet Supported
-----------------

Streaming dataframes algorithms do not currently pay special attention to data
arriving out-of-order.
