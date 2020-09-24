Streaming GPU DataFrames (cudf)
-------------------------------

The ``streamz.dataframe`` module provides a DataFrame-like interface
on streaming data as described in the ``dataframes`` documentation. It
provides support for dataframe-like libraries such as pandas and
cudf. This documentation is specific to streaming GPU dataframes using
cudf.

The example in the ``dataframes`` documentation is rewritten below
using cudf dataframes just by replacing the ``pandas`` module with
``cudf``:

.. code-block:: python

   import cudf
   from streamz.dataframe import DataFrame

   example = cudf.DataFrame({'name': [], 'amount': []})
   sdf = DataFrame(stream, example=example)

   sdf[sdf.name == 'Alice'].amount.sum()


Supported Operations
--------------------

Streaming cudf dataframes support the following classes of operations:

-  Elementwise operations like ``df.x + 1``
-  Filtering like ``df[df.name == 'Alice']``
-  Column addition like ``df['z'] = df.x + df.y``
-  Reductions like ``df.amount.mean()``
-  Windowed aggregations (fixed length) like ``df.window(n=100).amount.sum()``

The following operations are not yet supported with cudf (as of version 0.8):

-  Groupby-aggregations like ``df.groupby(df.name).amount.mean()``
-  Windowed aggregations (index valued) like ``df.window(value='2h').amount.sum()``
-  Windowed groupby aggregations like ``df.window(value='2h').groupby('name').amount.sum()``


Window-based Aggregations with cudf are supported just as explained in
the ``dataframes`` documentation.  Support for groupby operations is
expected to be added in the future.
