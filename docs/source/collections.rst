Collections
===========

Streamz high-level collection APIs are built on top of ``streamz.core``, and
bring special consideration to certain types of data:

1.  ``streamz.batch``: supports streams of lists of Python objects like tuples
    or dictionaries
2.  ``streamz.dataframe``: supports streams of Pandas/cudf dataframes or Pandas/cudf series.
    cudf support is in beta phase and has limited functionality as of cudf version ``0.8``

These high-level APIs help us handle common situations in data processing.
They help us implement complex algorithms and also improve efficiency.

These APIs are built on the streamz core operations (map, accumulate, buffer,
timed_window, ...) which provide the building blocks to build complex pipelines
but offer no help with what those functions should be.  The higher-level APIs
help to fill in this gap for common situations.


Conversion
----------

.. currentmodule:: streamz.core

.. autosummary::
   Stream.to_batch
   Stream.to_dataframe

You can convert from core Stream objects to Batch, and
DataFrame objects using the ``.to_batch`` and ``.to_dataframe``
methods.  In each case we assume that the stream is a stream of batches (lists
or tuples) or a list of Pandas dataframes.

.. code-block:: python

   >>> batch = stream.to_batch()
   >>> sdf = stream.to_dataframe()


To convert back from a Batch or a DataFrame to a
``core.Stream`` you can access the ``.stream`` property.

.. code-block:: python

   >>> stream = sdf.stream
   >>> stream = batch.stream

Example
-------

We create a stream and connect it to a file object

.. code-block:: python

    file = ...  # filename or file-like object
    from streamz import Stream

    source = Stream.from_textfile(file)

Our file produces line-delimited JSON serialized data on which we want to call
``json.loads`` to parse into dictionaries.

To reduce overhead we first batch our records up into 100-line batches and turn
this into a Batch object.  We provide our Batch object an
example element that it will use to help it determine metadata.

.. code-block:: python

    example = [{'name': 'Alice', 'x': 1, 'y': 2}]
    lines = source.partition(100).to_batch(example=example)  # batches of 100 elements
    records = lines.map(json.loads)  # convert lines to text.

We could have done the ``.map(json.loads)`` command on the original stream, but
this way reduce overhead by applying this function to lists of items, rather
than one item at a time.

Now we convert these batches of records into pandas dataframes and do some
basic filtering and groupby-aggregations.

.. code-block:: python

   sdf = records.to_dataframe()
   sdf = sdf[sdf.name == "Alice"]
   sdf = sdf.groupby(sdf.x).y.mean()

The DataFrames satisfy a subset of the Pandas API, but now rather than
operate on the data directly, they set up a pipeline to compute the data in an
online fashion.

Finally we convert this back to a stream and push the results into a fixed-size
deque.

.. code-block:: python

   from collections import deque
   d = deque(maxlen=10)

   sdf.stream.sink(d.append)

See :doc:`Collections API <collections-api>` for more information.
