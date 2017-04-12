Streams
=======

A minimal streaming system.

.. currentmodule:: streams

.. autosummary::
   Stream
   Stream.accumulate
   Stream.buffer
   Stream.concat
   Stream.delay
   Stream.emit
   Stream.filter
   Stream.flatten
   Stream.frequencies
   Stream.map
   Stream.partition
   Stream.rate_limit
   Stream.remove
   Stream.scan
   Stream.sink
   Stream.sink_to_list
   Stream.sliding_window
   Stream.timed_window
   Stream.to_dask
   Stream.unique
   Stream.zip

   DaskStream
   DaskStream.accumulate
   DaskStream.gather
   DaskStream.map
   DaskStream.scan
   DaskStream.scatter

Functions
---------

.. autoclass:: Stream
   :members:

.. autoclass:: DaskStream
   :members:
