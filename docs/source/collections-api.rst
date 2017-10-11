Collections API
===============

Collections
-----------

.. currentmodule:: streamz.collection

.. autosummary::
   Streaming
   Streaming.map_partitions
   Streaming.accumulate_partitions
   Streaming.verify

Batch
-----

.. currentmodule:: streamz.batch

.. autosummary::
   StreamingBatch
   StreamingBatch.filter
   StreamingBatch.map
   StreamingBatch.pluck
   StreamingBatch.to_dataframe
   StreamingBatch.to_stream

Dataframes
----------

.. currentmodule:: streamz.dataframe

.. autosummary::
   StreamingDataFrame
   StreamingDataFrame.groupby
   StreamingDataFrame.rolling
   StreamingDataFrame.assign
   StreamingDataFrame.sum
   StreamingDataFrame.mean
   StreamingDataFrame.cumsum
   StreamingDataFrame.cumprod
   StreamingDataFrame.cummin
   StreamingDataFrame.cummax
   StreamingDataFrame.plot

.. autosummary::
   Rolling
   Rolling.aggregate
   Rolling.count
   Rolling.max
   Rolling.mean
   Rolling.median
   Rolling.min
   Rolling.quantile
   Rolling.std
   Rolling.sum
   Rolling.var

.. autosummary::
   Random

Details
-------

.. currentmodule:: streamz.collection

.. autoclass:: Streaming
   :members:

.. currentmodule:: streamz.batch

.. autoclass:: StreamingBatch
   :members:
   :inherited-members:

.. currentmodule:: streamz.dataframe

.. autoclass:: StreamingDataFrame
   :members:
   :inherited-members:

.. autoclass:: Rolling
   :members:

.. autoclass:: Random
