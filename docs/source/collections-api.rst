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
   Batch
   Batch.filter
   Batch.map
   Batch.pluck
   Batch.to_dataframe
   Batch.to_stream

Dataframes
----------

.. currentmodule:: streamz.dataframe

.. autosummary::
   DataFrame
   DataFrame.groupby
   DataFrame.rolling
   DataFrame.assign
   DataFrame.sum
   DataFrame.mean
   DataFrame.cumsum
   DataFrame.cumprod
   DataFrame.cummin
   DataFrame.cummax

.. autosummary::
   GroupBy
   GroupBy.count
   GroupBy.mean
   GroupBy.size
   GroupBy.std
   GroupBy.sum
   GroupBy.var

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
   DataFrame.window
   Window.apply
   Window.count
   Window.groupby
   Window.sum
   Window.size
   Window.std
   Window.var

.. autosummary::
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
   PeriodicDataFrame

.. autosummary::
   Random

Details
-------

.. currentmodule:: streamz.collection

.. autoclass:: Streaming
   :members:

.. currentmodule:: streamz.batch

.. autoclass:: Batch
   :members:
   :inherited-members:

.. currentmodule:: streamz.dataframe

.. autoclass:: DataFrame
   :members:
   :inherited-members:

.. autoclass:: Rolling
   :members:

.. autoclass:: Window
   :members:

.. autoclass:: GroupBy
   :members:

.. autoclass:: Random
