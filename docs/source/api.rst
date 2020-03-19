API
===

Stream
------

.. currentmodule:: streamz

.. autosummary::
   Stream

.. autosummary::
   Stream.connect
   Stream.destroy
   Stream.disconnect
   Stream.visualize
   accumulate
   buffer
   collect
   combine_latest
   delay
   filter
   flatten
   map
   partition
   rate_limit
   scatter
   sink
   slice
   sliding_window
   starmap
   timed_window
   union
   unique
   pluck
   zip
   zip_latest

.. automethod:: Stream.connect
.. automethod:: Stream.disconnect
.. automethod:: Stream.destroy
.. automethod:: Stream.emit
.. automethod:: Stream.frequencies
.. automethod:: Stream.register_api
.. automethod:: Stream.sink_to_list
.. automethod:: Stream.update
.. automethod:: Stream.visualize

Sources
-------

.. autosummary::
   filenames
   from_kafka
   from_kafka_batched
   from_process
   from_textfile
   from_tcp
   from_http_server

DaskStream
----------

.. currentmodule:: streamz.dask

.. autosummary::
   DaskStream
   gather


Definitions
-----------

.. currentmodule:: streamz

.. autofunction:: accumulate
.. autofunction:: buffer
.. autofunction:: collect
.. autofunction:: combine_latest
.. autofunction:: delay
.. autofunction:: filter
.. autofunction:: flatten
.. autofunction:: map
.. autofunction:: partition
.. autofunction:: rate_limit
.. autofunction:: sink
.. autofunction:: sliding_window
.. autofunction:: Stream
.. autofunction:: timed_window
.. autofunction:: union
.. autofunction:: unique
.. autofunction:: pluck
.. autofunction:: zip
.. autofunction:: zip_latest

.. autofunction:: filenames
.. autofunction:: from_kafka
.. autofunction:: from_kafka_batched
.. autofunction:: from_textfile

.. currentmodule:: streamz.dask

.. autofunction:: DaskStream
.. autofunction:: gather
