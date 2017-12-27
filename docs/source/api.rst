API
===

Stream
------

.. currentmodule:: streamz

.. autosummary::
   Stream

.. autosummary::
   accumulate
   buffer
   collect
   combine_latest
   Stream.connect
   delay
   Stream.destroy
   Stream.disconnect
   filter
   flatten
   map
   partition
   rate_limit
   scatter
   sink
   sliding_window
   starmap
   timed_window
   union
   unique
   pluck
   zip
   zip_latest

Sources
-------

.. autosummary::
   filenames
   from_kafka
   from_textfile

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
.. autofunction:: from_textfile

.. currentmodule:: streamz.dask

.. autofunction:: DaskStream
.. autofunction:: gather
