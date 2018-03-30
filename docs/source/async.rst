Asynchronous Computation
========================

*This section is only relevant if you want to use time-based functionality.  If
you are only using operations like map and accumulate then you can safely skip
this section.*

When using time-based flow control like ``rate_limit``, ``delay``, or
``timed_window`` Streamz relies on the Tornado_ framework for concurrency.
This allows us to handle many concurrent operations cheaply and consistently
within a single thread.  However, this also adds complexity and requires some
understanding of asynchronous programming.  There are a few different ways to
use Streamz with a Tornado event loop.

We give a few examples below that all do the same thing, but with different
styles.  In each case we use the following toy functions:

.. code-block:: python

   from tornado import gen
   import time

   def increment(x):
       """ A blocking increment function

       Simulates a computational function that was not designed to work
       asynchronously
       """
       time.sleep(0.1)
       return x + 1

   @gen.coroutine
   def write(x):
       """ A non-blocking write function

       Simulates writing to a database asynchronously
       """
       yield gen.sleep(0.2)
       print(x)


Within the Event Loop
---------------------

You may have an application that runs strictly within an event loop.

.. code-block:: python

   from streamz import Stream
   from tornado.ioloop import IOLoop

   @gen.coroutine
   def f():
       source = Stream(asynchronous=True)  # tell the stream we're working asynchronously
       source.map(increment).rate_limit(0.500).sink(write)

       for x in range(10):
           yield source.emit(x)

   IOLoop().run_sync(f)

We call Stream with the ``asynchronous=True`` keyword, informing it that it
should expect to operate within an event loop.  This ensures that calls to
``emit`` return Tornado futures rather than block.  We wait on results using
``yield``.

.. code-block:: python

   yield source.emit(x)  # waits until the pipeline is ready

This would also work with async-await syntax in Python 3

.. code-block:: python

   from streamz import Stream
   from tornado.ioloop import IOLoop

   async def f():
       source = Stream(asynchronous=True)  # tell the stream we're working asynchronously
       source.map(increment).rate_limit(0.500).sink(write)

       for x in range(10):
           await source.emit(x)

   IOLoop().run_sync(f)


Event Loop on a Separate Thread
-------------------------------

Sometimes the event loop runs on a separate thread.  This is common when you
want to support interactive workloads (the user needs their own thread for
interaction) or when using Dask (next section).

.. code-block:: python

   from streamz import Stream

   source = Stream(asynchronous=False)  # starts IOLoop in separate thread
   source.map(increment).rate_limit('500ms').sink(write)

   for x in range(10):
       source.emit(x)

In this case we pass ``asynchronous=False`` to inform the stream that it is
expected to perform time-based computation (our write function is a coroutine)
but that it should not expect to run in an event loop, and so needs to start
its own in a separate thread.  Now when we call ``source.emit`` normally
without using ``yield`` or ``await`` the emit call blocks, waiting on a
coroutine to finish within the IOLoop.

All functions here happen on the IOLoop.  This is good for consistency, but can
cause other concurrent applications to become unresponsive if your functions
(like ``increment``) block for long periods of time.  You might address this by
using Dask (see below) which will offload these computations onto separate
threads or processes.


Using Dask
----------

Dask_ is a parallel computing library that uses Tornado for concurrency and
threads for computation.  The ``DaskStream`` object is a drop-in replacement
for ``Stream`` (mostly). Typically we create a Dask client, and then
``scatter`` a local Stream to become a DaskStream.

.. code-block:: python

   from dask.distributed import Client
   client = Client(processes=False)  # starts thread pool, IOLoop in separate thread

   from streamz import Stream
   source = Stream()
   (source.scatter()       # scatter local elements to cluster, creating a DaskStream
          .map(increment)  # map a function remotely
          .buffer(5)       # allow five futures to stay on the cluster at any time
          .gather()        # bring results back to local process
          .sink(write))    # call write locally

   for x in range(10):
       source.emit(x)

This operates very much like the synchronous case in terms of coding style (no
``@gen.coroutine`` or ``yield``) but does computations on separate threads.
This also provides parallelism and access to a dashboard at
http://localhost:8787/status .


Asynchronous Dask
-----------------

Dask can also operate within an event loop if preferred.  Here you can get the
non-blocking operation within an event loop while also offloading computations
to separate threads.

.. code-block:: python

   from dask.distributed import Client
   from tornado.ioloop import IOLoop

   async def f():
       client = await Client(processes=False, asynchronous=True)
       source = Stream(asynchronous=True)
       source.scatter().map(increment).rate_limit('500ms').gather().sink(write)

       for x in range(10):
           await source.emit(x)

   IOLoop().run_sync(f)


.. _Tornado: http://www.tornadoweb.org/en/stable/
.. _Dask: https://dask.pydata.org/en/latest/
