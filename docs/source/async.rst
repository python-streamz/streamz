Asynchronous Computation
========================

*This section is only relevant if you want to use time-based functionality.  If
you are only using operations like map and accumulate then you can safely skip
this section.*

When using time-based flow control like ``rate_limit``, ``delay``, or
``timed_window`` Streamz relies on the Tornado_ framework for concurrency.
This allows us to handle many conncurent processes cheaply and consistently
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

       for x in range(10)
           yield source.emit(x)

   IOLoop.current().add_callback(f)
   IOLoop.current().start()

We call Stream with the ``asynchronous=True`` keyword, informing it that it
should expect to operate within an event loop.  This ensures that calls to
``emit`` return Tornado futures rather than block.  We wait on results using
``yield``.

.. code-block:: python

   yield source.emit(x)  # waits until the pipeline is ready


Event Loop on a Separate Thread
-------------------------------

Sometimes the event loop runs on a separate thread.  This is common when you
want to support interactive workloads (the user needs their own thread for
interaction) or when using Dask (next section).

.. code-block:: python

   from streamz import Stream
   from tornado.ioloop import IOLoop
   from threading import Thread
   loop = IOLoop()
   thread = Thread(target=loop.start, daemon=True)
   thread.start()

   source = Stream()
   source.map(increment).rate_limit(0.500, loop=loop).sink(write)

   for x in range(10):
       source.emit(x)

In this case we start the IOLoop running in a separate thread.  We had to tell
``rate_limit`` which IOLoop to use explicitly by passing our IOLoop in the
``loop=`` keyword.  We call ``source.emit`` without using ``yield``.  The emit
call now blocks, waiting on a coroutine to finish within the IOLoop.

All functions here happen on the IOLoop.  This is good for consistency, but can
cause other concurrent applications to become unresponsive if your functions
(like ``increment``) block for long periods of time.  You might address this by
using Dask (see below) which will offload these computations onto separate
threads or processes.


Using Dask
----------

Dask_ is a parellel computing library that uses Tornado for concurrency and
threads for computation.  The ``DaskStream`` object is a drop-in replacement
for ``Stream`` (mostly).  We need to create a Dask client.  This will start a
thread and IOLoop for us.

.. code-block:: python

   from dask.distributed import Client
   client = Client(processes=False)  # starts thread pool, IOLoop in seaprate thread

   from streamz.dask import DaskStream
   source = DaskStream()  # connects to default client created above
   source.map(increment).rate_limit(0.500).gather().sink(write)

   for x in range(10):
       source.emit(x)

This operates very much like the synchronous case in terms of coding style (no
``@gen.coroutine`` or ``yield``) but does computations on separate threads.
This also provies parallelism and access to a dashboard at
http://localhost:8787/status .


Asynchronous Dask
-----------------

Dask can also operate within an event loop if preferred.  Here you can get the
non-blocking operation within an event loop while also offloading computations
to separate threads.

.. code-block:: python

   from streamz.dask import DaskStream
   from dask.distributed import Client
   from tornado import gen
   from tornado.ioloop import IOLoop

   @gen.coroutine
   def f():
       client = yield Client(processes=False, asynchronous=True)
       source = DaskStream(asynchronous=True)
       source.map(increment).rate_limit(0.500).gather().sink(write)

       for x in range(10):
           yield source.emit(x)

   IOLoop.current().add_callback(f)
   IOLoop.current().start()


AsyncIO
-------

Tornado works well with AsyncIO (see `Tornado-AsyncIO bridge docs
<http://www.tornadoweb.org/en/stable/asyncio.html>`_).  You will have to
install the AsyncIO event loop as the Tornado event loop.

.. code-block:: python

   from streamz import Stream
   from tornado.platform.asyncio import AsyncIOMainLoop
   AsyncIOMainLoop().install()

   @gen.coroutine
   def f():
       source = Stream(asynchronous=True)  # tell the stream we're working asynchronously
       source.map(increment).rate_limit(0.500).sink(write)

       for x in range(10):
           yield source.emit(x)

   f()

   import asyncio
   asyncio.get_event_loop().run_forever()



.. _Tornado: http://www.tornadoweb.org/en/stable/
.. _Dask: https://dask.pydata.org/en/latest/
