Streamz
=======

This is a small library to manage continuous streams of data, particularly when
complex branching and control flow situations arise.

Motivation
----------

Python usually handles continuous sequences of data with iterators or generator
expressions.

.. code-block:: python

    def fib():
        a, b = 0, 1
        while True:
            yield a
            a, b = b, a + b

    x = fib()

    y = map(f, seq)

However iterators become challenging when you want to fork them or control the
flow of data.  Typically people rely on tools like ``itertools.tee``, and
``zip``.

.. code-block:: python

    x1, x2 = itertools.tee(x, 2)
    y1 = map(f, x1)
    y2 = map(g, x2)

However this quickly become cumbersome, especially when building complex
pipelines.


Summary
-------

Streams handles the complexity of pushing data through complex pipelines.  It
handles branching, side effects, backpressure, recursion, etc..  See the
:doc:`API <api>` for a list of functionality.


Basic Use
---------

You can construct a stream and push data through that stream:

.. code-block:: python

   from streamz import Stream

   source = Stream()  # Create stream

   source.emit(1)     # Push data through stream
   source.emit(2)
   source.emit(3)

By default this doesn't do anything.  We create new streams from this stream
using common modifiers like ``map``, ``filter``, and ``accumualte``:


.. code-block:: python

    def inc(x):
        return x + 1

    def add(total, x):
        return total + x

   stream = source.map(inc).accumulate(add)

Eventually we sink our results down to functions that have side effects like
printing, appending to a list, or writing to a file.

.. code-block:: python

   stream.sink(L.append)
   stream.sink(print)

Now when we feed data into the source all of the operations trigger as necessary

.. code-block:: python

   >>> for i in range(3):
   ...     source.emit(i)
   1
   3
   7

   >>> L
   [1, 3, 7]

This example used only a linear stream, but in practice we often branch off
from many points in the object.


Backpressure
------------

Additionally everything responds to backpressure, so if the sink blocks the
source will block (although you can add in buffers if desired).  Additionally
everything supports asynchronous workloads with Tornado coroutines, so you can
do async/await stuff if you prefer (or gen.coroutine/yield in Python 2).

.. code-block:: python

   async def f(result):
       ... do non-blocking stuff with result ...

   stream.sink(f)  # f might impose waits like while a database ingests results

   for i in range(10):
       await source.emit(i)  # waiting at the sink is felt here at the source

This means that if the sinks can't keep up then the sources will stop pushing
data into the system.  This is useful to control buildup.


Recursion and Feedback
----------------------

By connecting sources to sinks you can create feedback loops.  Here is a tiny
web crawler:

.. code-block:: python

   from streamz import Stream
   source = Stream()

   pages = source.unique()
   content = (pages.map(requests.get)
                   .map(lambda x: x.content))
   links = (content.map(get_list_of_links)
                   .concat())
   links.sink(source.emit)

   pages.sink(print)

   >>> source.emit('http://github.com')
   http://github.com
   http://github.com/features
   http://github.com/business
   http://github.com/explore
   http://github.com/pricing
   ...

This was not an intentional feature of the system.  It just fell out from the
design.

Less Trivial Example
--------------------

.. code-block:: python

   source = Source()
   output = open('out')

   s = source.map(json.loads)        # Parse lines of JSON data
             .timed_window(0.050)    # Collect data into into 50ms batches
             .filter(len)            # Remove any batches that didn't have data
             .map(pd.DataFrame)      # Convert to pandas dataframes
             .map(pd.DataFrame.sum)  # Sum rows of each batch
             .scan(add)              # Maintain running sum of all data
             .map(str)               # Convert to string
             .sink(output.write)     # Write to file

   from some_kafka_library import KafkaReader

   topic = KafkaReader().subscribe('data')

   while True:
       for line in topic:
           source.emit(line)


What's missing?
---------------

This library is still quite young and so is missing several desirable features.

1.  A lot of API.  I recommend looking at the Rx or Flink APIs to get a sense
    of what people often need.
2.  Integration to collections like lists, numpy arrays, or pandas dataframes.
3.  Thinking about time.  It would be nice to be able to annotate elements with
    things like event and processing time, and have this information pass
    through operations like map
4.  More multi-stream operations like zip and joins
5.  More APIs for common endpoints like Kafka


Related Work
------------

Streamz is similar to reactive
programming systems like `RxPY <https://github.com/ReactiveX/RxPY>`_ or big
data streaming systems like `Flink <https://flink.apache.org/>`_, `Beam
<https://beam.apache.org/get-started/quickstart-py/>`_ or `Spark Streaming
<https://beam.apache.org/get-started/quickstart-py/>`_.

Parallelism
-----------

There is a rudimentary Dask implementation in ``streamz.dask``.  This should be
considered alpha software.


.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Contents

   api.rst
