Streams
=======

Do not use.  This is an exploratory project to learn more about what it takes
to build typical real time systems.

Other projects you should probably consider instead:

1.  [RxPY](https://github.com/ReactiveX/RxPY)
2.  [Flink](https://flink.apache.org/)
3.  [Beam](https://beam.apache.org/get-started/quickstart-py/)


Basic Explanation
-----------------

This implements a trivial subset of an API commonly found in streaming data
systems.  You can have a Stream and can create various other streams from that
stream using common modifiers like map, filter, scan, etc..  Eventually you
create a sink that consumes results.

```python
from dask_streams import Stream

source = Stream()

L = []
stream = source.map(inc).scan(add)
stream.sink(L.append)
stream.sink(print)
```

Now as you feed data into the source all of the operations trigger as necessary


```python
>>> for i in range(3):
...     source.emit(i)
1
3
7

>>> L
[1, 3, 7]
```

You can use the typical map/filter/scan syntax.  Everything can have
multiple subscribers at any point in the stream.

Backpressure
------------

Additionally everything responds to backpressure, so if the sink blocks the
source will block (although you can add in buffers if desired).  Additionally
everything supports asynchronous workloads with Tornado coroutines, so you can
do async/await stuff if you prefer (or gen.coroutine/yield in Python 2).

```python
async def f(result):
    ... do non-blocking stuff with result ...

stream.sink(f)  # f might impose waits like while a database ingests results

for i in range(10):
    await source.emit(i)  # waiting at the sink is felt here at the source
```

This means that if the sinks can't keep up then the sources will stop pushing
data into the system.  This is useful to control buildup.


Recursion and Feedback
----------------------

By connecting sources to sinks you can create feedback loops.  Here is a tiny
web crawler:

```python
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
```

This was not an intentional feature of the system.  It just fell out from the
design.

Less Trivial Example
--------------------

```python
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
```


What's missing?
---------------

This is still a toy library.  It has never been used for anything.  So
presumably many things are wrong.  I've tried to build a simple system that can
grow if use cases arrive.  Here are some obvious things that are missing:

1.  A lot of API.  I recommend looking at the Rx or Flink APIs to get a sense
    of what people often need.
2.  Integration to collections like lists, numpy arrays, or pandas dataframes.
    For example we should be able to think about streams of lists of things.
    In this case `seq_stream.map(func)` would apply the function across every
    element in the constituent lists.
3.  Thinking about time.  It would be nice to be able to annotate elements with
    things like event and processing time, and have this information pass
    through operations like map
4.  Multi-stream operations like zip and joins
5.  More APIs for common endpoints like Kafka
