Streams
=======

Do not use.  This is an exploratory project to learn more about what it takes
to build typical real time systems.

Other projects you should probably consider instead:

1.  [RxPY](https://github.com/ReactiveX/RxPY)
2.  [Flink](https://flink.apache.org/)


Basic Explanation
-----------------

This implements a trivial subset of an API commonly found in streaming data
systems.  You can have a Stream and can create various other streams from that
stream using common modifiers like map, filter, scan, etc..  Eventually you
create a sink that consumes results.

```python
from streams import Stream

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

Additionally everything responds to backpressure, so if the sink blocks the
source will block (although you can add in buffers if desired).  Additionally
everything supports asynchronous workloads with Tornado coroutines, so you can
do async/await stuff if you prefer (or gen.coroutine/yield in Python 2).

```python
async def f(result):
    ... do non-blocking stuff with result ...

stream.sink(f)

for i in range(10):
    await source.emit(i)
```

This means that if the sinks can't keep up then the sources will stop pushing
data into the system.  This is useful to control buildup.


Dask
----

Everything above runs with normal Python in the main thread or optionally in a
Tornado event loop.  Alternatively this library plays well with Dask.  You can
scatter data to the cluster, map and scan things up there, gather back, etc..

```python
source.to_dask().scatter().map(func).scan(func).gather().sink(...)
```

Less Trivial Example
--------------------

```python
source = Source()
output = open('out')

s = source.map(json.loads)        # Parse JSON data
          .timed_window(0.050)    # Batch into 50ms batches
          .filter(len)            # Remove any batches that didn't have data
          .to_dask().scatter()    # Send to cluster
          .map(pd.DataFrame)      # Convert to pandas dataframes on the cluster
          .map(pd.DataFrame.sum)  # Sum rows of each batch
          .scan(add)              # Maintain running sum of all data
          .gather()               # Collect results back to local machine
          .map(str)               # Convert to string
          .sink(output.write)     # Write to file

from kafka_library import KafkaReader

topic = KafkaReader('data')

while True:
    for line in topic:
        source.emit(line)
```
