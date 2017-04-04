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
from streams import Stream, map, scan

source = Stream()
a = map(inc, source)
b = scan(add, a, start=0)

L = []
Sink(b, L.append)
Sink(b, print)
```

Now as you feed data into the source all of the operations trigger as necessary


```python
for i in range(3):
    yield source.emit(i)
1
3
7

>>> L
[1, 3, 7]
```

Everything can have multiple subscribers.  Everything responds to
backpressure, meaning that if the sinks can't keep up then pushing in at the
source will slow down.
