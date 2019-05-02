#! /usr/env python
""" a recreation of spark-streaming's network_wordcount

https://spark.apache.org/docs/2.2.0/streaming-programming-guide.html#a-quick-example
"""
import time
from streamz import Stream

# absolute port on localhost for now
s = Stream.from_tcp(9999)
s.map(bytes.split).flatten().frequencies().sink(print)

print(
    """In another terminal execute
> nc 127.0.0.1 9999
and then start typing content
"""
)

s.start()
time.sleep(600)
