from streamz import Stream
from tornado.ioloop import IOLoop


source = Stream(asynchronous=True)
s = source.sliding_window(2).map(sum)
L = s.sink_to_list()                    # store result in a list

s.rate_limit(0.5).sink(source.emit)         # pipe output back to input
s.rate_limit(1.0).sink(lambda x: print(L))  # print state of L every second

source.emit(0)                          # seed with initial values
source.emit(1)

IOLoop.current().start()
