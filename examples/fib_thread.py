from threading import Thread
from streamz import Stream
from tornado.ioloop import IOLoop

loop = IOLoop()

source = Stream()
s = source.sliding_window(2).map(sum)
L = s.sink_to_list()                    # store result in a list

s.rate_limit(0.5, loop=loop).sink(source.emit)         # pipe output back to input
s.rate_limit(1.0, loop=loop).sink(lambda x: print(L))  # print state of L every second

source.emit(0)                          # seed with initial values
source.emit(1)

thread = Thread(target=loop.start, daemon=True)
thread.start()


# main thread is still free
# so we can operate even while the event loop cycles
import time
time.sleep(1)
source.emit(100)  # inject new data to fibonacci sequence

time.sleep(3)  # wait more time before closing
