from streamz import Stream
from tornado.ioloop import IOLoop


source = Stream(asynchronous=True)
s = source.sliding_window(2).map(sum)
L = s.sink_to_list()                         # store result in a list

s.rate_limit('500ms').sink(source.emit)      # pipe output back to input
s.rate_limit('1s').sink(lambda x: print(L))  # print state of L every second

source.emit(1)                               # seed with initial values, does not block thread due to Future return

try:
    IOLoop.current().start()
except KeyboardInterrupt:
    pass
