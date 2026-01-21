from streamz import Stream
from tornado.ioloop import IOLoop


source = Stream(asynchronous=True)
s = source.sliding_window(2).map(sum)
L = s.sink_to_list()                         # store result in a list

s.rate_limit('1s').sink(lambda x: print(L))  # print state of L every second
s.rate_limit('500ms').connect(source)        # pipe output back to input

source.emit(1)                               # seed with initial value, does not block thread due to Future return

try:
    IOLoop.current().start()
except KeyboardInterrupt:
    pass
