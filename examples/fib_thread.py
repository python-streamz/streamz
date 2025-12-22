from streamz import Stream

source = Stream()
s = source.sliding_window(2).map(sum)
L = s.sink_to_list()                         # store result in a list

s.rate_limit('500ms').sink(source.emit)      # pipe output back to input
s.rate_limit('1s').sink(lambda x: print(L))  # print state of L every second

try:
    source.emit(1)                           # seed with initial values, blocks thread due to cycle in stream
except KeyboardInterrupt:
    pass
