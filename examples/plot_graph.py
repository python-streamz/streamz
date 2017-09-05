from streamz import Stream
from operator import add

source1 = Stream(stream_name='source1')
source2 = Stream(stream_name='source2')
source3 = Stream(stream_name='awesome source')

n1 = source1.zip(source2)
n2 = n1.map(add)
n3 = n2.zip(source3)
L = n3.sink_to_list()

n2.visualize()
