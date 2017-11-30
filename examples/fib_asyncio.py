from streamz import Stream
import asyncio
from tornado.platform.asyncio import AsyncIOMainLoop
AsyncIOMainLoop().install()


source = Stream()
s = source.sliding_window(2).map(sum)
L = s.sink_to_list()                    # store result in a list

s.rate_limit(0.5).sink(source.emit)         # pipe output back to input
s.timed_window(1.0).sink(lambda x: print(x))

source.emit(0)                          # seed with initial values
source.emit(1)


def run_asyncio_loop():
    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()

run_asyncio_loop()
