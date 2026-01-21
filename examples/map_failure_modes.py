import asyncio
import sys
from itertools import count
from streamz import Stream


async def flaky_async(x, from_where):
    return flaky_sync(x, from_where)


def flaky_sync(x, from_where):
    if x % 5 == 4:
        raise ValueError(f"I flaked out on {x} for {from_where}")
    return x


def make_counter(name):
    return Stream.from_iterable(count(), asynchronous=True, stream_name=name)


async def main(run_flags):
    async_non_stop_source = make_counter("async not stopping")
    s_async = async_non_stop_source.rate_limit("500ms").map_async(flaky_async, async_non_stop_source)
    s_async.sink(print, async_non_stop_source.name)

    sync_source = make_counter("sync")
    s_sync = sync_source.rate_limit("500ms").map(flaky_sync, sync_source)
    s_sync.sink(print, sync_source.name)

    async_stopping_source = make_counter("async stopping")
    s_async_stop = async_stopping_source.rate_limit("500ms").map_async(flaky_async, async_stopping_source, stop_on_exception=True)
    s_async_stop.sink(print, async_stopping_source.name)

    if run_flags[0]:
        async_non_stop_source.start()
    if run_flags[1]:
        sync_source.start()
    if run_flags[2]:
        async_stopping_source.start()

    print(f"{async_non_stop_source.started=}, {sync_source.started=}, {async_stopping_source.started=}")
    await asyncio.sleep(3)
    print(f"{async_non_stop_source.stopped=}, {sync_source.stopped=}, {async_stopping_source.stopped=}")

    if run_flags[2]:
        print()
        print(f"Restarting {async_stopping_source}")
        async_stopping_source.start()
        print()
        await asyncio.sleep(2)
        print(f"{async_non_stop_source.stopped=}, {sync_source.stopped=}, {async_stopping_source.stopped=}")


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            flags = [char == "T" for char in sys.argv[1]]
        else:
            flags = [True, True, True]
        asyncio.run(main(flags))
    except KeyboardInterrupt:
        pass
