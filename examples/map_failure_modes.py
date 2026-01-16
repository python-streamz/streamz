import asyncio
from itertools import count
from streamz import Stream


async def flaky_async(x, from_where):
    return flaky_sync(x, from_where)


def flaky_sync(x, from_where):
    if x % 5 == 4:
        raise ValueError(f"I flaked out on {from_where}")
    return x


def make_counter(name):
    return Stream.from_iterable(count(), asynchronous=True, stream_name=name)


async def main():
    async_non_stop_source = make_counter("async not stopping")
    s_async = async_non_stop_source.map_async(flaky_async, async_non_stop_source)
    s_async.rate_limit("500ms").sink(print, async_non_stop_source.name)

    sync_source = make_counter("sync")
    s_sync = sync_source.map(flaky_sync, sync_source)
    s_sync.rate_limit("500ms").sink(print, sync_source.name)

    async_stopping_source = make_counter("async stopping")
    s_async = async_stopping_source.map_async(flaky_async, async_stopping_source, stop_on_exception=True)
    s_async.rate_limit("500ms").sink(print, async_stopping_source.name)

    async_non_stop_source.start()
    sync_source.start()
    async_stopping_source.start()
    print(f"{async_non_stop_source.started=}, {sync_source.started=}, {async_stopping_source.started=}")
    await asyncio.sleep(3)
    print(f"{async_non_stop_source.stopped=}, {sync_source.stopped=}, {async_stopping_source.stopped=}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
