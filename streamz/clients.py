from collections import Sequence, MutableMapping
from concurrent.futures import ThreadPoolExecutor, Future
from functools import wraps

from distributed import default_client as dask_default_client
from tornado import gen

from .core import identity


FILL_COLOR_LOOKUP = {"dask": "cornflowerblue", "threads": "coral"}


def result_maybe(future_maybe):
    if isinstance(future_maybe, Future):
        return future_maybe.result()
    else:
        if isinstance(future_maybe, Sequence) and not isinstance(
            future_maybe, str
        ):
            aa = []
            for a in future_maybe:
                aa.append(result_maybe(a))
            if isinstance(future_maybe, tuple):
                aa = tuple(aa)
            return aa
        elif isinstance(future_maybe, MutableMapping):
            for k, v in future_maybe.items():
                future_maybe[k] = result_maybe(v)
        return future_maybe


def delayed_execution(func):
    @wraps(func)
    def inner(*args, **kwargs):
        args = tuple([result_maybe(v) for v in args])
        kwargs = {k: result_maybe(v) for k, v in kwargs.items()}
        return func(*args, **kwargs)

    return inner


def executor_to_client(executor):
    executor._submit = executor.submit

    @wraps(executor.submit)
    def inner(fn, *args, **kwargs):
        wfn = delayed_execution(fn)
        return executor._submit(wfn, *args, **kwargs)

    executor.submit = inner

    @gen.coroutine
    def scatter(x, asynchronous=True):
        f = executor.submit(identity, x)
        return f

    executor.scatter = getattr(executor, "scatter", scatter)

    @gen.coroutine
    def gather(x, asynchronous=True):
        # If we have a sequence of futures await each one
        if isinstance(x, Sequence):
            final_result = []
            for sub_x in x:
                yx = yield sub_x
                final_result.append(yx)
            result = type(x)(final_result)
        else:
            result = yield x
        return result

    executor.gather = getattr(executor, "gather", gather)
    return executor


thread_ex_list = []


def thread_default_client():
    if thread_ex_list:
        ex = thread_ex_list[0]
        if ex._shutdown:
            thread_ex_list.pop()
            ex = executor_to_client(ThreadPoolExecutor())
            thread_ex_list.append(ex)
    else:
        ex = executor_to_client(ThreadPoolExecutor())
        thread_ex_list.append(ex)
    return ex


DEFAULT_BACKENDS = {
    "dask": dask_default_client,
    "thread": thread_default_client,
}
