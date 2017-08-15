from contextlib import contextmanager
import os
import shutil
import tempfile

from tornado import gen
from tornado.ioloop import IOLoop


@contextmanager
def tmpfile(extension=''):
    extension = '.' + extension.lstrip('.')
    handle, filename = tempfile.mkstemp(extension)
    os.close(handle)
    os.remove(filename)

    yield filename

    if os.path.exists(filename):
        if os.path.isdir(filename):
            shutil.rmtree(filename)
        else:
            try:
                os.remove(filename)
            except OSError:  # sometimes we can't remove a generated temp file
                pass


def inc(x):
    return x + 1


def double(x):
    return 2 * x


@contextmanager
def pristine_loop():
    IOLoop.clear_instance()
    IOLoop.clear_current()
    loop = IOLoop()
    loop.make_current()
    try:
        yield loop
    finally:
        loop.close(all_fds=True)
        IOLoop.clear_instance()
        IOLoop.clear_current()


def gen_test(timeout=10):
    """ Coroutine test

    @gen_test(timeout=5)
    def test_foo():
        yield ...  # use tornado coroutines
    """
    def _(func):
        def test_func():
            with pristine_loop() as loop:
                cor = gen.coroutine(func)
                try:
                    loop.run_sync(cor, timeout=timeout)
                finally:
                    loop.stop()
        return test_func
    return _
