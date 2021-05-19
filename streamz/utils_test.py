import asyncio
from contextlib import contextmanager
import logging
import os
import six
import shutil
import tempfile
from time import time, sleep

import pytest
from tornado import gen
from tornado.ioloop import IOLoop

from .core import _io_loops, Stream


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


@contextmanager
def captured_logger(logger, level=logging.INFO, propagate=None):
    """Capture output from the given Logger.
    """
    if isinstance(logger, str):
        logger = logging.getLogger(logger)
    orig_level = logger.level
    orig_handlers = logger.handlers[:]
    if propagate is not None:
        orig_propagate = logger.propagate
        logger.propagate = propagate
    sio = six.StringIO()
    logger.handlers[:] = [logging.StreamHandler(sio)]
    logger.setLevel(level)
    try:
        yield sio
    finally:
        logger.handlers[:] = orig_handlers
        logger.setLevel(orig_level)
        if propagate is not None:
            logger.propagate = orig_propagate


@pytest.fixture
def clean():
    for loop in _io_loops:
        loop.add_callback(loop.stop)

    del _io_loops[:]


def wait_for(predicate, timeout, fail_func=None, period=0.001):
    """Wait for predicate to turn true, or fail this test"""
    # from distributed.utils_test
    deadline = time() + timeout
    while not predicate():
        sleep(period)
        if time() > deadline:  # pragma: no cover
            if fail_func is not None:
                fail_func()
            pytest.fail("condition not reached within %s seconds" % timeout)


async def await_for(predicate, timeout, fail_func=None, period=0.001):
    deadline = time() + timeout
    while not predicate():
        await asyncio.sleep(period)
        if time() > deadline:  # pragma: no cover
            if fail_func is not None:
                fail_func()
            pytest.fail("condition not reached until %s seconds" % (timeout,))


class metadata(Stream):
    def update(self, x, who=None, metadata=None):
        if metadata:
            return self._emit(metadata)
