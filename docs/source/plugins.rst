Plugins
=======

In addition to using ``@Stream.register_api()`` decorator, custom stream nodes can
be added to Streamz by installing 3rd-party Python packages.


Known plugins
-------------

Extras
++++++

These plugins are supported by the Streamz community and can be installed as extras,
e.g. ``pip install streamz[kafka]``.

There are no plugins here yet, but hopefully soon there will be.

.. only:: comment
    ================= ======================================================
    Extra name        Description
    ================= ======================================================
    ``files``         Advanced filesystem operations: listening for new
                    files in a directory, writing to multiple files etc.
    ``kafka``         Reading from and writing to Kafka topics.
    ================= ======================================================


Entry points
------------

Plugins register themselves with Streamz by using ``entry_points`` argument
in ``setup.py``:

.. code-block:: Python

    # setup.py

    from setuptools import setup

    setup(
        name="streamz_example_plugin",
        version="0.0.1",
        entry_points={
            "streamz.nodes": [
                "repeat = streamz_example_plugin:RepeatNode"
            ]
        }
    )

In this example, ``RepeatNode`` class will be imported from
``streamz_example_plugin`` package and will be available as ``Stream.repeat``.
In practice, class name and entry point name (the part before ``=`` in entry point
definition) are usually the same, but they `can` be different.

Different kinds of add-ons go into different entry point groups:

=========== ======================= =====================
 Node type   Required parent class   Entry point group
=========== ======================= =====================
 Source      ``streamz.Source``      ``streamz.sources``
 Node        ``streamz.Stream``      ``streamz.nodes``
 Sink        ``streamz.Sink``        ``streamz.sinks``
=========== ======================= =====================


Lazy loading
++++++++++++

Streamz will attach methods from existing plugins to the ``Stream`` class when it's
imported, but actual classes will be loaded only when the corresponding ``Stream``
method is first called. Streamz will also validate the loaded class before attaching it
and will raise an appropriate exception if validation fails.


Reference implementation
------------------------

Let's look at how stream nodes can be implemented.

.. code-block:: Python

    # __init__.py

    from tornado import gen
    from streamz import Stream


    class RepeatNode(Stream):

        def __init__(self, upstream, n, **kwargs):
            super().__init__(upstream, ensure_io_loop=True, **kwargs)
            self._n = n

        @gen.coroutine
        def update(self, x, who=None, metadata=None):
            for _ in range(self._n):
                yield self._emit(x, metadata=metadata)

As you can see, implementation is the same as usual, but there's no
``@Stream.register_api()`` — Streamz will take care of that when loading the plugin.
It will still work if you add the decorator, but you don't have to.
