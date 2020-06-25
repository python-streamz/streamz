Streamz
=======

|Build Status| |Doc Status| |Version Status| |RAPIDS custreamz gpuCI|

Streamz helps you build pipelines to manage continuous streams of data. It is simple to use in simple cases, but also supports complex pipelines that involve branching, joining, flow control, feedback, back pressure, and so on.

Optionally, Streamz can also work with both `Pandas <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_ and `cuDF <https://docs.rapids.ai/api/cudf/stable/>`_ dataframes, to provide sensible streaming operations on continuous tabular data.

To learn more about how to use Streamz see documentation at `streamz.readthedocs.org <https://streamz.readthedocs.org>`_.

LICENSE
-------

BSD-3 Clause

.. |Build Status| image:: https://travis-ci.org/python-streamz/streamz.svg?branch=master
   :target: https://travis-ci.org/python-streamz/streamz
.. |Doc Status| image:: http://readthedocs.org/projects/streamz/badge/?version=latest
   :target: http://streamz.readthedocs.org/en/latest/
   :alt: Documentation Status
.. |Version Status| image:: https://img.shields.io/pypi/v/streamz.svg
   :target: https://pypi.python.org/pypi/streamz/
.. |RAPIDS custreamz gpuCI| image:: https://img.shields.io/badge/gpuCI-custreamz-green
   :target: https://github.com/jdye64/cudf/blob/kratos/python/custreamz/custreamz/kafka.py
