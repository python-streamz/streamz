from __future__ import absolute_import, division, print_function

from .core import *
from .graph import *
from .sources import *
from .sinks import *
from .plugins import load_plugins

load_plugins(Stream)

try:
    from .dask import DaskStream, scatter
except ImportError:
    pass

__version__ = '0.6.4'
