from __future__ import division, print_function

import inspect
import sys


def is_dataframe_like(df):
    """ Looks like a Pandas DataFrame. ** Borrowed from dask.dataframe.utils ** """
    typ = type(df)
    return (
        all(hasattr(typ, name) for name in ("groupby", "head", "merge", "mean"))
        and all(hasattr(df, name) for name in ("dtypes", "columns"))
        and not any(hasattr(typ, name) for name in ("name", "dtype"))
    )


def is_series_like(s):
    """ Looks like a Pandas Series. ** Borrowed from dask.dataframe.utils ** """
    typ = type(s)
    return (
        all(hasattr(typ, name) for name in ("groupby", "head", "mean"))
        and all(hasattr(s, name) for name in ("dtype", "name"))
        and "index" not in typ.__name__.lower()
    )


def is_index_like(s):
    """ Looks like a Pandas Index. ** Borrowed from dask.dataframe.utils ** """
    typ = type(s)
    return (
        all(hasattr(s, name) for name in ("name", "dtype"))
        and "index" in typ.__name__.lower()
    )


def get_base_frame_type(frame_name, is_frame_like, example=None):
    """Handles type check for input example for DataFrame/Series/Index initialization.
       Returns the base type of streaming objects if type checks pass."""
    if example is None:
        raise TypeError("Missing required argument:'example'")
    if is_frame_like is is_dataframe_like and not is_frame_like(example):
        import pandas as pd
        example = pd.DataFrame(example)

    elif not is_frame_like(example):
        msg = "Streaming {0} expects an example of {0} like objects. Got: {1}."\
                                             .format(frame_name, example)
        raise TypeError(msg)
    return type(example)


def get_dataframe_package(df):
    """ Utility function to return the top level package (pandas/cudf)
     of DataFrame/Series/Index objects """
    module = inspect.getmodule(df)
    package, _, _ = module.__name__.partition('.')
    return sys.modules[package]
