def is_dataframe_like(df):
    """ Looks like a Pandas DataFrame. ** Borrowed from dask.dataframe.utils ** """
    typ = type(df)
    return (all(hasattr(typ, name)
                for name in ('groupby', 'head', 'merge', 'mean')) and
            all(hasattr(df, name) for name in ('dtypes',)) and not
            any(hasattr(typ, name)
                for name in ('value_counts', 'dtype')))


def is_series_like(s):
    """ Looks like a Pandas Series. ** Borrowed from dask.dataframe.utils ** """
    typ = type(s)
    return (all(hasattr(typ, name) for name in ('groupby', 'head', 'mean')) and
            all(hasattr(s, name) for name in ('dtype', 'name')) and
            'index' not in typ.__name__.lower())


def is_index_like(s):
    """ Looks like a Pandas Index. ** Borrowed from dask.dataframe.utils ** """
    typ = type(s)
    return (all(hasattr(s, name) for name in ('name', 'dtype'))
            and 'index' in typ.__name__.lower())


def is_frame_like(frame, method, example=None):
    """Handles type check for input example for DataFrame/Series/Index initialization"""
    if example is None:
        raise TypeError("Missing required argument:'example'")
    if not method(example):
        msg = "Streaming {0} expects an example of {0} like objects. Got: {1}."\
                                             .format(frame.__class__.__name__, example)
        raise TypeError(msg)
    frame._subtype = type(example)
