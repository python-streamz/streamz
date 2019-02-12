from enum import Enum


def is_dataframe_like(df):
    """ Looks like a Pandas/cudf DataFrame """
    return set(dir(df)) > {'dtypes', 'columns', 'groupby', 'head'} and not isinstance(df, type)


def is_series_like(s):
    """ Looks like a Pandas/cudf Series """
    return set(dir(s)) > {'index', 'dtype', 'take', 'head'} and not isinstance(s, type)


def is_index_like(s):
    """ Looks like a Pandas/cudf Index """
    attrs = set(dir(s))
    return attrs > {'take', 'values'} and 'head' not in attrs and not isinstance(s, type)


class FrameType(Enum):
    DATAFRAME = 0
    SERIES = 1
    INDEX = 2


FrameType._types = [is_dataframe_like, is_series_like, is_index_like]


def is_frame_like(frame, method, example=None):
    """Handles type check for input example for DataFrame/Series/Index initialization"""
    if example is None:
        raise TypeError("Missing required argument:'example'")
    method = FrameType._types[method.value]
    if not method(example):
        msg = "Streaming {0} expects an example of {0} like objects. Got: {1}."\
                                             .format(frame.__class__.__name__, example)
        raise TypeError(msg)
    frame._subtype = type(example)
