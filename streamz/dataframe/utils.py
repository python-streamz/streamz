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