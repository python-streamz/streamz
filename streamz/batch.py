from .collection import Streaming, _subtypes
import toolz
import toolz.curried


class StreamingBatch(Streaming):
    """ A Stream of batches

    This streaming collection manages batches of Python objects such as lists
    of text or dictionaries.  By batching many elements together we reduce
    overhead from Python.

    This library is typically used at the early stages of data ingestion before
    handing off to streaming dataframes

    Examples
    --------
    >>> text = Streaming.from_file(myfile)
    >>> sdf = text.map(json.loads).to_dataframe()
    """
    def __init__(self, stream=None, example=None):
        if example is None:
            example = []
        super(StreamingBatch, self).__init__(stream=stream, example=example)

    def sum(self):
        return self.accumulate_partitions(_accumulate_sum, start=0)

    def pluck(self, ind):
        return self.map_partitions(_pluck, ind)

    def map(self, func, **kwargs):
        return self.map_partitions(_map_map, func, **kwargs)

    def to_dataframe(self):
        import pandas as pd
        import streamz.dataframe  # flake8: noqa
        return self.map_partitions(pd.DataFrame)


def _pluck(seq, ind):
    return list(toolz.pluck(ind, seq))


def _map_map(seq, func, **kwargs):
    return list(map(func, seq, **kwargs))


def _accumulate_sum(accumulator, new):
    return accumulator + sum(new)


map_type = type(map(lambda x: x, []))

_subtypes.append(((list, tuple, set), StreamingBatch))
