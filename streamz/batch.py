from .collection import Streaming, _subtypes
import toolz
import toolz.curried


class StreamingBatch(Streaming):
    """ A Stream of tuples or lists

    This streaming collection manages batches of Python objects such as lists
    of text or dictionaries.  By batching many elements together we reduce
    overhead from Python.

    This library is typically used at the early stages of data ingestion before
    handing off to streaming dataframes

    Examples
    --------
    >>> text = Streaming.from_file(myfile)  # doctest: +SKIP
    >>> b = text.partition(100).map(json.loads)  # doctest: +SKIP
    """
    def __init__(self, stream=None, example=None):
        if example is None:
            example = []
        super(StreamingBatch, self).__init__(stream=stream, example=example)

    def sum(self):
        """ Sum elements """
        return self.accumulate_partitions(_accumulate_sum, start=0)

    def filter(self, predicate):
        """ Filter elements by a predicate """
        return self.map_partitions(_filter, predicate)

    def pluck(self, ind):
        """ Pick a field out of all elements

        Example
        -------
        >>> s.pluck('name').sink(print)  # doctest: +SKIP
        >>> s.emit({'name': 'Alice', 'x': 123})  # doctest: +SKIP
        'Alice'
        """
        return self.map_partitions(_pluck, ind)

    def map(self, func, **kwargs):
        """ Map a function across all elements """
        return self.map_partitions(_map_map, func, **kwargs)

    def to_dataframe(self):
        """
        Convert to a streaming dataframe

        This calls ``pd.DataFrame`` on all list-elements of this stream
        """
        import pandas as pd
        import streamz.dataframe  # flake8: noqa
        return self.map_partitions(pd.DataFrame)

    def to_stream(self):
        """ Concatenate batches and return base Stream

        Returned stream will be composed of single elements
        """
        return self.stream.flatten()


def _filter(seq, predicate):
    return list(filter(predicate, seq))


def _pluck(seq, ind):
    return list(toolz.pluck(ind, seq))


def _map_map(seq, func, **kwargs):
    return list(map(func, seq, **kwargs))


def _accumulate_sum(accumulator, new):
    return accumulator + sum(new)


map_type = type(map(lambda x: x, []))

_subtypes.append(((list, tuple, set), StreamingBatch))
