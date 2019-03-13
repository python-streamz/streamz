from .collection import Streaming, _stream_types
import toolz
import toolz.curried


class Batch(Streaming):
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
        super(Batch, self).__init__(stream=stream, example=example)

    def sum(self):
        """ Sum elements """
        return self.accumulate_partitions(_accumulate_sum, start=0)

    def filter(self, predicate):
        """ Filter elements by a predicate """
        return self.map_partitions(_filter, self, predicate)

    def pluck(self, ind):
        """ Pick a field out of all elements

        Example
        -------
        >>> s.pluck('name').sink(print)  # doctest: +SKIP
        >>> s.emit({'name': 'Alice', 'x': 123})  # doctest: +SKIP
        'Alice'
        """
        return self.map_partitions(_pluck, self, ind)

    def map(self, func, **kwargs):
        """ Map a function across all elements """
        return self.map_partitions(_map_map, self, func, **kwargs)

    def to_dataframe(self):
        """
        Convert to a streaming dataframe

        This calls ``pd.DataFrame`` on all list-elements of this stream
        """
        import pandas as pd
        import streamz.dataframe  # noqa: F401
        return self.map_partitions(pd.DataFrame, self)

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

_stream_types['streaming'].append(((list, tuple, set), Batch))
