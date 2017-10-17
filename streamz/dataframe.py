from functools import partial
import operator
from time import time

import numpy as np
import pandas as pd
from tornado.ioloop import IOLoop
from tornado import gen

from .collection import Streaming, _subtypes, stream_type
from .sources import Source
from .utils import M


class StreamingFrame(Streaming):
    def sum(self):
        """ Sum frame """
        return self.accumulate_partitions(_accumulate_sum, start=0)

    def round(self, decimals=0):
        """ Round elements in frame """
        return self.map_partitions(M.round, decimals=decimals)

    def tail(self, n=5):
        """ Round elements in frame """
        return self.map_partitions(M.tail, n=n)

    def rolling(self, window, min_periods=1):
        """ Compute rolling aggregations

        When followed by an aggregation method like ``sum``, ``mean``, or
        ``std`` this produces a new Streaming dataframe whose values are
        aggregated over that window.

        The window parameter can be either a number of rows or a timedelta like
        ``"2 minutes"` in which case the index should be a datetime index.

        This operates by keeping enough of a backlog of records to maintain an
        accurate stream.  It performs a copy at every added dataframe.  Because
        of this it may be slow if the rolling window is much larger than the
        average stream element.

        Parameters
        ----------
        window: int or timedelta
            Window over which to roll

        Returns
        -------
        Rolling object
        """
        return Rolling(self, window, min_periods)

    def plot(self, backlog=1000, width=800, height=300, **kwargs):
        """ Plot streaming dataframe as Bokeh plot

        This is fragile.  It only works in the classic Jupyter Notebook.  It
        only works on numeric data.  It assumes that the index is a datetime
        index
        """
        from bokeh.palettes import Category10
        from bokeh.io import output_notebook, push_notebook, show
        from bokeh.models import value
        from bokeh.plotting import figure, ColumnDataSource
        output_notebook()

        sdf = self.to_frame()

        colors = Category10[max(3, min(10, len(sdf.columns)))]
        data = {c: [] for c in sdf.columns}
        data['index'] = []
        cds = ColumnDataSource(data)

        if ('x_axis_type' not in kwargs and
                np.issubdtype(self.index.dtype, np.datetime64)):
            kwargs['x_axis_type'] = 'datetime'

        fig = figure(width=width, height=height, **kwargs)

        for i, column in enumerate(sdf.columns):
            color = colors[i % len(colors)]
            fig.line(source=cds, x='index', y=column, color=color, legend=value(column))

        fig.legend.click_policy = 'hide'
        fig.min_border_left = 30
        fig.min_border_bottom = 30

        result = show(fig, notebook_handle=True)

        loop = IOLoop.current()

        def push_data(df):
            df = df.reset_index()
            d = {c: df[c] for c in df.columns}

            def _():
                cds.stream(d, backlog)
                push_notebook(handle=result)
            loop.add_callback(_)

        return {'figure': fig, 'cds': cds, 'stream': sdf.stream.gather().map(push_data)}

    @property
    def index(self):
        return self.map_partitions(lambda x: x.index)

    def _cumulative_aggregation(self, op):
        return self.accumulate_partitions(_cumulative_accumulator,
                                          returns_state=True,
                                          start=(),
                                          op=op)

    def cumsum(self):
        """ Cumulative sum """
        return self._cumulative_aggregation(op='cumsum')

    def cumprod(self):
        """ Cumulative product """
        return self._cumulative_aggregation(op='cumprod')

    def cummin(self):
        """ Cumulative minimum """
        return self._cumulative_aggregation(op='cummin')

    def cummax(self):
        """ Cumulative maximum """
        return self._cumulative_aggregation(op='cummax')


def _cumulative_accumulator(state, new, op=None):
    if not len(new):
        return state, new

    if not len(state):
        df = new
    else:
        df = pd.concat([state, new])  # ouch, full copy

    result = getattr(df, op)()
    new_state = result.iloc[-1:]
    if len(state):
        result = result[1:]
    return new_state, result


class Rolling(object):
    """ Rolling aggregations

    This intermediate class enables rolling aggregations across either a fixed
    number of rows or a time window.

    Examples
    --------
    >>> sdf.rolling(10).x.mean()  # doctest: +SKIP
    >>> sdf.rolling('100ms').x.mean()  # doctest: +SKIP
    """
    def __init__(self, sdf, window, min_periods):
        self.sdf = sdf
        if not isinstance(window, int):
            window = pd.Timedelta(window)
            min_periods = 1
        self.window = window
        self.min_periods = min_periods

    def __getitem__(self, key):
        sdf = self.sdf[key]
        return Rolling(sdf, self.window, self.min_periods)

    def __getattr__(self, key):
        if key in self.sdf.columns or not len(self.sdf.columns):
            return self[key]
        else:
            raise AttributeError("StreamingSeriesGroupby has no attribute %r" % key)

    def _known_aggregation(self, op, *args, **kwargs):
        return self.sdf.accumulate_partitions(rolling_accumulator,
                                              window=self.window,
                                              op=op,
                                              args=args,
                                              kwargs=kwargs,
                                              start=(),
                                              returns_state=True)

    def sum(self):
        """ Rolling sum """
        return self._known_aggregation('sum')

    def mean(self):
        """ Rolling mean """
        return self._known_aggregation('mean')

    def min(self):
        """ Rolling minimum """
        return self._known_aggregation('min')

    def max(self):
        """ Rolling maximum """
        return self._known_aggregation('max')

    def median(self):
        """ Rolling median """
        return self._known_aggregation('median')

    def std(self, *args, **kwargs):
        """ Rolling standard deviation """
        return self._known_aggregation('std', *args, **kwargs)

    def var(self, *args, **kwargs):
        """ Rolling variance """
        return self._known_aggregation('var', *args, **kwargs)

    def count(self, *args, **kwargs):
        """ Rolling count """
        return self._known_aggregation('count', *args, **kwargs)

    def aggregate(self, *args, **kwargs):
        """ Rolling aggregation """
        return self._known_aggregation('aggregate', *args, **kwargs)

    def quantile(self, *args, **kwargs):
        """ Rolling quantile """
        return self._known_aggregation('quantile', *args, **kwargs)


def rolling_accumulator(acc, new, window=None, op=None, args=(), kwargs={}):
    if len(acc):
        df = pd.concat([acc, new])
    else:
        df = new
    result = getattr(df.rolling(window), op)(*args, **kwargs)
    if isinstance(window, int):
        new_acc = df.iloc[-window:]
    else:
        new_acc = df.loc[result.index.max() - window:]
    result = result.iloc[len(acc):]
    return new_acc, result


class StreamingDataFrame(StreamingFrame):
    """ A Streaming dataframe

    This is a logical collection over a stream of Pandas dataframes.
    Operations on this object will translate to the appropriate operations on
    the underlying Pandas dataframes.

    See Also
    --------
    streams.dataframe.StreamingSeries
    streams.sequence.StreamingSequence
    """
    _subtype = pd.DataFrame

    def __init__(self, *args, **kwargs):
        # {'x': sdf.x + 1, 'y': sdf.y - 1}
        if len(args) == 1 and not kwargs and isinstance(args[0], dict):
            def concat(tup, columns=None):
                result = pd.concat(tup, axis=1)
                result.columns = columns
                return result

            columns, values = zip(*args[0].items())
            stream = type(values[0].stream).zip(*[v.stream for v in values])
            stream = stream.map(concat, columns=list(columns))
            example = pd.DataFrame({k: getattr(v, 'example', v)
                                    for k, v in args[0].items()})
            StreamingDataFrame.__init__(self, stream, example)
        else:
            return super(StreamingDataFrame, self).__init__(*args, **kwargs)

    @property
    def columns(self):
        return self.example.columns

    @property
    def dtypes(self):
        return self.example.dtypes

    def __getitem__(self, index):
        return self.map_partitions(operator.getitem, index)

    def __getattr__(self, key):
        if key in self.columns or not len(self.columns):
            return self.map_partitions(getattr, key)
        else:
            raise AttributeError("StreamingDataFrame has no attribute %r" % key)

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(self.__dict__)
        o.update(c for c in self.columns if
                 (isinstance(c, pd.compat.string_types) and
                 pd.compat.isidentifier(c)))
        return list(o)

    def verify(self, x):
        """ Verify consistency of elements that pass through this stream """
        super(StreamingDataFrame, self).verify(x)
        if list(x.columns) != list(self.example.columns):
            raise IndexError("Input expected to have columns %s, got %s" %
                             (self.example.columns, x.columns))

    def mean(self):
        """ Average """
        start = pd.DataFrame({'sums': 0, 'counts': 0},
                             index=self.example.columns)
        return self.accumulate_partitions(_accumulate_mean, start=start,
                                          returns_state=True)

    def groupby(self, other):
        """ Groupby aggreagtions """
        return StreamingSeriesGroupby(self, other)

    def assign(self, **kwargs):
        """ Assign new columns to this dataframe

        Alternatively use setitem syntax

        Examples
        --------
        >>> sdf = sdf.assign(z=sdf.x + sdf.y)  # doctest: +SKIP
        >>> sdf['z'] = sdf.x + sdf.y  # doctest: +SKIP
        """
        def concat(tup, columns=None):
            result = pd.concat(tup, axis=1)
            result.columns = columns
            return result
        columns, values = zip(*kwargs.items())
        stream = self.stream.zip(*[v.stream for v in values])
        stream = stream.map(concat, columns=list(self.columns) + list(columns))
        example = self.example.assign(**{c: v.example for c, v in kwargs.items()})
        return StreamingDataFrame(stream, example)

    def to_frame(self):
        """ Convert to a streaming dataframe """
        return self

    def __setitem__(self, key, value):
        if isinstance(value, StreamingSeries):
            result = self.assign(**{key: value})
        elif isinstance(value, StreamingDataFrame):
            result = self.assign(**{k: value[c] for k, c in zip(key, value.columns)})
        else:
            example = self.example.copy()
            example[key] = value
            result = self.map_partitions(pd.DataFrame.assign, **{key: value})

        self.stream = result.stream
        self.example = result.example
        return self


class StreamingSeries(StreamingFrame):
    """ A Streaming series

    This is a logical collection over a stream of Pandas series objects.
    Operations on this object will translate to the appropriate operations on
    the underlying Pandas series.

    See Also
    --------
    streams.dataframe.StreamingDataFrame
    streams.sequence.StreamingSequence
    """
    _subtype = pd.Series

    @property
    def dtype(self):
        return self.example.dtype

    def mean(self):
        """ Average """
        start = pd.Series({'sums': 0, 'counts': 0})
        return self.accumulate_partitions(_accumulate_mean, start=start,
                                          returns_state=True)

    def to_frame(self):
        """ Convert to a streaming dataframe """
        return self.map_partitions(M.to_frame)


class StreamingIndex(StreamingSeries):
    _subtype = pd.Index


def _accumulate_mean(accumulator, new):
    accumulator = accumulator.copy()
    accumulator['sums'] += new.sum()
    accumulator['counts'] += new.count()
    result = accumulator['sums'] / accumulator['counts']
    return accumulator, result


def _accumulate_sum(accumulator, new):
    return accumulator + new.sum()


def _accumulate_size(accumulator, new):
    return accumulator + new.size()


class StreamingSeriesGroupby(object):
    def __init__(self, root, grouper, index=None):
        self.root = root
        self.grouper = grouper
        self.index = index

    def __getitem__(self, index):
        return StreamingSeriesGroupby(self.root, self.grouper, index)

    def __getattr__(self, key):
        if key in self.root.columns or not len(self.root.columns):
            return self[key]
        else:
            raise AttributeError("StreamingSeriesGroupby has no attribute %r" % key)

    def sum(self):
        func = _accumulate_groupby_sum
        start = 0
        if isinstance(self.grouper, Streaming):
            func = partial(func, index=self.index)
            example = self.root.example.groupby(self.grouper.example)
            if self.index is not None:
                example = example[self.index]
            example = example.sum()
            stream = self.root.stream.zip(self.grouper.stream)
            stream = stream.accumulate(func, start=start)
        else:
            func = partial(func, grouper=self.grouper, index=self.index)
            example = self.root.example.groupby(self.grouper)
            if self.index is not None:
                example = example[self.index]
            example = example.sum()
            stream = self.root.stream.accumulate(func, start=start)
        return stream_type(example)(stream, example)

    def mean(self):
        # TODO, there is a lot of copy-paste with the code above
        # TODO, we should probably define groupby.aggregate
        func = _accumulate_groupby_mean
        start = (0, 0)
        if isinstance(self.grouper, Streaming):
            func = partial(func, index=self.index)
            example = self.root.example.groupby(self.grouper.example)
            if self.index is not None:
                example = example[self.index]
            example = example.mean()
            stream = self.root.stream.zip(self.grouper.stream)
            stream = stream.accumulate(func, start=start, returns_state=True)
        else:
            func = partial(func, grouper=self.grouper, index=self.index)
            example = self.root.example.groupby(self.grouper)
            if self.index is not None:
                example = example[self.index]
            example = example.mean()
            stream = self.root.stream.accumulate(func, start=start,
                                                 returns_state=True)
        return stream_type(example)(stream, example)


def _accumulate_groupby_sum(accumulator, new, grouper=None, index=None):
    if isinstance(new, tuple):  # zipped
        assert grouper is None
        new, grouper = new
    g = new.groupby(grouper)
    if index is not None:
        g = g[index]
    if isinstance(accumulator, int):
        return g.sum()
    else:
        return accumulator.add(g.sum(), fill_value=0)


def _accumulate_groupby_mean(accumulator, new, grouper=None, index=None):
    if isinstance(new, tuple):  # zipped
        assert grouper is None
        new, grouper = new
    g = new.groupby(grouper)
    if index is not None:
        g = g[index]

    (sums, counts) = accumulator
    if isinstance(sums, int):  # first time
        sums = g.sum()
        counts = g.count()
    else:
        sums = sums.add(g.sum(), fill_value=0)
        counts = counts.add(g.count(), fill_value=0)
    return (sums, counts), sums / counts


def _random_df(tup):
    last, now, freq = tup
    index = pd.DatetimeIndex(start=(last + freq.total_seconds()) * 1e9,
                             end=now * 1e9,
                             freq=freq)

    df = pd.DataFrame({'x': np.random.random(len(index)),
                       'y': np.random.poisson(size=len(index)),
                       'z': np.random.normal(0, 1, size=len(index))},
                       index=index)
    return df


class Random(StreamingDataFrame):
    """ A streaming dataframe of random data

    The x column is uniformly distributed.
    The y column is poisson distributed.
    The z column is normally distributed.

    This class is experimental and will likely be removed in the future

    Parameters
    ----------
    freq: timedelta
        The time interval between records
    interval: timedelta
        The time interval between new dataframes, should be significantly
        larger than freq

    Example
    -------
    >>> source = Random(freq='100ms', interval='1s')  # doctest: +SKIP
    """
    def __init__(self, freq='100ms', interval='500ms', dask=False):
        if dask:
            from streamz.dask import DaskStream
            source = DaskStream()
            loop = source.loop
        else:
            source = Source()
            loop = IOLoop.current()
        self.freq = pd.Timedelta(freq)
        self.interval = pd.Timedelta(interval).total_seconds()
        self.source = source
        self.continue_ = [True]

        stream = self.source.map(_random_df)
        example = _random_df((time(), time(), self.freq))

        super(Random, self).__init__(stream, example)

        loop.add_callback(self._cb, self.interval, self.freq, self.source,
                          self.continue_)

    def __del__(self):
        self.stop()

    def stop(self):
        self.continue_[0] = False

    @staticmethod
    @gen.coroutine
    def _cb(interval, freq, source, continue_):
        last = time()
        while continue_[0]:
            yield gen.sleep(interval)
            now = time()
            yield source._emit((last, now, freq))
            last = now


_subtypes.append((pd.DataFrame, StreamingDataFrame))
_subtypes.append((pd.Index, StreamingIndex))
_subtypes.append((pd.Series, StreamingSeries))
