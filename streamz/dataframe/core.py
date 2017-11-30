from __future__ import division, print_function

from collections import OrderedDict
import operator
from time import time

import numpy as np
import pandas as pd
import toolz
from tornado.ioloop import IOLoop
from tornado import gen

from ..collection import Streaming, _stream_types, OperatorMixin
from ..sources import Source
from ..utils import M
from . import aggregations


class BaseFrame(Streaming):

    def round(self, decimals=0):
        """ Round elements in frame """
        return self.map_partitions(M.round, self, decimals=decimals)

    def reset_index(self):
        """ Reset Index """
        return self.map_partitions(M.reset_index, self)

    def set_index(self, index, **kwargs):
        """ Set Index """
        return self.map_partitions(M.set_index, self, index, **kwargs)

    def tail(self, n=5):
        """ Round elements in frame """
        return self.map_partitions(M.tail, self, n=n)

    def astype(self, dt):
        return self.map_partitions(M.astype, self, dt)

    @property
    def index(self):
        return self.map_partitions(lambda x: x.index, self)

    def map(self, func, na_action=None):
        return self.map_partitions(self._subtype.map, self, func, na_action=na_action)


class Frame(BaseFrame):
    _stream_type = 'streaming'

    def groupby(self, other):
        """ Groupby aggreagtions """
        return GroupBy(self, other)

    def sum(self):
        """ Sum frame """
        return self.accumulate_partitions(aggregations.accumulator,
                                          agg=aggregations.Sum(),
                                          start=None, stream_type='updating',
                                          returns_state=True)

    def count(self):
        """ Count of frame """
        return self.accumulate_partitions(aggregations.accumulator,
                                          agg=aggregations.Count(),
                                          start=None, stream_type='updating',
                                          returns_state=True)

    @property
    def size(self):
        """ size of frame """
        return self.accumulate_partitions(aggregations.accumulator,
                                          agg=aggregations.Size(),
                                          start=None, stream_type='updating',
                                          returns_state=True)

    def mean(self):
        """ Average """
        return self.accumulate_partitions(aggregations.accumulator,
                                          agg=aggregations.Mean(),
                                          start=None, stream_type='updating',
                                          returns_state=True)

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

        See Also
        --------
        DataFrame.window: more generic window operations
        """
        return Rolling(self, window, min_periods)

    def window(self, n=None, value=None):
        """ Sliding window operations

        Windowed operations are defined over a sliding window of data, either
        with a fixed number of elements::

            >>> df.window(n=10).sum()  # sum of the last ten elements

        or over an index value range (index must be monotonic)::

            >>> df.window(value='2h').mean()  # average over the last two hours

        Windowed dataframes support all normal arithmetic, aggregations, and
        groupby-aggregations.

        Examples
        --------
        >>> df.window(n=10).std()
        >>> df.window(value='2h').count()

        >>> w = df.window(n=100)
        >>> w.groupby(w.name).amount.sum()
        >>> w.groupby(w.x % 10).y.var()

        See Also
        --------
        DataFrame.rolling: mimic's Pandas rolling aggregations
        """
        return Window(self, n=n, value=value)

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


class Frames(BaseFrame):
    _stream_type = 'updating'

    def sum(self, **kwargs):
        return self.map_partitions(M.sum, self, **kwargs)

    def mean(self, **kwargs):
        return self.map_partitions(M.mean, self, **kwargs)

    def std(self, **kwargs):
        return self.map_partitions(M.std, self, **kwargs)

    def var(self, **kwargs):
        return self.map_partitions(M.var, self, **kwargs)

    @property
    def size(self, **kwargs):
        return self.map_partitions(M.size, self, **kwargs)

    def count(self, **kwargs):
        return self.map_partitions(M.count, self, **kwargs)

    def nlargest(self, n, *args, **kwargs):
        return self.map_partitions(M.nlargest, self, n, *args, **kwargs)

    def tail(self, n=5):
        """ Round elements in frame """
        return self.map_partitions(M.tail, self, n=n)


class _DataFrameMixin(object):
    @property
    def columns(self):
        return self.example.columns

    @property
    def dtypes(self):
        return self.example.dtypes

    def __getitem__(self, index):
        return self.map_partitions(operator.getitem, self, index)

    def __getattr__(self, key):
        if key in self.columns or not len(self.columns):
            return self.map_partitions(getattr, self, key)
        else:
            raise AttributeError("DataFrame has no attribute %r" % key)

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(self.__dict__)
        o.update(c for c in self.columns if
                 (isinstance(c, pd.compat.string_types) and
                 pd.compat.isidentifier(c)))
        return list(o)

    def assign(self, **kwargs):
        """ Assign new columns to this dataframe

        Alternatively use setitem syntax

        Examples
        --------
        >>> sdf = sdf.assign(z=sdf.x + sdf.y)  # doctest: +SKIP
        >>> sdf['z'] = sdf.x + sdf.y  # doctest: +SKIP
        """
        kvs = list(toolz.concat(kwargs.items()))

        def _assign(df, *kvs):
            keys = kvs[::2]
            values = kvs[1::2]
            kwargs = OrderedDict(zip(keys, values))
            return df.assign(**kwargs)

        return self.map_partitions(_assign, self, *kvs)

    def to_frame(self):
        """ Convert to a streaming dataframe """
        return self

    def __setitem__(self, key, value):
        if isinstance(value, Series):
            result = self.assign(**{key: value})
        elif isinstance(value, DataFrame):
            result = self.assign(**{k: value[c] for k, c in zip(key, value.columns)})
        else:
            example = self.example.copy()
            example[key] = value
            result = self.map_partitions(pd.DataFrame.assign, self, **{key: value})

        self.stream = result.stream
        self.example = result.example
        return self

    def query(self, expr, **kwargs):
        return self.map_partitions(pd.DataFrame.query, self, expr, **kwargs)


class DataFrame(Frame, _DataFrameMixin):
    """ A Streaming dataframe

    This is a logical collection over a stream of Pandas dataframes.
    Operations on this object will translate to the appropriate operations on
    the underlying Pandas dataframes.

    See Also
    --------
    Series
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
            DataFrame.__init__(self, stream, example)
        else:
            return super(DataFrame, self).__init__(*args, **kwargs)

    def verify(self, x):
        """ Verify consistency of elements that pass through this stream """
        super(DataFrame, self).verify(x)
        if list(x.columns) != list(self.example.columns):
            raise IndexError("Input expected to have columns %s, got %s" %
                             (self.example.columns, x.columns))


class _SeriesMixin(object):
    @property
    def dtype(self):
        return self.example.dtype

    def to_frame(self):
        """ Convert to a streaming dataframe """
        return self.map_partitions(M.to_frame, self)


class Series(Frame, _SeriesMixin):
    """ A Streaming series

    This is a logical collection over a stream of Pandas series objects.
    Operations on this object will translate to the appropriate operations on
    the underlying Pandas series.

    See Also
    --------
    DataFrame
    """
    _subtype = pd.Series

    def value_counts(self):
        return self.accumulate_partitions(aggregations.accumulator,
                                          agg=aggregations.ValueCounts(),
                                          start=None, stream_type='updating',
                                          returns_state=True)


class Index(Series):
    _subtype = pd.Index


class DataFrames(Frames, _DataFrameMixin):
    pass


class Seriess(Frames, _SeriesMixin):
    pass


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
        self.root = sdf
        if not isinstance(window, int):
            window = pd.Timedelta(window)
            min_periods = 1
        self.window = window
        self.min_periods = min_periods

    def __getitem__(self, key):
        sdf = self.root[key]
        return Rolling(sdf, self.window, self.min_periods)

    def __getattr__(self, key):
        if key in self.root.columns or not len(self.root.columns):
            return self[key]
        else:
            raise AttributeError("Rolling has no attribute %r" % key)

    def _known_aggregation(self, op, *args, **kwargs):
        return self.root.accumulate_partitions(rolling_accumulator,
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


class Window(OperatorMixin):
    """ Windowed aggregations

    This provides a set of aggregations that can be applied over a sliding
    window of data.

    See Also
    --------
    DataFrame.window: contains full docstring
    """
    def __init__(self, sdf, n=None, value=None):
        if value is None and isinstance(n, (str, pd.Timedelta)):
            value = n
            n = None
        self.n = n
        self.root = sdf
        if isinstance(value, str) and isinstance(self.root.example.index, pd.DatetimeIndex):
            value = pd.Timedelta(value)
        self.value = value

    def __getitem__(self, key):
        sdf = self.root[key]
        return Window(sdf, n=self.n, value=self.value)

    def __getattr__(self, key):
        if key in self.root.columns or not len(self.root.columns):
            return self[key]
        else:
            raise AttributeError("Window has no attribute %r" % key)

    def map_partitions(self, func, *args, **kwargs):
        args2 = [a.root if isinstance(a, Window) else a for a in args]
        root = self.root.map_partitions(func, *args2, **kwargs)
        return Window(root, n=self.n, value=self.value)

    @property
    def index(self):
        return self.map_partitions(lambda x: x.index, self)

    @property
    def columns(self):
        return self.root.columns

    @property
    def dtypes(self):
        return self.root.dtypes

    @property
    def example(self):
        return self.root.example

    def reset_index(self):
        return Window(self.root.reset_index(), n=self.n, value=self.value)

    def _known_aggregation(self, agg):
        if self.n is not None:
            diff = aggregations.diff_iloc
            window = self.n
        elif self.value is not None:
            diff = aggregations.diff_loc
            window = self.value
        return self.root.accumulate_partitions(aggregations.window_accumulator,
                                              diff=diff,
                                              window=window,
                                              agg=agg,
                                              start=None,
                                              returns_state=True,
                                              stream_type='updating')

    def full(self):
        return self._known_aggregation(aggregations.Full())

    def apply(self, func):
        """ Apply an arbitrary function over each window of data """
        result = self._known_aggregation(aggregations.Full())
        return result.map_partitions(func, result)

    def sum(self):
        """ Sum elements within window """
        return self._known_aggregation(aggregations.Sum())

    def count(self):
        """ Count elements within window """
        return self._known_aggregation(aggregations.Count())

    def mean(self):
        """ Average elements within window """
        return self._known_aggregation(aggregations.Mean())

    def var(self, ddof=1):
        """ Compute variance of elements within window """
        return self._known_aggregation(aggregations.Var(ddof=ddof))

    def std(self, ddof=1):
        """ Compute standard deviation of elements within window """
        return self.var(ddof=ddof) ** 0.5

    @property
    def size(self):
        """ Number of elements within window """
        return self._known_aggregation(aggregations.Size())

    def value_counts(self):
        """ Count groups of elements within window """
        return self._known_aggregation(aggregations.ValueCounts())

    def groupby(self, other):
        """ Groupby-aggregations within window """
        return WindowedGroupBy(self.root, other, None, self.n, self.value)


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


class GroupBy(object):
    """ Groupby aggregations on streaming dataframes """
    def __init__(self, root, grouper, index=None):
        self.root = root
        self.grouper = grouper
        self.index = index

    def __getitem__(self, index):
        return GroupBy(self.root, self.grouper, index)

    def __getattr__(self, key):
        if key in self.root.columns or not len(self.root.columns):
            return self[key]
        else:
            raise AttributeError("GroupBy has no attribute %r" % key)

    def _accumulate(self, Agg, **kwargs):
        stream_type = 'updating'

        if isinstance(self.grouper, Streaming):
            stream = self.root.stream.zip(self.grouper.stream)
            grouper_example = self.grouper.example
            agg = Agg(self.index, grouper=None, **kwargs)
        else:
            stream = self.root.stream
            grouper_example = self.grouper
            agg = Agg(self.index, grouper=self.grouper, **kwargs)

        # Compute example
        state = agg.initial(self.root.example, grouper=grouper_example)
        if hasattr(grouper_example, 'iloc'):
            grouper_example = grouper_example.iloc[:0]
        elif isinstance(grouper_example, (np.ndarray, pd.Index)):
            grouper_example = grouper_example[:0]
        _, example = agg.on_new(state,
                                self.root.example.iloc[:0],
                                grouper=grouper_example)

        outstream = stream.accumulate(aggregations.groupby_accumulator,
                                      agg=agg,
                                      start=None,
                                      returns_state=True)

        for typ, s_type in _stream_types[stream_type]:
            if isinstance(example, typ):
                return s_type(outstream, example)
        return Streaming(outstream, example, stream_type=stream_type)

    def count(self):
        """ Groupby-count """
        return self._accumulate(aggregations.GroupbyCount)

    def mean(self):
        """ Groupby-mean """
        return self._accumulate(aggregations.GroupbyMean)

    def size(self):
        """ Groupby-size """
        return self._accumulate(aggregations.GroupbySize)

    def std(self, ddof=1):
        """ Groupby-std """
        return self.var(ddof=ddof) ** 0.5

    def sum(self):
        """ Groupby-sum """
        return self._accumulate(aggregations.GroupbySum)

    def var(self, ddof=1):
        """ Groupby-variance """
        return self._accumulate(aggregations.GroupbyVar, ddof=ddof)


class WindowedGroupBy(GroupBy):
    """ Groupby aggregations over a window of data """
    def __init__(self, root, grouper, index=None, n=None, value=None):
        self.root = root
        self.grouper = grouper
        self.index = index
        self.n = n
        if isinstance(value, str) and isinstance(self.root.example.index, pd.DatetimeIndex):
            value = pd.Timedelta(value)
        self.value = value

    def __getitem__(self, index):
        return WindowedGroupBy(self.root, self.grouper, index, self.n, self.value)

    def _accumulate(self, Agg, **kwargs):
        stream_type = 'updating'

        if isinstance(self.grouper, Streaming):
            stream = self.root.stream.zip(self.grouper.stream)
            grouper_example = self.grouper.example
            agg = Agg(self.index, grouper=None, **kwargs)
        elif isinstance(self.grouper, Window):
            stream = self.root.stream.zip(self.grouper.root.stream)
            grouper_example = self.grouper.root.example
            agg = Agg(self.index, grouper=None, **kwargs)
        else:
            stream = self.root.stream
            grouper_example = self.grouper
            agg = Agg(self.index, grouper=self.grouper, **kwargs)

        # Compute example
        state = agg.initial(self.root.example, grouper=grouper_example)
        if hasattr(grouper_example, 'iloc'):
            grouper_example = grouper_example.iloc[:0]
        elif isinstance(grouper_example, (np.ndarray, pd.Index)):
            grouper_example = grouper_example[:0]
        _, example = agg.on_new(state,
                                self.root.example.iloc[:0],
                                grouper=grouper_example)

        if self.n is not None:
            diff = aggregations.diff_iloc
            window = self.n
        elif self.value is not None:
            diff = aggregations.diff_loc
            window = self.value

        outstream = stream.accumulate(aggregations.windowed_groupby_accumulator,
                                      agg=agg,
                                      start=None,
                                      returns_state=True,
                                      diff=diff,
                                      window=window)

        for typ, s_type in _stream_types[stream_type]:
            if isinstance(example, typ):
                return s_type(outstream, example)
        return Streaming(outstream, example, stream_type=stream_type)


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


class Random(DataFrame):
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


_stream_types['streaming'].append((pd.DataFrame, DataFrame))
_stream_types['streaming'].append((pd.Index, Index))
_stream_types['streaming'].append((pd.Series, Series))
_stream_types['updating'].append((pd.DataFrame, DataFrames))
_stream_types['updating'].append((pd.Series, Seriess))
