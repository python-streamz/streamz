import asyncio

import operator
from collections import OrderedDict
import numpy as np
import pandas as pd
import toolz

from ..collection import Streaming, _stream_types, OperatorMixin
from ..sources import Source
from ..utils import M
from . import aggregations
from .utils import is_dataframe_like, is_series_like, is_index_like, \
                    get_base_frame_type, get_dataframe_package


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
        """ Groupby aggregations """
        return GroupBy(self, other)

    def aggregate(self, aggregation, start=None):
        return self.accumulate_partitions(aggregations.accumulator,
                                          agg=aggregation,
                                          start=start, stream_type='updating',
                                          returns_state=True)

    def sum(self, start=None):
        """ Sum frame.

        Parameters
        ----------
        start: None or resulting Python object type from the operation
            Accepts a valid start state.
        """
        return self.aggregate(aggregations.Sum(), start)

    def count(self, start=None):
        """ Count of frame

        Parameters
        ----------
        start: None or resulting Python object type from the operation
            Accepts a valid start state.
        """
        return self.aggregate(aggregations.Count(), start)

    @property
    def size(self):
        """ size of frame """
        return self.aggregate(aggregations.Size())

    def mean(self, start=None):
        """ Average frame

        Parameters
        ----------
        start: None or resulting Python object type from the operation
            Accepts a valid start state.
        """
        return self.aggregate(aggregations.Mean(), start)

    def rolling(self, window, min_periods=1, with_state=False, start=()):
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
        with_state: bool (False)
            Whether to return the state along with the result as a tuple (state, result).
            State may be needed downstream for a number of reasons like checkpointing.
        start: () or resulting Python object type from the operation
            Accepts a valid start state.

        Returns
        -------
        Rolling object

        See Also
        --------
        DataFrame.window: more generic window operations
        """
        return Rolling(self, window, min_periods, with_state, start)

    def window(self, n=None, value=None, with_state=False, start=None):
        """ Sliding window operations

        Windowed operations are defined over a sliding window of data, either
        with a fixed number of elements::

            >>> df.window(n=10).sum()  # sum of the last ten elements

        or over an index value range (index must be monotonic)::

            >>> df.window(value='2h').mean()  # average over the last two hours

        Windowed dataframes support all normal arithmetic, aggregations, and
        groupby-aggregations.

        Parameters
        ----------
        n: int
            Window of number of elements over which to roll
        value: str
            Window of time over which to roll
        with_state: bool (False)
            Whether to return the state along with the result as a tuple (state, result).
            State may be needed downstream for a number of reasons like checkpointing.
        start: None or resulting Python object type from the operation
            Accepts a valid start state.

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
        return Window(self, n=n, value=value, with_state=with_state, start=start)

    def expanding(self, with_state=False, start=None):
        return Expanding(self, n=1, with_state=with_state, start=start)

    def ewm(self, com=None, span=None, halflife=None, alpha=None, with_state=False, start=None):
        return EWM(self, n=1, com=com, span=span, halflife=halflife, alpha=alpha, with_state=with_state, start=start)

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
        o.update(c for c in self.columns
                 if (isinstance(c, str) and c.isidentifier()))
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
            df_type = type(self.example)
            result = self.map_partitions(df_type.assign, self, **{key: value})

        self.stream = result.stream
        self.example = result.example
        return self

    def query(self, expr, **kwargs):
        df_type = type(self.example)
        return self.map_partitions(df_type.query, self, expr, **kwargs)


class DataFrame(Frame, _DataFrameMixin):
    """ A Streaming Dataframe

    This is a logical collection over a stream of Pandas dataframes.
    Operations on this object will translate to the appropriate operations on
    the underlying Pandas dataframes.

    See Also
    --------
    Series
    """

    def __init__(self, *args, **kwargs):
        # {'x': sdf.x + 1, 'y': sdf.y - 1}
        if len(args) == 1 and not kwargs and isinstance(args[0], dict):
            def concat(tup, module=None, columns=None):
                result = module.concat(tup, axis=1)
                result.columns = columns
                return result

            columns, values = zip(*args[0].items())
            base_frame_type = values[0]._subtype
            df_package = get_dataframe_package(base_frame_type)
            stream = type(values[0].stream).zip(*[v.stream for v in values])
            stream = stream.map(concat, module=df_package, columns=list(columns))
            example = df_package.DataFrame({k: getattr(v, 'example', v)
                                    for k, v in args[0].items()})
            DataFrame.__init__(self, stream, example)
        else:
            example = None
            if "example" in kwargs:
                example = kwargs.get('example')
            elif len(args) > 1:
                example = args[1]
            if callable(example):
                example = example()
                kwargs["example"] = example

            self._subtype = get_base_frame_type(self.__class__.__name__,
                                                is_dataframe_like, example)
            super(DataFrame, self).__init__(*args, **kwargs)

    def verify(self, x):
        """ Verify consistency of elements that pass through this stream """
        super(DataFrame, self).verify(x)
        if list(x.columns) != list(self.example.columns):
            raise IndexError("Input expected to have columns %s, got %s" %
                             (self.example.columns, x.columns))

    @property
    def plot(self):
        try:
            # import has side-effect of attaching .hvplot attribute
            import hvplot.streamz  # # noqa: F401
        except ImportError as err:  # pragma: no cover
            raise ImportError("Streamz dataframe plotting requires hvplot") from err
        return self.hvplot


class _SeriesMixin(object):
    @property
    def dtype(self):
        return self.example.dtype

    def to_frame(self):
        """ Convert to a streaming dataframe """
        return self.map_partitions(M.to_frame, self)


class Series(Frame, _SeriesMixin):
    """ A Streaming Series

    This is a logical collection over a stream of Pandas series objects.
    Operations on this object will translate to the appropriate operations on
    the underlying Pandas series.

    See Also
    --------
    DataFrame
    """

    def __init__(self, *args, **kwargs):
        example = None
        if "example" in kwargs:
            example = kwargs.get('example')
        elif len(args) > 1:
            example = args[1]
        if isinstance(self, Index):
            self._subtype = get_base_frame_type(self.__class__.__name__,
                                                is_index_like, example)
        else:
            self._subtype = get_base_frame_type(self.__class__.__name__,
                                                is_series_like, example)
        super(Series, self).__init__(*args, **kwargs)

    def value_counts(self):
        return self.accumulate_partitions(aggregations.accumulator,
                                          agg=aggregations.ValueCounts(),
                                          start=None, stream_type='updating',
                                          returns_state=True)


class Index(Series):
    pass


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
        df_package = get_dataframe_package(new)
        df = df_package.concat([state, new])  # ouch, full copy

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

    def __init__(self, sdf, window, min_periods, with_state, start):
        self.root = sdf
        if not isinstance(window, int):
            window = pd.Timedelta(window)
            min_periods = 1
        self.window = window
        self.min_periods = min_periods
        self.with_state = with_state
        self.start = start

    def __getitem__(self, key):
        sdf = self.root[key]
        return Rolling(sdf, self.window, self.min_periods, self.with_state, self.start)

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
                                               start=self.start,
                                               returns_state=True,
                                               with_state=self.with_state)

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

    def __init__(self, sdf, n=None, value=None, with_state=False, start=None):
        if value is None and isinstance(n, (str, pd.Timedelta)):
            value = n
            n = None
        self.n = n
        self.root = sdf
        if isinstance(value, str) and isinstance(self.root.example.index, pd.DatetimeIndex):
            value = pd.Timedelta(value)
        self.value = value
        self.with_state = with_state
        self.start = start

    def __getitem__(self, key):
        sdf = self.root[key]
        return type(self)(
            sdf,
            n=self.n,
            value=self.value,
            with_state=self.with_state,
            start=self.start
        )

    def __getattr__(self, key):
        if key in self.root.columns or not len(self.root.columns):
            return self[key]
        else:
            raise AttributeError(f"{type(self)} has no attribute {key}")

    def map_partitions(self, func, *args, **kwargs):
        args2 = [a.root if isinstance(a, type(self)) else a for a in args]
        root = self.root.map_partitions(func, *args2, **kwargs)
        return type(self)(
            root,
            n=self.n,
            value=self.value,
            with_state=self.with_state,
            start=self.start
        )

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
        return type(self)(self.root.reset_index(), n=self.n, value=self.value)

    def aggregate(self, agg):
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
                                               start=self.start,
                                               returns_state=True,
                                               stream_type='updating',
                                               with_state=self.with_state)

    def full(self):
        return self.aggregate(aggregations.Full())

    def apply(self, func):
        """ Apply an arbitrary function over each window of data """
        result = self.aggregate(aggregations.Full())
        return result.map_partitions(func, result)

    def sum(self):
        """ Sum elements within window """
        return self.aggregate(aggregations.Sum())

    def count(self):
        """ Count elements within window """
        return self.aggregate(aggregations.Count())

    def mean(self):
        """ Average elements within window """
        return self.aggregate(aggregations.Mean())

    def var(self, ddof=1):
        """ Compute variance of elements within window """
        return self.aggregate(aggregations.Var(ddof=ddof))

    def std(self, ddof=1):
        """ Compute standard deviation of elements within window """
        return self.var(ddof=ddof) ** 0.5

    @property
    def size(self):
        """ Number of elements within window """
        return self.aggregate(aggregations.Size())

    def value_counts(self):
        """ Count groups of elements within window """
        return self.aggregate(aggregations.ValueCounts())

    def groupby(self, other):
        """ Groupby-aggregations within window """
        return WindowedGroupBy(self.root, other, None, self.n, self.value,
                               self.with_state, self.start)


class Expanding(Window):

    def aggregate(self, agg):
        window = self.n
        diff = aggregations.diff_expanding
        return self.root.accumulate_partitions(aggregations.window_accumulator,
                                               diff=diff,
                                               window=window,
                                               agg=agg,
                                               start=self.start,
                                               returns_state=True,
                                               stream_type='updating',
                                               with_state=self.with_state)

    def groupby(self, other):
        raise NotImplementedError


class EWM(Expanding):

    def __init__(
            self,
            sdf,
            n=1,
            value=None,
            with_state=False,
            start=None,
            com=None,
            span=None,
            halflife=None,
            alpha=None
    ):
        super().__init__(sdf, n=n, value=value, with_state=with_state, start=start)
        self._com = self._get_com(com, span, halflife, alpha)
        self.com = com
        self.span = span
        self.alpha = alpha
        self.halflife = halflife

    def __getitem__(self, key):
        sdf = self.root[key]
        return type(self)(
            sdf,
            n=self.n,
            value=self.value,
            with_state=self.with_state,
            start=self.start,
            com=self.com,
            span=self.span,
            halflife=self.halflife,
            alpha=self.alpha
        )

    @staticmethod
    def _get_com(com, span, halflife, alpha):
        if sum(var is not None for var in (com, span, halflife, alpha)) > 1:
            raise ValueError("Can only provide one of `com`, `span`, `halflife`, `alpha`.")
        # Convert to center of mass; domain checks ensure 0 < alpha <= 1
        if com is not None:
            if com < 0:
                raise ValueError("com must satisfy: comass >= 0")
        elif span is not None:
            if span < 1:
                raise ValueError("span must satisfy: span >= 1")
            com = (span - 1) / 2
        elif halflife is not None:
            if halflife <= 0:
                raise ValueError("halflife must satisfy: halflife > 0")
            decay = 1 - np.exp(np.log(0.5) / halflife)
            com = 1 / decay - 1
        elif alpha is not None:
            if alpha <= 0 or alpha > 1:
                raise ValueError("alpha must satisfy: 0 < alpha <= 1")
            com = (1 - alpha) / alpha
        else:
            raise ValueError("Must pass one of com, span, halflife, or alpha")

        return float(com)

    def full(self):
        raise NotImplementedError

    def apply(self, func):
        """ Apply an arbitrary function over each window of data """
        raise NotImplementedError

    def sum(self):
        """ Sum elements within window """
        raise NotImplementedError

    def count(self):
        """ Count elements within window """
        raise NotImplementedError

    def mean(self):
        """ Average elements within window """
        return self.aggregate(aggregations.EWMean(self._com))

    def var(self, ddof=1):
        """ Compute variance of elements within window """
        raise NotImplementedError

    def std(self, ddof=1):
        """ Compute standard deviation of elements within window """
        raise NotImplementedError

    @property
    def size(self):
        """ Number of elements within window """
        raise NotImplementedError

    def value_counts(self):
        """ Count groups of elements within window """
        raise NotImplementedError


def rolling_accumulator(acc, new, window=None, op=None,
                        with_state=False, args=(), kwargs={}):
    if len(acc):
        df_package = get_dataframe_package(new)
        df = df_package.concat([acc, new])
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

    def _accumulate(self, Agg, with_state=False, start=None, **kwargs):
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
        elif isinstance(grouper_example, np.ndarray) or is_index_like(grouper_example):
            grouper_example = grouper_example[:0]
        _, example = agg.on_new(state,
                                self.root.example.iloc[:0],
                                grouper=grouper_example)

        outstream = stream.accumulate(aggregations.groupby_accumulator,
                                      agg=agg,
                                      start=start,
                                      returns_state=True,
                                      with_state=with_state)

        for fn, s_type in _stream_types[stream_type]:
            """Function checks if example is of a specific frame type"""
            if fn(example):
                return s_type(outstream, example)
        return Streaming(outstream, example, stream_type=stream_type)

    def count(self, start=None):
        """ Groupby-count

        Parameters
        ----------
        start: None or resulting Python object type from the operation
            Accepts a valid start state.
        """
        return self._accumulate(aggregations.GroupbyCount, start=start)

    def mean(self, with_state=False, start=None):
        """ Groupby-mean

        Parameters
        ----------
        start: None or resulting Python object type from the operation
            Accepts a valid start state.
        """
        return self._accumulate(aggregations.GroupbyMean, with_state=with_state, start=start)

    def size(self):
        """ Groupby-size """
        return self._accumulate(aggregations.GroupbySize)

    def std(self, ddof=1):
        """ Groupby-std """
        return self.var(ddof=ddof) ** 0.5

    def sum(self, start=None):
        """ Groupby-sum

        Parameters
        ----------
        start: None or resulting Python object type from the operation
            Accepts a valid start state.

        """
        return self._accumulate(aggregations.GroupbySum, start=start)

    def var(self, ddof=1):
        """ Groupby-variance """
        return self._accumulate(aggregations.GroupbyVar, ddof=ddof)


class WindowedGroupBy(GroupBy):
    """ Groupby aggregations over a window of data """

    def __init__(self, root, grouper, index=None, n=None, value=None, with_state=False, start=None):
        self.root = root
        self.grouper = grouper
        self.index = index
        self.n = n
        if isinstance(value, str) and isinstance(self.root.example.index, pd.DatetimeIndex):
            value = pd.Timedelta(value)
        self.value = value
        self.with_state = with_state
        self.start = start

    def __getitem__(self, index):
        return WindowedGroupBy(self.root, self.grouper, index, self.n, self.value, self.with_state, self.start)

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
        elif isinstance(grouper_example, np.ndarray) or is_index_like(grouper_example):
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
                                      start=self.start,
                                      returns_state=True,
                                      diff=diff,
                                      window=window,
                                      with_state=self.with_state)

        for fn, s_type in _stream_types[stream_type]:
            """Function checks if example is of a specific frame type"""
            if fn(example):
                return s_type(outstream, example)
        return Streaming(outstream, example, stream_type=stream_type)


def random_datapoint(now=None, **kwargs):
    """Example of querying a single current value"""
    if now is None:
        now = pd.Timestamp.now()
    return pd.DataFrame(
        {'a': np.random.random(1)}, index=[now])


def random_datablock(last, now, **kwargs):
    """
    Example of querying over a time range since last update

    Parameters
    ----------
    last: pd.Timestamp
        Time of previous call to this function.
    now: pd.Timestamp
        Current time.
    freq: pd.Timedelta, optional
        The time interval between individual records to be returned.
        For good throughput, should be much smaller than the
        interval at which this function is called.

    Returns a pd.DataFrame with random values where:

    The x column is uniformly distributed.
    The y column is Poisson distributed.
    The z column is normally distributed.
    """
    freq = kwargs.get("freq", pd.Timedelta("100ms"))
    index = pd.date_range(start=last + freq, end=now, freq=freq)

    df = pd.DataFrame({'x': np.random.random(len(index)),
                       'y': np.random.poisson(size=len(index)),
                       'z': np.random.normal(0, 1, size=len(index))},
                      index=index)
    return df


@DataFrame.register_api(staticmethod, "from_periodic")
class PeriodicDataFrame(DataFrame):
    """A streaming dataframe using the asyncio ioloop to poll a callback fn

    Parameters
    ----------
    datafn: callable
        Callback function accepting **kwargs and returning a
        pd.DataFrame.  kwargs will include at least
        'last' (pd.Timestamp.now() when datafn was last invoked), and
        'now' (current pd.Timestamp.now()).
    interval: timedelta
        The time interval between new dataframes.
    dask: boolean
        If true, uses a DaskStream instead of a regular Source.
    **kwargs:
        Optional keyword arguments to be passed into the callback function.

    By default, returns a three-column random pd.DataFrame generated
    by the 'random_datablock' function.

    Example
    -------
    >>> df = PeriodicDataFrame(interval='1s', datafn=random_datapoint)  # doctest: +SKIP
    """

    def __init__(self, datafn=random_datablock, interval='500ms', dask=False,
                 start=True, **kwargs):
        if dask:
            from streamz.dask import DaskStream
            source = DaskStream()
        else:
            source = Source()
        self.loop = source.loop
        self.interval = pd.Timedelta(interval).total_seconds()
        self.source = source
        self.continue_ = [False]  # like the oppose of self.stopped
        self.kwargs = kwargs

        stream = self.source.map(lambda x: datafn(**x, **kwargs))
        example = datafn(last=pd.Timestamp.now(), now=pd.Timestamp.now(), **kwargs)

        super(PeriodicDataFrame, self).__init__(stream, example)
        if start:
            self.start()

    def start(self):
        if not self.continue_[0]:
            self.continue_[0] = True
            self.loop.add_callback(self._cb, self.interval, self.source,
                                   self.continue_)

    def __del__(self):
        self.stop()

    def stop(self):
        self.continue_[0] = False

    @staticmethod
    async def _cb(interval, source, continue_):
        last = pd.Timestamp.now()
        while continue_[0]:
            await asyncio.sleep(interval)
            now = pd.Timestamp.now()
            await asyncio.gather(*source._emit(dict(last=last, now=now)))
            last = now


@DataFrame.register_api(staticmethod, "random")
class Random(PeriodicDataFrame):
    """PeriodicDataFrame providing random values by default

    Accepts same parameters as PeriodicDataFrame, plus
    `freq`, a string that will be converted to a pd.Timedelta
    and passed to the 'datafn'.

    Useful mainly for examples and docs.

    Example
    -------
    >>> source = Random(freq='100ms', interval='1s')  # doctest: +SKIP
    """

    def __init__(self, freq='100ms', interval='500ms', dask=False,
                 start=True, datafn=random_datablock):
        super(Random, self).__init__(datafn, interval, dask, start,
                                     freq=pd.Timedelta(freq))


_stream_types['streaming'].append((is_dataframe_like, DataFrame))
_stream_types['streaming'].append((is_index_like, Index))
_stream_types['streaming'].append((is_series_like, Series))
_stream_types['updating'].append((is_dataframe_like, DataFrames))
_stream_types['updating'].append((is_series_like, Seriess))
