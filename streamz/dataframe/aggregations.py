from __future__ import division, print_function

from collections import deque
from numbers import Number

import numpy as np
import pandas as pd
from .utils import is_series_like, is_index_like, get_dataframe_package


class Aggregation(object):
    pass


class Sum(Aggregation):
    def on_new(self, acc, new):
        if len(new):
            result = acc + new.sum()
        else:
            result = acc
        return result, result

    def on_old(self, acc, old):
        result = acc - old.sum()
        return result, result

    def initial(self, new):
        result = new.sum()
        if isinstance(result, Number):
            result = 0
        else:
            result[:] = 0
        return result


class Mean(Aggregation):
    def on_new(self, acc, new):
        totals, counts = acc
        if len(new):
            totals = totals + new.sum()
            counts = counts + new.count()
        if isinstance(counts, Number) and counts == 0:
            counts = 1
        return (totals, counts), totals / counts

    def on_old(self, acc, old):
        totals, counts = acc
        if len(old):
            totals = totals - old.sum()
            counts = counts - old.count()
        if isinstance(counts, Number) and counts == 0:
            counts = 1
        return (totals, counts), totals / counts

    def initial(self, new):
        s, c = new.sum(), new.count()
        if isinstance(s, Number):
            s = 0
            c = 0
        else:
            s[:] = 0
            c[:] = 0
        return (s, c)


class Count(Aggregation):
    def on_new(self, acc, new):
        result = acc + new.count()
        return result, result

    def on_old(self, acc, old):
        result = acc - old.count()
        return result, result

    def initial(self, new):
        return new.iloc[:0].count()


class Size(Aggregation):
    def on_new(self, acc, new):
        result = acc + new.size
        return result, result

    def on_old(self, acc, old):
        result = acc - old.size
        return result, result

    def initial(self, new):
        return 0


class Var(Aggregation):
    def __init__(self, ddof=1):
        self.ddof = ddof

    def _compute_result(self, x, x2, n):
        result = (x2 / n) - (x / n) ** 2
        if self.ddof != 0:
            result = result * n / (n - self.ddof)
        return result

    def on_new(self, acc, new):
        x, x2, n = acc
        if len(new):
            x = x + new.sum()
            x2 = x2 + (new ** 2).sum()
            n = n + new.count()

        return (x, x2, n), self._compute_result(x, x2, n)

    def on_old(self, acc, new):
        x, x2, n = acc
        if len(new):
            x = x - new.sum()
            x2 = x2 - (new ** 2).sum()
            n = n - new.count()

        return (x, x2, n), self._compute_result(x, x2, n)

    def initial(self, new):
        s = new.sum()
        c = new.count()
        if isinstance(s, Number):
            s = 0
            c = 0
        else:
            s[:] = 0
            c[:] = 0
        return (s, s, c)


class Full(Aggregation):
    """ Return the full window of data every time

    This is somewhat expensive, builtin aggregations should be preferred when
    possible
    """
    def on_new(self, acc, new):
        df_package = get_dataframe_package(new)
        result = df_package.concat([acc, new])
        return result, result

    def on_old(self, acc, old):
        result = acc.iloc[len(old):]
        return result, result

    def initial(self, new):
        return new.iloc[:0]


class EWMean(Aggregation):
    def __init__(self, com):
        self.com = com
        alpha = 1. / (1. + self.com)
        self.old_wt_factor = 1. - alpha
        self.new_wt = 1.

    def on_new(self, acc, new):
        result, old_wt, is_first = acc
        for i in range(int(is_first), len(new)):
            old_wt *= self.old_wt_factor
            result = ((old_wt * result) + (self.new_wt * new.iloc[i])) / (old_wt + self.new_wt)
            old_wt += self.new_wt
        return (result, old_wt, False), result

    def on_old(self, acc, old):
        pass

    def initial(self, new):
        return new.iloc[:1], 1, True


def diff_iloc(dfs, new, window=None):
    """ Emit new list of dfs and decayed data

    Parameters
    ----------
    dfs: list
        List of historical dataframes
    new: DataFrame, Series
        New data
    window: int

    Returns
    -------
    dfs: list
        New list of historical data
    old: list
        List of dataframes to decay
    """
    dfs = deque(dfs)
    if len(new) > 0:
        dfs.append(new)
    old = []
    if len(dfs) > 0:
        n = sum(map(len, dfs)) - window
        while n > 0:
            if len(dfs[0]) <= n:
                df = dfs.popleft()
                old.append(df)
                n -= len(df)
            else:
                old.append(dfs[0].iloc[:n])
                dfs[0] = dfs[0].iloc[n:]
                n = 0

    return dfs, old


def diff_loc(dfs, new, window=None):
    """ Emit new list of dfs and decayed data

    Parameters
    ----------
    dfs: list
        List of historical dataframes
    new: DataFrame, Series
        New data
    window: value

    Returns
    -------
    dfs: list
        New list of historical data
    old: list
        List of dataframes to decay
    """
    dfs = deque(dfs)
    if len(new) > 0:
        dfs.append(new)
    old = []
    if len(dfs) > 0:
        mx = max(df.index.max() for df in dfs)
        mn = mx - pd.Timedelta(window) + pd.Timedelta('1ns')
        while pd.Timestamp(dfs[0].index.min()) < mn:
            o = dfs[0].loc[:mn]
            if len(old) > 0:
                old.append(o)
            else:
                old = [o]
            dfs[0] = dfs[0].iloc[len(o):]
            if not len(dfs[0]):
                dfs.popleft()

    return dfs, old


def diff_expanding(dfs, new, window=None):
    dfs = deque(dfs)
    if len(new) > 0:
        dfs.append(new)
    return dfs, []


def diff_align(dfs, groupers):
    """ Align groupers to newly-diffed dataframes

    For groupby aggregations we keep historical values of the grouper along
    with historical values of the dataframes.  The dataframes are kept in
    historical sync with the ``diff_loc`` and ``diff_iloc`` functions above.
    This function copies that functionality over to the secondary list of
    groupers.
    """
    old = []
    while len(dfs) < len(groupers):
        old.append(groupers.popleft())

    if dfs:
        n = len(groupers[0]) - len(dfs[0])
        if n:
            old.append(groupers[0][:n])
            groupers[0] = groupers[0][n:]

    assert len(dfs) == len(groupers)
    for df, g in zip(dfs, groupers):
        assert len(df) == len(g)
    return old, groupers


def window_accumulator(acc, new, diff=None, window=None, agg=None, with_state=False):
    """ An accumulation binary operator for windowed aggregations

    This is the function that is actually given to the ``Stream.accumulate``
    function.  It performs all of the work given old state, new data, a diff
    function, window value, and aggregation object.

    Parameters
    ----------
    acc: state
    new: DataFrame, Series
        The new data to add to the window.
    diff: callable
        One of ``diff_iloc`` or ``diff_loc``
    window: int, value
        Either an integer for ``n=...`` for a value like ``value='2h'``
    agg: Aggregation
        The aggregation object to apply, like ``Sum()``

    Returns
    -------
    acc: state
    result: newly emitted result

    See Also
    --------
    accumulator
    windowed_groupby_accumulator
    """
    if acc is None:
        acc = {'dfs': [], 'state': agg.initial(new)}
    dfs = acc['dfs']
    state = acc['state']
    dfs, old = diff(dfs, new, window=window)
    if new is not None:
        state, result = agg.on_new(state, new)
    for o in old:
        if len(o):
            state, result = agg.on_old(state, o)
    acc2 = {'dfs': dfs, 'state': state}
    return acc2, result


def windowed_groupby_accumulator(acc, new, diff=None, window=None, agg=None, grouper=None, with_state=False):
    """ An accumulation binary operator for windowed groupb-aggregations

    This is the function that is actually given to the ``Stream.accumulate``
    function.

    Parameters
    ----------
    acc: state
    new: DataFrame, Series
        The new data to add to the window.
    diff: callable
        One of ``diff_iloc`` or ``diff_loc``
    window: int, value
        Either an integer for ``n=...`` for a value like ``value='2h'``
    agg: Aggregation
        The aggregation object to apply, like ``Sum()``
    grouper: key or Frame
        Either a column like ``'x'`` or a Pandas Series if the groupby was
        given a streaming frame.

    Returns
    -------
    acc: state
    result: newly emitted result

    See Also
    --------
    accumulator
    windowed_accumulator
    """
    if agg.grouper is None and isinstance(new, tuple):
        new, grouper = new
    else:
        grouper = None

    size = GroupbySize(agg.columns, agg.grouper)

    if acc is None:
        acc = {'dfs': [],
               'state': agg.initial(new, grouper=grouper),
               'size-state': size.initial(new, grouper=grouper)}
        if isinstance(grouper, np.ndarray) or is_series_like(grouper) or is_index_like(grouper):
            acc['groupers'] = deque([])

    dfs = acc['dfs']
    state = acc['state']
    size_state = acc['size-state']

    dfs, old = diff(dfs, new, window=window)

    if 'groupers' in acc:
        groupers = deque(acc['groupers'])
        if len(grouper) > 0:
            groupers.append(grouper)
        old_groupers, groupers = diff_align(dfs, groupers)
    else:
        old_groupers = [grouper] * len(old)

    if new is not None:
        state, result = agg.on_new(state, new, grouper=grouper)
        size_state, _ = size.on_new(size_state, new, grouper=grouper)
    for o, og in zip(old, old_groupers):
        if 'groupers' in acc:
            assert len(o) == len(og)
        if len(o):
            state, result = agg.on_old(state, o, grouper=og)
            size_state, _ = size.on_old(size_state, o, grouper=og)

    nonzero = size_state != 0
    if not nonzero.all():
        size_state = size_state[nonzero]
        result = result[nonzero]
        if isinstance(state, tuple):
            state = tuple(s[nonzero] for s in state)
        else:
            state = state[nonzero]

    acc2 = {'dfs': dfs, 'state': state, 'size-state': size_state}
    if 'groupers' in acc:
        acc2['groupers'] = groupers
    return acc2, result


def accumulator(acc, new, agg=None):
    """ An accumulation binary operator

    This is the function that is actually given to the ``Stream.accumulate``
    function.

    See Also
    --------
    windowed_accumulator
    windowed_groupby_accumulator
    """
    if acc is None:
        acc = agg.initial(new)
    return agg.on_new(acc, new)


class GroupbyAggregation(Aggregation):
    def __init__(self, columns, grouper=None, **kwargs):
        self.grouper = grouper
        self.columns = columns
        for k, v in kwargs.items():
            setattr(self, k, v)

    def grouped(self, df, grouper=None):
        if grouper is None:
            grouper = self.grouper

        g = df.groupby(grouper)

        if self.columns is not None:
            g = g[self.columns]

        return g


class GroupbySum(GroupbyAggregation):
    def on_new(self, acc, new, grouper=None):
        g = self.grouped(new, grouper=grouper)
        result = acc.add(g.sum(), fill_value=0)
        result.index.name = acc.index.name
        return result, result

    def on_old(self, acc, old, grouper=None):
        g = self.grouped(old, grouper=grouper)
        result = acc.sub(g.sum(), fill_value=0)
        result.index.name = acc.index.name
        return result, result

    def initial(self, new, grouper=None):
        if hasattr(grouper, 'iloc'):
            grouper = grouper.iloc[:0]
        if isinstance(grouper, np.ndarray) or is_index_like(grouper):
            grouper = grouper[:0]
        return self.grouped(new.iloc[:0], grouper=grouper).sum()


class GroupbyCount(GroupbyAggregation):
    def on_new(self, acc, new, grouper=None):
        g = self.grouped(new, grouper=grouper)
        result = acc.add(g.count(), fill_value=0)
        result = result.astype(int)
        result.index.name = acc.index.name
        return result, result

    def on_old(self, acc, old, grouper=None):
        g = self.grouped(old, grouper=grouper)
        result = acc.sub(g.count(), fill_value=0)
        result = result.astype(int)
        result.index.name = acc.index.name
        return result, result

    def initial(self, new, grouper=None):
        if hasattr(grouper, 'iloc'):
            grouper = grouper.iloc[:0]
        if isinstance(grouper, np.ndarray) or is_index_like(grouper):
            grouper = grouper[:0]
        return self.grouped(new.iloc[:0], grouper=grouper).count()


class GroupbySize(GroupbyAggregation):
    def on_new(self, acc, new, grouper=None):
        g = self.grouped(new, grouper=grouper)
        result = acc.add(g.size(), fill_value=0)
        result = result.astype(int)
        result.index.name = acc.index.name
        return result, result

    def on_old(self, acc, old, grouper=None):
        g = self.grouped(old, grouper=grouper)
        result = acc.sub(g.size(), fill_value=0)
        result = result.astype(int)
        result.index.name = acc.index.name
        return result, result

    def initial(self, new, grouper=None):
        if hasattr(grouper, 'iloc'):
            grouper = grouper.iloc[:0]
        if isinstance(grouper, np.ndarray) or is_index_like(grouper):
            grouper = grouper[:0]
        return self.grouped(new.iloc[:0], grouper=grouper).size()


class ValueCounts(Aggregation):
    def on_new(self, acc, new, grouper=None):
        result = acc.add(new.value_counts(), fill_value=0).astype(int)
        result.index.name = acc.index.name
        return result, result

    def on_old(self, acc, new, grouper=None):
        result = acc.sub(new.value_counts(), fill_value=0).astype(int)
        result.index.name = acc.index.name
        return result, result

    def initial(self, new, grouper=None):
        return new.iloc[:0].value_counts()


class GroupbyMean(GroupbyAggregation):
    def on_new(self, acc, new, grouper=None):
        totals, counts = acc
        g = self.grouped(new, grouper=grouper)
        totals = totals.add(g.sum(), fill_value=0)
        counts = counts.add(g.count(), fill_value=0)
        totals.index.name = acc[0].index.name
        counts.index.name = acc[1].index.name
        return (totals, counts), totals / counts

    def on_old(self, acc, old, grouper=None):
        totals, counts = acc
        g = self.grouped(old, grouper=grouper)
        totals = totals.sub(g.sum(), fill_value=0)
        counts = counts.sub(g.count(), fill_value=0)
        totals.index.name = acc[0].index.name
        counts.index.name = acc[1].index.name
        return (totals, counts), totals / counts

    def initial(self, new, grouper=None):
        if hasattr(grouper, 'iloc'):
            grouper = grouper.iloc[:0]
        if isinstance(grouper, np.ndarray) or is_index_like(grouper):
            grouper = grouper[:0]
        g = self.grouped(new.iloc[:0], grouper=grouper)
        return (g.sum(), g.count())


class GroupbyVar(GroupbyAggregation):
    def _compute_result(self, x, x2, n):
        result = (x2 / n) - (x / n) ** 2
        if self.ddof != 0:
            result = result * n / (n - self.ddof)
        return result

    def on_new(self, acc, new, grouper=None):
        x, x2, n = acc
        g = self.grouped(new, grouper=grouper)
        if len(new):
            x = x.add(g.sum(), fill_value=0)
            x2 = x2.add(g.agg(lambda x: (x**2).sum()), fill_value=0)
            n = n.add(g.count(), fill_value=0)

        return (x, x2, n), self._compute_result(x, x2, n)

    def on_old(self, acc, old, grouper=None):
        x, x2, n = acc
        g = self.grouped(old, grouper=grouper)
        if len(old):
            x = x.sub(g.sum(), fill_value=0)
            x2 = x2.sub(g.agg(lambda x: (x**2).sum()), fill_value=0)
            n = n.sub(g.count(), fill_value=0)

        return (x, x2, n), self._compute_result(x, x2, n)

    def initial(self, new, grouper=None):
        if hasattr(grouper, 'iloc'):
            grouper = grouper.iloc[:0]
        if isinstance(grouper, np.ndarray) or is_index_like(grouper):
            grouper = grouper[:0]

        new = new.iloc[:0]
        g = self.grouped(new, grouper=grouper)
        x = g.sum()
        x2 = g.agg(lambda x: (x**2).sum())
        n = g.count()

        return (x, x2, n)


def groupby_accumulator(acc, new, agg=None):
    if agg.grouper is None and isinstance(new, tuple):
        new, grouper = new
    else:
        grouper = None
    if acc is None:
        acc = agg.initial(new, grouper=grouper)
    result = agg.on_new(acc, new, grouper=grouper)
    return result
