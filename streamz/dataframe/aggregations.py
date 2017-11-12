from collections import deque
from numbers import Number

import pandas as pd


class Aggregation(object):
    def update(self, acc, new_old):
        new, old = new_old
        if old is not None:
            acc, result = self.on_old(acc, old)
        if new is not None:
            acc, result = self.on_new(acc, new)
        return acc, result

    def stateless(self, new):
        acc = self.initial(new)
        acc, result = self.on_new(acc, df)
        return result


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
        return (totals, counts), totals / counts

    def on_old(self, acc, old):
        totals, counts = acc
        if len(old):
            totals = totals - old.sum()
            counts = counts - old.count()
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
    def on_new(self, acc, new):
        result = pd.concat([acc, new])
        return result, result

    def on_old(self, acc, old):
        result = acc.iloc[len(old):]
        return result, result

    def stateless(self, new):
        return new


def diff_iloc(dfs, new, window=None):
    dfs = deque(dfs)
    dfs.append(new)
    old = []
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
    dfs = deque(dfs)
    dfs.append(new)
    mx = max(df.index.max() for df in dfs)
    mn = mx - window
    old = []
    while dfs[0].index.min() < mn:
        o = dfs[0].loc[:mn]
        old.append(o)  # TODO: avoid copy if fully lost
        dfs[0] = dfs[0].iloc[len(o):]
        if not len(dfs[0]):
            dfs.popleft()

    return dfs, old


def window_accumulator(acc, new, diff=None, window=None, agg=None):
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


def accumulator(acc, new, agg=None):
    if acc is None:
        acc = agg.initial(new)
    return agg.on_new(acc, new)
