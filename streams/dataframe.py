import pandas as pd
from .core import Stream


class sum(Stream):
    def __init__(self, child):
        self.cumsum = pd.Series()
        Stream.__init__(self, child)

    def update(self, x, who=None):
        self.cumsum = self.cumsum.add(x.sum(), fill_value=0)
        self.emit(self.cumsum)


class mean(Stream):
    def __init__(self, child):
        self.cumsum = pd.Series()
        self.cumcount = pd.Series()
        Stream.__init__(self, child)

    def update(self, x, who=None):
        self.cumsum = self.cumsum.add(x.sum(), fill_value=0)
        self.cumcount = self.cumcount.add(x.count(), fill_value=0)
        self.emit(self.cumsum / self.cumcount)


class min(Stream):
    def __init__(self, child):
        self.min = pd.DataFrame()
        Stream.__init__(self, child)

    def update(self, x, who=None):
        self.min = pd.concat([self.min, x.min()], 1).T.min()
        self.emit(self.min)


class max(Stream):
    def __init__(self, child):
        self.max = pd.DataFrame()
        Stream.__init__(self, child)

    def update(self, x, who=None):
        self.max = pd.concat([self.max, x.max()], 1).T.max()
        self.emit(self.max)
