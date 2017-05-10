try:
    from cytoolz import accumulate
except ImportError:
    from toolz import accumulate

class Batch(tuple):
    def __stream_map__(self, func):
        return Batch(map(func, self))

    def __stream_reduce__(self, func, accumulator):
        if isinstance(accumulator, Batch):
            return Batch(accumulate(func, self, accumulator[-1]))
        else:
            return Batch(accumulate(func, self, accumulator))

    def __stream_merge__(self, *others):
        return Batch(zip(self, *others))
