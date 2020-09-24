import pytest
import toolz

from streamz.batch import Batch, Streaming
from streamz.utils_test import inc


def test_core():
    a = Batch()
    b = a.pluck('x').map(inc)
    c = b.sum()
    L = c.stream.sink_to_list()

    a.emit([{'x': i, 'y': 0} for i in range(4)])

    assert isinstance(b, Batch)
    assert isinstance(c, Streaming)
    assert L == [1 + 2 + 3 + 4]


def test_dataframes():
    pd = pytest.importorskip('pandas')
    from streamz.dataframe import DataFrame
    data = [{'x': i, 'y': 2 * i} for i in range(10)]

    s = Batch(example=[{'x': 0, 'y': 0}])
    sdf = s.map(lambda d: toolz.assoc(d, 'z', d['x'] + d['y'])).to_dataframe()

    assert isinstance(sdf, DataFrame)

    L = sdf.stream.sink_to_list()

    for batch in toolz.partition_all(3, data):
        s.emit(batch)

    result = pd.concat(L)
    assert result.z.tolist() == [3 * i for i in range(10)]


def test_periodic_dataframes():
    pd = pytest.importorskip('pandas')
    from streamz.dataframe import PeriodicDataFrame
    from streamz.dataframe.core import random_datapoint
    df = random_datapoint(now=pd.Timestamp.now())
    assert len(df) == 1

    def callback(now, **kwargs):
        return pd.DataFrame(dict(x=50, index=[now]))

    df = PeriodicDataFrame(callback, interval='20ms')
    assert df.tail(0).x == 50
    df.stop()


def test_filter():
    a = Batch()
    f = a.filter(lambda x: x % 2 == 0)
    s = f.to_stream()
    L = s.sink_to_list()

    a.emit([1, 2, 3, 4])
    a.emit([5, 6])

    assert L == [2, 4, 6]
