import json

import pytest
from dask.dataframe.utils import assert_eq
import numpy as np
import pandas as pd
from tornado import gen

from streamz import Stream
from streamz.utils_test import gen_test
from streamz.dataframe import StreamingDataFrame, StreamingSeries
import streamz.dataframe as sd


def test_identity():
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df)
    L = sdf.stream.sink_to_list()

    sdf.emit(df)

    assert L[0] is df
    assert list(sdf.example.columns) == ['x', 'y']

    x = sdf.x
    assert isinstance(x, StreamingSeries)
    L2 = x.stream.sink_to_list()
    assert not L2

    sdf.emit(df)
    assert isinstance(L2[0], pd.Series)
    assert assert_eq(L2[0], df.x)


def test_exceptions():
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df)
    with pytest.raises(TypeError):
        sdf.emit(1)

    with pytest.raises(IndexError):
        sdf.emit(pd.DataFrame())


def test_sum():
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df)
    df_out = sdf.sum().stream.sink_to_list()

    x = sdf.x
    x_out = (x.sum() + 1).stream.sink_to_list()

    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf.emit(df)
    sdf.emit(df)

    assert assert_eq(df_out[0], df.sum())
    assert assert_eq(df_out[1], df.sum() + df.sum())

    assert x_out[0] == df.x.sum() + 1
    assert x_out[1] == df.x.sum() + df.x.sum() + 1


def test_mean():
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df)
    mean = sdf.mean()
    assert isinstance(mean, StreamingSeries)
    df_out = mean.stream.sink_to_list()

    x = sdf.x
    x_out = x.mean().stream.sink_to_list()

    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf.emit(df)
    sdf.emit(df)

    assert assert_eq(df_out[0], df.mean())
    assert assert_eq(df_out[1], df.mean())

    assert x_out[0] == df.x.mean()
    assert x_out[1] == df.x.mean()


def test_arithmetic():
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    a = StreamingDataFrame(example=df)
    b = a + 1

    L1 = b.stream.sink_to_list()

    c = b.x * 10

    L2 = c.stream.sink_to_list()

    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    a.emit(df)

    assert assert_eq(L1[0], df + 1)
    assert assert_eq(L2[0], (df + 1).x * 10)


@pytest.mark.xfail(reason='need to zip two streaming dataframes together')
def test_pair_arithmetic():
    df = pd.DataFrame({'x': list(range(10)), 'y': [1] * 10})

    a = StreamingDataFrame(example=df.iloc[0])
    L = ((a.x + a.y) * 2).sink_to_list()

    a.emit(df.iloc[:5])
    a.emit(df.iloc[5:])

    assert len(L) == 2
    assert_eq(pd.concat(L, axis=0), (df.x + df.y) * 2)


@pytest.mark.parametrize('agg', ['sum', 'mean'])
@pytest.mark.parametrize('grouper', [lambda a: a.x % 3,
                                     lambda a: 'x',
                                     lambda a: ['x']])
@pytest.mark.parametrize('indexer', [lambda g: g.y,
                                     lambda g: g,
                                     lambda g: g[['y']],
                                     lambda g: g[['x', 'y']]])
def test_groupby_aggregate(agg, grouper, indexer):
    df = pd.DataFrame({'x': (np.arange(10) // 2).astype(float), 'y': [1.0] * 10})

    a = StreamingDataFrame(example=df.iloc[:0])

    L = getattr(indexer(a.groupby(grouper(a))), agg)().stream.sink_to_list()

    a.emit(df.iloc[:3])
    a.emit(df.iloc[3:7])
    a.emit(df.iloc[7:])

    assert assert_eq(L[-1], getattr(indexer(df.groupby(grouper(df))), agg)())


def test_repr():
    df = pd.DataFrame({'x': (np.arange(10) // 2).astype(float), 'y': [1.0] * 10})
    a = StreamingDataFrame(example=df)

    text = repr(a)
    assert type(a).__name__ in text
    assert 'x' in text
    assert 'y' in text

    text = repr(a.x)
    assert type(a.x).__name__ in text
    assert 'x' in text

    text = repr(a.x.sum())
    assert type(a.x.sum()).__name__ in text


def test_repr_html():
    df = pd.DataFrame({'x': (np.arange(10) // 2).astype(float), 'y': [1.0] * 10})
    a = StreamingDataFrame(example=df)

    for x in [a, a.y, a.y.mean()]:
        html = x._repr_html_()
        assert type(x).__name__ in html
        assert '1' in html


def test_setitem():
    df = pd.DataFrame({'x': list(range(10)), 'y': [1] * 10})

    sdf = StreamingDataFrame(example=df.iloc[:0])
    stream = sdf.stream

    sdf['z'] = sdf['x'] * 2
    sdf['a'] = 10
    sdf[['c', 'd']] = sdf[['x', 'y']]

    L = sdf.mean().stream.sink_to_list()

    stream.emit(df.iloc[:3])
    stream.emit(df.iloc[3:7])
    stream.emit(df.iloc[7:])

    df['z'] = df['x'] * 2
    df['a'] = 10
    df[['c', 'd']] = df[['x', 'y']]

    assert_eq(L[-1], df.mean())


@pytest.mark.parametrize('kwargs,op', [
    ({}, 'sum'),
    ({}, 'mean'),
    pytest.mark.slow(({}, 'min')),
    ({}, 'median'),
    pytest.mark.slow(({}, 'max')),
    pytest.mark.slow(({}, 'var')),
    pytest.mark.slow(({}, 'count')),
    ({'ddof': 0}, 'std'),
    pytest.mark.slow(({'quantile': 0.5}, 'quantile')),
    ({'arg': {'A': 'sum', 'B': 'min'}}, 'aggregate')
])
@pytest.mark.parametrize('window', [
    pytest.mark.slow(2),
    7,
    pytest.mark.slow('3h'),
    pd.Timedelta('200 minutes')
])
@pytest.mark.parametrize('m', [
    2,
    pytest.mark.slow(5)
])
@pytest.mark.parametrize('pre_get,post_get', [
    (lambda df: df, lambda df: df),
    (lambda df: df.x, lambda x: x),
    (lambda df: df, lambda df: df.x)
])
def test_rolling_count_aggregations(op, window, m, pre_get, post_get, kwargs):
    index = pd.DatetimeIndex(start='2000-01-01', end='2000-01-03', freq='1h')
    df = pd.DataFrame({'x': np.arange(len(index))}, index=index)

    expected = getattr(post_get(pre_get(df).rolling(window)), op)(**kwargs)

    sdf = StreamingDataFrame(example=df.iloc[:0])
    roll = getattr(post_get(pre_get(sdf).rolling(window)), op)(**kwargs)
    L = roll.stream.sink_to_list()
    assert len(L) == 0

    for i in range(0, len(df), m):
        sdf.emit(df.iloc[i: i + m])

    assert len(L) > 1

    assert_eq(pd.concat(L), expected)


def test_stream_to_dataframe():
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    source = Stream()
    L = source.to_dataframe(example=df).x.sum().stream.sink_to_list()

    source.emit(df)
    source.emit(df)
    source.emit(df)

    assert L == [6, 12, 18]


def test_integration_from_stream():
    source = Stream()
    sdf = source.partition(4).to_batch(example=['{"x": 0, "y": 0}']).map(json.loads).to_dataframe()
    result = sdf.groupby(sdf.x).y.sum().mean()
    L = result.stream.sink_to_list()

    for i in range(12):
        source.emit(json.dumps({'x': i % 3, 'y': i}))

    assert L == [2, 17 / 3, 100 / 9]


@gen_test()
def test_random_source():
    source = sd.Random(freq='10ms', interval='100ms')
    L = source.stream.sink_to_list()
    yield gen.sleep(0.5)
    assert 2 < len(L) < 8
    assert all(2 < len(df) < 25 for df in L)

    source.x
    source.rolling('10s')


@pytest.mark.xfail(reason="Does not yet exist")
@pytest.mark.parametrize('n', [2, 3, 4])
def test_repartition_n(n):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    source = Stream()
    L = source.to_dataframe(example=df).repartition(n=n).stream.sink_to_list()

    source.emit(df)
    source.emit(df)
    source.emit(df)
    source.emit(df)

    assert all(len(x) == n for x in L)
    assert_eq(pd.concat(L), pd.concat([df] * 4))


@pytest.mark.xfail(reason="Does not yet exist")
@gen_test()
def test_repartition_interval(n):
    source = sd.Random(freq='10ms', interval='100ms')
    L = source.stream.sink_to_list()
    L2 = source.repartition(interval='150ms').stream.sink_to_list()

    yield gen.sleep(0.400)

    assert L2

    for df in L2:
        assert df.index.max() - df.index.min() <= pd.Timedelta('150ms')

    expected = pd.concat(L).iloc[:sum(map(len, L2))]

    assert_eq(pd.concat(L2), expected)
