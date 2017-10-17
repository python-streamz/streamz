import json
import operator
from time import sleep

import pytest
from dask.dataframe.utils import assert_eq
import numpy as np
import pandas as pd
from tornado import gen

from streamz import Stream
from streamz.utils_test import gen_test
from streamz.dataframe import StreamingDataFrame, StreamingSeries
import streamz.dataframe as sd
from streamz.dask import DaskStream

from distributed import Client
from distributed.utils_test import loop  # flake8: noqa


@pytest.fixture(scope="module")
def client():
    client = Client(processes=False)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture(params=['core', 'dask'])
def stream(loop, request, client):  # flake8: noqa
    if request.param == 'core':
        return Stream()
    else:
        return DaskStream()


def test_identity(stream):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df, stream=stream)
    L = sdf.stream.gather().sink_to_list()

    sdf.emit(df)

    assert L[0] is df
    assert list(sdf.example.columns) == ['x', 'y']

    x = sdf.x
    assert isinstance(x, StreamingSeries)
    L2 = x.stream.gather().sink_to_list()
    assert not L2

    sdf.emit(df)
    assert isinstance(L2[0], pd.Series)
    assert assert_eq(L2[0], df.x)


def test_dtype(stream):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df, stream=stream)

    assert str(sdf.dtypes) == str(df.dtypes)
    assert sdf.x.dtype == df.x.dtype
    assert sdf.index.dtype == df.index.dtype


def test_attributes():
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df)

    assert 'x' in dir(sdf)
    assert 'z' not in dir(sdf)

    sdf.x
    with pytest.raises(AttributeError):
        sdf.z


def test_exceptions(stream):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df, stream=stream)
    with pytest.raises(TypeError):
        sdf.emit(1)

    with pytest.raises(IndexError):
        sdf.emit(pd.DataFrame())


def test_sum(stream):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df, stream=stream)
    df_out = sdf.sum().stream.gather().sink_to_list()

    x = sdf.x
    x_out = (x.sum() + 1).stream.gather().sink_to_list()

    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf.emit(df)
    sdf.emit(df)

    assert assert_eq(df_out[0], df.sum())
    assert assert_eq(df_out[1], df.sum() + df.sum())

    assert x_out[0] == df.x.sum() + 1
    assert x_out[1] == df.x.sum() + df.x.sum() + 1


def test_mean(stream):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df, stream=stream)
    mean = sdf.mean()
    assert isinstance(mean, StreamingSeries)
    df_out = mean.stream.gather().sink_to_list()

    x = sdf.x
    x_out = x.mean().stream.gather().sink_to_list()

    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf.emit(df)
    sdf.emit(df)

    assert assert_eq(df_out[0], df.mean())
    assert assert_eq(df_out[1], df.mean())

    assert x_out[0] == df.x.mean()
    assert x_out[1] == df.x.mean()


@pytest.mark.parametrize('op', [
    operator.add,
    operator.and_,
    operator.eq,
    operator.floordiv,
    operator.ge,
    operator.gt,
    operator.le,
    operator.lshift,
    operator.lt,
    operator.mod,
    operator.mul,
    operator.ne,
    operator.or_,
    operator.pow,
    operator.rshift,
    operator.sub,
    operator.truediv,
    operator.xor,
])
@pytest.mark.parametrize('getter', [lambda df: df, lambda df: df.x])
def test_binary_operators(op, getter, stream):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    try:
        left = op(getter(df), 2)
        right = op(2, getter(df))
    except Exception:
        return

    a = StreamingDataFrame(example=df, stream=stream)
    l = op(getter(a), 2).stream.gather().sink_to_list()
    r = op(2, getter(a)).stream.gather().sink_to_list()

    a.emit(df)

    assert_eq(l[0], left)
    assert_eq(r[0], right)


@pytest.mark.parametrize('op', [
    operator.abs,
    operator.inv,
    operator.invert,
    operator.neg,
    operator.not_,
])
@pytest.mark.parametrize('getter', [lambda df: df, lambda df: df.x])
def test_unary_operators(op, getter):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    try:
        expected = op(getter(df))
    except Exception:
        return

    a = StreamingDataFrame(example=df)
    b = op(getter(a)).stream.sink_to_list()

    a.emit(df)

    assert_eq(b[0], expected)


def test_binary_stream_operators(stream):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})

    expected = df.x + df.y

    a = StreamingDataFrame(example=df, stream=stream)
    b = (a.x + a.y).stream.gather().sink_to_list()

    a.emit(df)

    assert_eq(b[0], expected)


def test_index(stream):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    a = StreamingDataFrame(example=df, stream=stream)
    b = a.index + 5
    L = b.stream.gather().sink_to_list()

    a.emit(df)
    a.emit(df)

    assert_eq(L[0], df.index + 5)
    assert_eq(L[1], df.index + 5)


def test_pair_arithmetic(stream):
    df = pd.DataFrame({'x': list(range(10)), 'y': [1] * 10})

    a = StreamingDataFrame(example=df.iloc[:0], stream=stream)
    L = ((a.x + a.y) * 2).stream.gather().sink_to_list()

    a.emit(df.iloc[:5])
    a.emit(df.iloc[5:])

    assert len(L) == 2
    assert_eq(pd.concat(L, axis=0), (df.x + df.y) * 2)


@pytest.mark.parametrize('agg', ['sum', 'mean'])
@pytest.mark.parametrize('grouper', [lambda a: a.x % 3,
                                     lambda a: 'x',
                                     lambda a: a.index % 2,
                                     lambda a: ['x']])
@pytest.mark.parametrize('indexer', [lambda g: g.y,
                                     lambda g: g,
                                     lambda g: g[['y']],
                                     lambda g: g[['x', 'y']]])
def test_groupby_aggregate(agg, grouper, indexer, stream):
    df = pd.DataFrame({'x': (np.arange(10) // 2).astype(float), 'y': [1.0] * 10})

    a = StreamingDataFrame(example=df.iloc[:0], stream=stream)

    L = getattr(indexer(a.groupby(grouper(a))), agg)().stream.gather().sink_to_list()

    a.emit(df.iloc[:3])
    a.emit(df.iloc[3:7])
    a.emit(df.iloc[7:])

    assert assert_eq(L[-1], getattr(indexer(df.groupby(grouper(df))), agg)())


def test_repr(stream):
    df = pd.DataFrame({'x': (np.arange(10) // 2).astype(float), 'y': [1.0] * 10})
    a = StreamingDataFrame(example=df, stream=stream)

    text = repr(a)
    assert type(a).__name__ in text
    assert 'x' in text
    assert 'y' in text

    text = repr(a.x)
    assert type(a.x).__name__ in text
    assert 'x' in text

    text = repr(a.x.sum())
    assert type(a.x.sum()).__name__ in text


def test_repr_html(stream):
    df = pd.DataFrame({'x': (np.arange(10) // 2).astype(float), 'y': [1.0] * 10})
    a = StreamingDataFrame(example=df, stream=stream)

    for x in [a, a.y, a.y.mean()]:
        html = x._repr_html_()
        assert type(x).__name__ in html
        assert '1' in html


def test_setitem(stream):
    df = pd.DataFrame({'x': list(range(10)), 'y': [1] * 10})

    sdf = StreamingDataFrame(example=df.iloc[:0], stream=stream)
    stream = sdf.stream

    sdf['z'] = sdf['x'] * 2
    sdf['a'] = 10
    sdf[['c', 'd']] = sdf[['x', 'y']]

    L = sdf.mean().stream.gather().sink_to_list()

    stream.emit(df.iloc[:3])
    stream.emit(df.iloc[3:7])
    stream.emit(df.iloc[7:])

    df['z'] = df['x'] * 2
    df['a'] = 10
    df[['c', 'd']] = df[['x', 'y']]

    assert_eq(L[-1], df.mean())


@pytest.mark.slow
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
def test_rolling_count_aggregations(op, window, m, pre_get, post_get, kwargs,
        stream):
    index = pd.DatetimeIndex(start='2000-01-01', end='2000-01-03', freq='1h')
    df = pd.DataFrame({'x': np.arange(len(index))}, index=index)

    expected = getattr(post_get(pre_get(df).rolling(window)), op)(**kwargs)

    sdf = StreamingDataFrame(example=df.iloc[:0], stream=stream)
    roll = getattr(post_get(pre_get(sdf).rolling(window)), op)(**kwargs)
    L = roll.stream.gather().sink_to_list()
    assert len(L) == 0

    for i in range(0, len(df), m):
        sdf.emit(df.iloc[i: i + m])

    assert len(L) > 1

    assert_eq(pd.concat(L), expected)


def test_stream_to_dataframe(stream):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    source = stream
    L = source.to_dataframe(example=df).x.sum().stream.gather().sink_to_list()

    source.emit(df)
    source.emit(df)
    source.emit(df)

    assert L == [6, 12, 18]


def test_integration_from_stream(stream):
    source = stream
    sdf = source.partition(4).to_batch(example=['{"x": 0, "y": 0}']).map(json.loads).to_dataframe()
    result = sdf.groupby(sdf.x).y.sum().mean()
    L = result.stream.gather().sink_to_list()

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
def test_repartition_n(n, stream):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    source = stream
    L = source.to_dataframe(example=df).repartition(n=n).stream.gather().sink_to_list()

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


def test_to_frame(stream):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df, stream=stream)

    assert sdf.to_frame() is sdf

    a = sdf.x.to_frame()
    assert isinstance(a, StreamingDataFrame)
    assert list(a.columns) == ['x']


@gen_test()
def test_plot():
    pytest.importorskip('bokeh')
    sdf = sd.Random(freq='10ms', interval='50ms')
    result = sdf[['x', 'y']].plot()

    cds = result['cds']

    assert set(cds.data) == {'x', 'y', 'index'}
    assert not len(cds.data['x'])

    yield gen.sleep(0.130)
    assert len(cds.data['x'])
    assert len(set(map(len, cds.data.values()))) == 1

    assert set(sdf.x.plot()['cds'].data) == {'x', 'index'}


def test_instantiate_with_dict(stream):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df, stream=stream)

    sdf2 = StreamingDataFrame({'a': sdf.x, 'b': sdf.x * 2,
                               'c': sdf.y % 2})
    L = sdf2.stream.gather().sink_to_list()
    assert len(sdf2.columns) == 3

    sdf.emit(df)
    sdf.emit(df)

    assert len(L) == 2
    for x in L:
        assert_eq(x[['a', 'b', 'c']],
                  pd.DataFrame({'a': df.x, 'b': df.x * 2, 'c': df.y % 2},
                               columns=['a', 'b', 'c']))


@pytest.mark.parametrize('op', ['cumsum', 'cummax', 'cumprod', 'cummin'])
@pytest.mark.parametrize('getter', [lambda df: df, lambda df: df.x])
def test_cumulative_aggregations(op, getter, stream):
    df = pd.DataFrame({'x': list(range(10)), 'y': [1] * 10})
    expected = getattr(getter(df), op)()

    sdf = StreamingDataFrame(example=df, stream=stream)

    L = getattr(getter(sdf), op)().stream.gather().sink_to_list()

    for i in range(0, 10, 3):
        sdf.emit(df.iloc[i: i + 3])
    sdf.emit(df.iloc[:0])

    assert len(L) > 1

    assert_eq(pd.concat(L), expected)


@gen_test()
def test_gc():
    sdf = sd.Random(freq='5ms', interval='100ms')
    a = StreamingDataFrame({'volatility': sdf.x.rolling('100ms').var(),
                            'sub': sdf.x - sdf.x.rolling('100ms').mean()})
    n = len(sdf.stream.downstreams)
    yield gen.sleep(0.1)
    a = StreamingDataFrame({'volatility': sdf.x.rolling('100ms').var(),
                            'sub': sdf.x - sdf.x.rolling('100ms').mean()})
    yield gen.sleep(0.1)
    a = StreamingDataFrame({'volatility': sdf.x.rolling('100ms').var(),
                            'sub': sdf.x - sdf.x.rolling('100ms').mean()})
    yield gen.sleep(0.1)
    a = StreamingDataFrame({'volatility': sdf.x.rolling('100ms').var(),
                            'sub': sdf.x - sdf.x.rolling('100ms').mean()})

    assert len(sdf.stream.downstreams) == n
    del a
    import gc; gc.collect()
    assert len(sdf.stream.downstreams) == 0


@gen_test()
def test_gc_random():
    from weakref import WeakValueDictionary
    w = WeakValueDictionary()
    a = sd.Random(freq='5ms', interval='100ms')
    w[1] = a
    yield gen.sleep(0.1)
    a = sd.Random(freq='5ms', interval='100ms')
    w[2] = a
    yield gen.sleep(0.1)
    a = sd.Random(freq='5ms', interval='100ms')
    w[3] = a
    yield gen.sleep(0.1)
    assert len(w) == 1


def test_display(stream):
    pytest.importorskip('ipywidgets')
    pytest.importorskip('IPython')

    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df, stream=stream)

    s = sdf.x.sum()

    s._ipython_display_()


def test_tail(stream):
    df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
    sdf = StreamingDataFrame(example=df, stream=stream)

    L = sdf.tail(2).stream.gather().sink_to_list()

    sdf.emit(df)
    sdf.emit(df)

    assert_eq(L[0], df.tail(2))
    assert_eq(L[1], df.tail(2))


def test_random_source(loop):
    with Client(processes=False, diagnostics_port=False, loop=loop) as client:
        source = sd.Random(freq='1ms', interval='10ms', dask=True)
        L = source.x.stream.gather().sink_to_list()
        sleep(0.20)
        assert len(client.cluster.scheduler.tasks) < 10


def test_example_type_error_message():
    try:
        sdf = StreamingDataFrame(example=[123])
    except Exception as e:
        assert 'StreamingDataFrame' in str(e)
        assert 'DataFrame' in str(e)
        assert '[123]' in str(e)
