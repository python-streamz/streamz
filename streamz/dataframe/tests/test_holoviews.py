from __future__ import division, print_function

from holoviews.core import NdOverlay, Dimension
from holoviews.element import (
    Curve, Scatter, Area, Bars, BoxWhisker, Dataset, Distribution,
    Table
)
from holoviews.streams import Buffer, Pipe
import pytest
import streamz.dataframe as sd
import streamz.dataframe.holoviews

ELEMENT_TYPES = {'line': Curve, 'scatter': Scatter, 'area': Area,
                 'bars': Bars, 'barh': Bars}


def test_sdf_stream_setup():
    source = sd.Random(freq='10ms', interval='100ms')
    dmap = source.plot()
    assert isinstance(dmap.streams[0], Buffer)

def test_series_stream_setup():
    source = sd.Random(freq='10ms', interval='100ms')
    dmap = source.x.plot()
    assert isinstance(dmap.streams[0], Buffer)

def test_ssdf_stream_setup():
    source = sd.Random(freq='10ms', interval='100ms')
    dmap = source.groupby('y').sum().plot()
    assert type(dmap.streams[0]) is Pipe

def test_sseries_stream_setup():
    source = sd.Random(freq='10ms', interval='100ms')
    dmap = source.groupby('y').sum().x.plot()
    assert type(dmap.streams[0]) is Pipe

@pytest.mark.parametrize('kind', ['line', 'scatter', 'area'])
def test_plot_multi_column_chart_from_sdf(kind):
    source = sd.Random(freq='10ms', interval='100ms')
    dmap = source.plot(kind=kind)
    element = dmap[()]
    assert isinstance(element, NdOverlay)
    assert element.keys() == ['x', 'y', 'z']
    assert isinstance(element.last, ELEMENT_TYPES[kind])
    assert element.last.kdims == [Dimension('index')]

@pytest.mark.parametrize('kind', ['line', 'scatter', 'area', 'bars', 'barh'])
def test_plot_xy_chart_from_sdf(kind):
    source = sd.Random(freq='10ms', interval='100ms')
    dmap = source.plot(x='index', y='y', kind=kind)
    element = dmap[()]
    assert isinstance(element, ELEMENT_TYPES[kind])
    assert element.kdims == [Dimension('index')]
    assert element.vdims == [Dimension('y')]

@pytest.mark.parametrize('kind', ['line', 'scatter', 'area', 'bars', 'barh'])
def test_plot_xy_chart_from_series(kind):
    source = sd.Random(freq='10ms', interval='100ms')
    dmap = source.y.plot(kind=kind)
    element = dmap[()]
    assert isinstance(element, ELEMENT_TYPES[kind])
    assert element.kdims == [Dimension('index')]
    assert element.vdims == [Dimension('y')]
