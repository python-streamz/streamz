from pandas import DataFrame, Series
from pandas.util.testing import assert_series_equal
from ..core import Stream
from .. import dataframe


def test_sum():
    source = Stream()
    L = dataframe.sum(source).sink_to_list()

    source.emit(DataFrame({'a': [1, 2], 'b': [10, 20]}))
    assert_series_equal(L[-1], Series([3., 30.], list('ab')))

    source.emit(DataFrame({'a': [3], 'c': [1]}))
    assert_series_equal(L[-1], Series([6., 30., 1.], list('abc')))


def test_mean():
    source = Stream()
    L = dataframe.mean(source).sink_to_list()

    source.emit(DataFrame({'a': [1, 2], 'b': [10, 20]}))
    assert_series_equal(L[-1], Series([1.5, 15.], list('ab')))

    # This tests that 'b' does not change if a DataFrame with no values is
    # included, while 'a' is updated and 'c' is added.
    source.emit(DataFrame({'a': [3], 'c': [1]}))
    assert_series_equal(L[-1], Series([2., 15., 1.], list('abc')))


def test_min_and_max():
    source = Stream()
    Lmin = dataframe.min(source).sink_to_list()
    Lmax = dataframe.max(source).sink_to_list()

    source.emit(DataFrame({'a': [1, 2], 'b': [10, 20]}))
    assert_series_equal(Lmin[-1], Series([1, 10], list('ab')))
    assert_series_equal(Lmax[-1], Series([2, 20], list('ab')))

    source.emit(DataFrame({'a': [3], 'c': [1]}))
    assert_series_equal(Lmin[-1], Series([1., 10., 1.], list('abc')))
    assert_series_equal(Lmax[-1], Series([3., 20., 1.], list('abc')))

    source.emit(DataFrame({'a': [0], 'c': [1]}))
    assert_series_equal(Lmin[-1], Series([0., 10., 1.], list('abc')))
    assert_series_equal(Lmax[-1], Series([3., 20., 1.], list('abc')))
