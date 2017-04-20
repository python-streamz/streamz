from nose.tools import assert_raises

from streams.StreamDoc import StreamDoc, parse_streamdoc, _is_streamdoc
from streams.core import Stream


def test_type_check():
    sdoc = StreamDoc()
    assert(_is_streamdoc(sdoc))
    sdoc = dict()
    assert(not _is_streamdoc(sdoc))
    sdoc = 1
    assert(not _is_streamdoc(sdoc))


def test_streamdoc_index():
    ''' test that the various methods of selecting inputs/outputs of StreamDoc
    work as intended.'''
    sdoc = StreamDoc(args=(1, 2, 3, 4), kwargs=dict(foo="bar", a="fdfa"),
                     attributes={"name": "cat"})

    # one arg
    assert(sdoc.select(1)['args'][0] == 2)
    # list
    assert(sdoc.select([0, 1])['args'][1] == 2)
    # list of tuples
    assert(sdoc.select([(1, ), (1, )])['args'][0] == 2)

    # onekwarg
    assert(sdoc.select('foo')['kwargs']['foo'] == 'bar')
    # map a kwarg
    assert(sdoc.select([('foo', 'foo')])['kwargs']['foo'] == 'bar')
    assert(sdoc.select([('foo', 'bar')])['kwargs']['bar'] == 'bar')
    # list of kwarg
    assert(sdoc.select(['foo', 'a'])['kwargs']['foo'] == 'bar')

    # map an arg to a kwarg
    assert(sdoc.select((1, 'foo'))['kwargs']['foo'] == 2)
    assert(sdoc.select([(1, 'foo')])['kwargs']['foo'] == 2)
    # map a kwarg to an arg
    assert(sdoc.select([('foo', None)])['args'][0] == 'bar')
    assert(sdoc.select([('foo', None), ('a', None)])['args'][1] == 'fdfa')

    assert_raises(ValueError, sdoc.select, ('foo', 1))
    assert_raises(ValueError, sdoc.select, (1, 1))


def test_streamdoc_dec():
    @parse_streamdoc("test")
    def foo(a, b, **kwargs):
        return a+b

    sdoc = StreamDoc(args=[1, 2])
    res = foo(sdoc)
    assert(res['args'][0] == 3)


def test_with_streams():
    def foo(a, b):
        return a + b

    # initialize the wrapper to understand streamdocs
    s1 = Stream(wrapper=parse_streamdoc)
    s2 = s1.map(foo)
    s2.map(print)
    s2.sink(print)

    sdoc = StreamDoc(args=[1, 2])

    s1.emit(sdoc)


def test_with_stream_accumulator():
    def accumfunc(prevstate, curstate):
        ''' here prevstate just assumed an int.'''
        return prevstate + curstate

    # initialize the wrapper to understand streamdocs
    s1 = Stream(wrapper=parse_streamdoc)
    s2 = s1.accumulate(accumfunc)
    s2.map(print)

    sdoc = StreamDoc(args=[1])

    s1.emit(sdoc)
    s1.emit(sdoc)
    s1.emit(sdoc)
