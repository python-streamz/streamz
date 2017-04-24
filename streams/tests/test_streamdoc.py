from nose.tools import assert_raises

from streams.StreamDoc import StreamDoc, parse_streamdoc, _is_streamdoc, select
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

    # compound selections
    sdoc1 = sdoc.select([(0, 'foo'), ('foo', None), ('a', None)])
    assert(sdoc1.select([('foo','bar')])['kwargs']['bar'] == 1)
    assert(sdoc1.select([(0, 'bar')])['kwargs']['bar'] == 'bar')

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

    def validate_result(res):
        assert(res==4)
    # initialize the wrapper to understand streamdocs
    s1 = Stream(wrapper=parse_streamdoc)
    s2 = s1.map(foo)
    # s2.map(print)
    s2.sink(validate_result)

    sdoc = StreamDoc(args=[2, 2])

    s1.emit(sdoc)


def test_with_stream_accumulator():
    def accumfunc(prevstate, curstate):
        ''' here prevstate just assumed an int.'''
        return prevstate + curstate

    def validate_result(res):
        assert(res == 3)
    # initialize the wrapper to understand streamdocs
    s1 = Stream(wrapper=parse_streamdoc)
    s2 = s1.accumulate(accumfunc)
    # for debugging:
    # s2.map(print)

    sdoc = StreamDoc(args=[1])

    s1.emit(sdoc)
    s1.emit(sdoc)
    # not good practice to change streams as its being used but this
    # is just to validate a result
    s2.map(validate_result)
    s1.emit(sdoc)

def test_streamdoc_apply():
    def myadder(a, b, **kwargs):
        return a + b

    def print_kwargs(*args, **kwargs):
        print("args : {}".format(args))
        print("kwargs : {}".format(kwargs))

    def validate_args(*args, **kwargs):
        assert(kwargs['foo'] == 3)

    # add two numbers, return, then move returned result to 'foo' in kwarg
    s1 = Stream(wrapper=parse_streamdoc)
    s2 = s1.map(myadder)
    # map zeroth arg to 'foo' in kwarg
    s3 = s2.apply(select, [(0, 'foo')])
    # hard coded validation
    s3.map(validate_args)
    s3.map(print_kwargs)

    # the sum 1+2 = 3 will go to 'foo' in kwarg
    sdoc = StreamDoc(args=[1,2], kwargs=dict(g=2))

    s1.emit(sdoc)

def test_on_class():
    ''' Test that it works on a class with a hidden state.
        Hidden states can be useful ways to pass data to functions that are not
        necessarily essential to the calculation. In this case it is, but
        things like plot linewidth etc for plotting are not.
    '''
    class myincrementer:
        def __init__(self, incby):
            self.incby = incby
        def increment(self, a):
            return a + self.incby

    myacc = myincrementer(3)
    s1 = Stream(wrapper=parse_streamdoc)
    s2 = s1.map(myacc.increment)
    s2.map(print)

    sdoc = StreamDoc(args=(1,))
    s1.emit(sdoc)
    s1.emit(sdoc)
    s1.emit(sdoc)
    s1.emit(sdoc)

def test_delayed():
    ''' try making a delayed wrapper.'''
    from dask import delayed
    def myadder(a,b):
        return a + b

    def makenewdec(name):
        def newdec(f):
            @delayed(pure=True)
            @parse_streamdoc(name)
            def newf(*args, **kwargs):
                return f(*args, **kwargs)
            return newf
        return newdec

    def compute(obj):
        return obj.compute()

    def print_args(*args, **kwargs):
        print("printing")
        print("args : {}".format(args))
        print("kwargs : {}".format(kwargs))

    s1 = Stream(wrapper=makenewdec)
    # map maps the unwrapped function, apply maps the wrapped function
    s2 = s1.map(myadder)
    s3 = s2.apply(compute)
    s4 = s3.map(print_args)
    s4.apply(compute)

    sdoc = StreamDoc(args=(2,3))
    s1.emit(sdoc)
