'''
    This code is where most of the StreamDoc processing in the application
    layer should reside. Conversions from other interfaces to StreamDoc are
    found in corresponding interface folders.
'''
from functools import wraps
import time
import sys
from uuid import uuid4

# convenience routine to return a hash of the streamdoc
from dask.delayed import tokenize

class StreamDoc(dict):
    def __init__(self, streamdoc=None, args=(), kwargs={}, attributes={}):
        ''' A generalized document meant to be parsed by Streams.

            Components:
                attributes : the metadata
                outputs : a dictionary of outputs from stream
                args : a list of args
                kwargs : a list of kwargs
                statistics : some statistics of the stream that generated this
                    It can be anything, like run_start, run_stop etc
        '''
        # initialize the dictionary class
        super(StreamDoc, self).__init__(self)

        # initialize the metadata and kwargs
        self['attributes'] = dict()
        self['kwargs'] = dict()
        self['args'] = list()

        # these two pieces are specific to the run
        self['statistics'] = dict()
        self['uid'] = str(uuid4())

        # needed to distinguish that it is a StreamDoc by stream methods
        self['_StreamDoc'] = 'StreamDoc v1.0'

        # update
        if streamdoc is not None:
            self.updatedoc(streamdoc)

        # override with args
        self.add(args=args, kwargs=kwargs, attributes=attributes)

    def updatedoc(self, streamdoc):
        self.add(args=streamdoc['args'], kwargs=streamdoc['kwargs'],
                 attributes=streamdoc['attributes'],
                 statistics=streamdoc['statistics'])

    def add(self, args=[], kwargs={}, attributes={}, statistics={}):
        ''' add args and kwargs'''
        if not isinstance(args, list) and not isinstance(args, tuple):
            args = (args, )

        self['args'].extend(args)
        # Note : will overwrite previous kwarg data without checking
        self['kwargs'].update(kwargs)
        self['attributes'].update(attributes)
        self['statistics'].update(statistics)

    @property
    def args(self):
        return self['args']

    @property
    def kwargs(self):
        return self['kwargs']

    @property
    def attributes(self):
        return self['attributes']

    @property
    def statistics(self):
        return self['statistics']

    def get_return(self, elem=None):
        ''' get what the function would have normally returned.

            returns raw data
            Parameters
            ----------
            elem : optional
                if an integer: get that nth argumen
                if a string : get that kwarg
        '''
        if isinstance(elem, int):
            res = self['args'][elem]
        elif isinstance(elem, str):
            res = self['kwargs'][elem]
        elif elem is None:
            # return general expected function output
            if len(self['args']) == 0 and len(self['kwargs']) > 0:
                res = dict(self['kwargs'])
            elif len(self['args']) > 0 and len(self['kwargs']) == 0:
                res = self['args']
                if len(res) == 1:
                    # a function with one arg normally returns this way
                    res = res[0]
            else:
                # if it's more complex, then it wasn't a function output
                res = self

        return res

    def tokenize(self):
        ''' Get the properties specific to data. So args and kwargs.
            Don't return the attributes, stats or uid which are specific to a
        StreamDoc instance.

            This is a convenience routine meant for routines that cache results.
            Some identifier is necessary to cache the results.
        '''
        args = self['args']
        kwargs = self['kwargs']
        hsh = tokenize(*args, **kwargs)
        # If stream name is given, make hash easier to read by giving the stream name
        if 'function_list' in self['attributes']:
            funlist = self['attributes']['function_list']
            last_func_name = funlist[len(funlist)-1]
            hsh = last_func_name + "-" + hsh
        print("tokenized {}".format(hsh))
        return hsh

    def repr(self):
        mystr = "args : {}\n\n".format(self['args'])
        mystr += "kwargs : {}\n\n".format(self['kwargs'])
        mystr += "attributes : {}\n\n".format(self['attributes'])
        mystr += "statistics : {}\n\n".format(self['statistics'])
        return mystr

    def get_attributes(self):
        return StreamDoc(args=self['attributes'])

    def merge(self, newstreamdoc):
        ''' Merge another streamdoc into this one.
            The new streamdoc's attributes/kwargs will override this one upon
            collison.
        '''
        streamdoc = StreamDoc(self)
        streamdoc.add(args=newstreamdoc['args'], kwargs=newstreamdoc['kwargs'],
                      attributes=newstreamdoc['attributes'])
        return streamdoc

    def select(self, *mapping):
        ''' remap args and kwargs
            combinations can be any one of the following:


            Some examples:

            (1,)        : map 1st arg to next available arg
            (1, None)   : map 1st arg to next available arg
            'a',        : map 'a' to 'a'
            'a', None   : map 'a' to next available arg
            'a','b'     : map 'a' to 'b'
            1, 'a'      : map 1st arg to 'a'

            The following is NOT accepted:
            (1,2) : this would map arg 1 to arg 2. Use proper ordering instead
            ('a',1) : this would map 'a' to arg 1. Use proper ordering instead

            Notes
            -----
            These *must* be tuples, and the list a list kwarg elems must be
                strs and arg elems must be ints to accomplish this instead
        '''
        #print("IN STREAMDOC -> SELECT")
        # TODO : take args instead
        #if not isinstance(mapping, list):
            #mapping = [mapping]
        streamdoc = StreamDoc(self)
        newargs = list()
        newkwargs = dict()
        totargs = dict(args=newargs, kwargs=newkwargs)

        for mapelem in mapping:
            if isinstance(mapelem, str):
                mapelem = mapelem, mapelem
            elif isinstance(mapelem, int):
                mapelem = mapelem, None

            # length 1 for strings, repeat, for int give None
            if len(mapelem) == 1 and isinstance(mapelem[0], str):
                mapelem = mapelem[0], mapelem[0]
            elif len(mapelem) == 1 and isinstance(mapelem[0], int):
                mapelem = mapelem[0], None

            oldkey = mapelem[0]
            newkey = mapelem[1]

            if isinstance(oldkey, int):
                oldparentkey = 'args'
            elif isinstance(oldkey, str):
                oldparentkey = 'kwargs'

            if newkey is None:
                newparentkey = 'args'
            elif isinstance(newkey, str):
                newparentkey = 'kwargs'
            elif isinstance(newkey, int):
                raise ValueError("Integer tuple pairs not accepted")

            if newparentkey == 'args':
                totargs[newparentkey].append(streamdoc[oldparentkey][oldkey])
            else:
                totargs[newparentkey][newkey] = streamdoc[oldparentkey][oldkey]

        streamdoc['args'] = totargs['args']
        streamdoc['kwargs'] = totargs['kwargs']

        return streamdoc


def _is_streamdoc(doc):
    if isinstance(doc, dict) and '_StreamDoc' in doc:
        return True
    else:
        return False


def parse_streamdoc(name):
    ''' Decorator to parse StreamDocs from functions

    This is a decorator meant to wrap functions that process streams.
        It must make the following two assumptions:
            functions on streams process either one or two arguments:
                - if processing two arguments, it is assumed that the operation
                is an accumulation: newstate = f(prevstate, newinstance)

        Generally, this wrapper should be used hand in hand with a type.
        For example, here, the type is StreamDoc. If a StreamDoc is detected,
        process the inputs/outputs in a more complicated fashion. Else, leave
        function untouched.

        output:
            if a dict, makes a StreamDoc of args where keys are dict elements
            if a tuple, makes a StreamDoc with only arguments
            else, makes a StreamDoc of just one element
    '''
    def streamdoc_dec(f):
        @wraps(f)
        def f_new(x, x2=None, **kwargs_additional):
            if x2 is None:
                if _is_streamdoc(x):
                    # extract the args and kwargs
                    args = x.args
                    kwargs = x.kwargs
                    attributes = x.attributes
                else:
                    args = (x,)
                    kwargs = dict()
                    attributes = dict()
            else:
                if _is_streamdoc(x) and _is_streamdoc(x2):
                    args = x.get_return(), x2.get_return()
                    kwargs = dict()
                    attributes = x.attributes
                    # attributes of x2 overrides x
                    attributes.update(x2.attributes)
                else:
                    raise ValueError("Two normal arguments not accepted")

            kwargs.update(kwargs_additional)

            statistics = dict()
            t1 = time.time()
            try:
                # now run the function
                result = f(*args, **kwargs)
                statistics['status'] = "Success"
            except Exception:
                result = {}
                statistics['status'] = "Failure"
                type, value, traceback = sys.exc_info()
                statistics['error_message'] = value
                print("caught exception {}".format(value))
                #print("traceback:  {}".format(traceback.__dir__()))


            t2 = time.time()
            statistics['runtime'] = t1-t2
            statistics['runstart'] = t1

            if 'function_list' not in attributes:
                attributes['function_list'] = list()
            else:
                attributes['function_list'] = attributes['function_list'].copy()
            attributes['function_list'].append(f.__name__)
            #print("Running function {}".format(f.__name__))
            # instantiate new stream doc
            streamdoc = StreamDoc(attributes=attributes)
            # load in attributes
            # Save outputs to StreamDoc
            if isinstance(result, dict):
                streamdoc.add(kwargs=result)
            else:
                streamdoc.add(args=result)

            return streamdoc

        return f_new

    return streamdoc_dec
