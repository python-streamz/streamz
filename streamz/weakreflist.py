# -*- coding: utf8 -*-
# This is a copy from weakreflist
# https://pypi.python.org/pypi/weakreflist
from weakref import ref, ReferenceType
import sys

__all__ = ["WeakList"]


def is_slice(index):
    return isinstance(index, slice)


class WeakList(list):
    def __init__(self, items=list()):
        list.__init__(self, self._refs(items))

    def value(self, item):
        return item() if isinstance(item, ReferenceType) else item

    def ref(self, item):
        try:
            item = ref(item, self.remove_all)
        finally:
            return item

    def __contains__(self, item):
        return list.__contains__(self, self.ref(item))

    def __getitem__(self, index):
        items = list.__getitem__(self, index)
        return type(self)(self._values(items)) if is_slice(index) else self.value(items)

    def __setitem__(self, index, item):
        items = self._refs(item) if is_slice(index) else self.ref(item)
        return list.__setitem__(self, index, items)

    def __iter__(self):
        return iter(self[index] for index in range(len(self)))

    def __reversed__(self):
        reversed_self = type(self)(self)
        reversed_self.reverse()
        return reversed_self

    def append(self, item):
        list.append(self, self.ref(item))

    def remove(self, item):
        return list.remove(self, self.ref(item))

    def remove_all(self, item):
        item = self.ref(item)
        while list.__contains__(self, item):
            list.remove(self, item)

    def index(self, item):
        return list.index(self, self.ref(item))

    def count(self, item):
        return list.count(self, self.ref(item))

    def pop(self, item):
        return list.pop(self, self.ref(item))

    def insert(self, index, item):
        return list.insert(self, index, self.ref(item))

    def extend(self, items):
        return list.extend(self, self._refs(items))

    def __iadd__(self, other):
        return list.__iadd__(self, self._refs(other))

    def _refs(self, items):
        return map(self.ref, items)

    def _values(self, items):
        return map(self.value, items)

    def _sort_key(self, key=None):
        return self.value if key == None else lambda item: key(self.value(item))

    if sys.version_info < (3,):
        def sort(self, cmp=None, key=None, reverse=False):
            return list.sort(self, cmp=cmp, key=self._sort_key(key), reverse=reverse)

        def __setslice__(self, lower_bound, upper_bound, items):
            return self.__setitem__(slice(lower_bound, upper_bound), items)

        def __getslice__(self, lower_bound, upper_bound):
            return self.__getitem__(slice(lower_bound, upper_bound))
    else:
        def sort(self, key=None, reverse=False):
            return list.sort(self, key=self._sort_key(key), reverse=reverse)
