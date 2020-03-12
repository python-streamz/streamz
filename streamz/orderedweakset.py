# -*- coding: utf8 -*-
# This is a copy from Stack Overflow
# https://stackoverflow.com/questions/7828444/indexable-weak-ordered-set-in-python
# Asked by Neil G https://stackoverflow.com/users/99989/neil-g
# Answered/edited by https://stackoverflow.com/users/1001643/raymond-hettinger
import collections
import weakref


class OrderedSet(collections.abc.MutableSet):
    def __init__(self, values=()):
        self._od = collections.OrderedDict().fromkeys(values)

    def __len__(self):
        return len(self._od)

    def __iter__(self):
        return iter(self._od)

    def __contains__(self, value):
        return value in self._od

    def add(self, value):
        self._od[value] = None

    def discard(self, value):
        self._od.pop(value, None)


class OrderedWeakrefSet(weakref.WeakSet):
    def __init__(self, values=()):
        super(OrderedWeakrefSet, self).__init__()
        self.data = OrderedSet()
        for elem in values:
            self.add(elem)
