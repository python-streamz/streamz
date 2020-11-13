import inspect

import pytest
from streamz import Source, Stream


class MockEntryPoint:

    def __init__(self, name, cls, module_name=None):
        self.name = name
        self.cls = cls
        self.module_name = module_name

    def load(self):
        return self.cls


def test_register_plugin_entry_point():
    class test_stream(Stream):
        pass

    entry_point = MockEntryPoint("test_node", test_stream)
    Stream.register_plugin_entry_point(entry_point)

    assert Stream.test_node.__name__ == "stub"

    Stream().test_node()

    assert Stream.test_node.__name__ == "test_stream"


def test_register_plugin_entry_point_modifier():
    class test_source(Source):
        pass

    entry_point = MockEntryPoint("from_test", test_source)
    Stream.register_plugin_entry_point(entry_point, staticmethod)

    Stream.from_test()

    assert inspect.isfunction(Stream().from_test)


def test_register_plugin_entry_point_raises_type():
    class invalid_node:
        pass

    entry_point = MockEntryPoint("test", invalid_node, "test_module.test")

    Stream.register_plugin_entry_point(entry_point)

    with pytest.raises(TypeError):
        Stream.test()


def test_register_plugin_entry_point_raises_duplicate_name():
    entry_point = MockEntryPoint("map", None)

    with pytest.raises(ValueError):
        Stream.register_plugin_entry_point(entry_point)
