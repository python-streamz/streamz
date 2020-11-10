from streamz.sources import Source
from streamz import Stream


class MockEntryPoint:

    def __init__(self, name, cls):
        self.name = name
        self.cls = cls

    def load(self):
        return self.cls


def test_register_plugin_entry_point():
    class test(Stream):
        pass

    entry_point = MockEntryPoint("test_node", test)
    Stream.register_plugin_entry_point(entry_point)

    assert Stream.test_node.__name__ == "stub"

    Stream().test_node()

    assert Stream.test_node.__name__ == "test"


def test_register_plugin_entry_point_modifier():
    class test(Source):
        pass

    entry_point = MockEntryPoint("from_test", test)
    Stream.register_plugin_entry_point(entry_point, staticmethod)

    Stream.from_test()

    assert Stream.from_test.__self__ is Stream
