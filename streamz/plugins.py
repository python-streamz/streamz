import pkg_resources


def load_plugins(cls):
    for entry_point in pkg_resources.iter_entry_points("streamz.sources"):
        cls.register_plugin_entry_point(entry_point, staticmethod)
    for entry_point in pkg_resources.iter_entry_points("streamz.nodes"):
        cls.register_plugin_entry_point(entry_point)
    for entry_point in pkg_resources.iter_entry_points("streamz.sinks"):
        cls.register_plugin_entry_point(entry_point)
