import pkg_resources


def load_plugins():
    for entry_point in pkg_resources.iter_entry_points("streamz.plugins"):
        entry_point.load()
