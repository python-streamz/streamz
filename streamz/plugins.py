import warnings

import importlib.metadata


def try_register(cls, entry_point, *modifier):
    try:
        cls.register_plugin_entry_point(entry_point, *modifier)
    except ValueError:
        warnings.warn(
            f"Can't add {entry_point.name} from {entry_point.module_name}: "
            "name collision with existing stream node."
        )


def get_entry_point(eps, group):
    if hasattr(eps, "select"):  # Python 3.10+ / importlib_metadata >= 3.9.0
        return eps.select(group=group)
    else:
        return eps.get(group, [])

def load_plugins(cls):
    eps = importlib.metadata.entry_points()

    for entry_point in get_entry_point(eps, "streamz.sources"):
        try_register(cls, entry_point, staticmethod)
    for entry_point in get_entry_point(eps, "streamz.nodes"):
        try_register(cls, entry_point)
    for entry_point in get_entry_point(eps, "streamz.sinks"):
        try_register(cls, entry_point)
