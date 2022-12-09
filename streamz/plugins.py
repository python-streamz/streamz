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


def load_plugins(cls):
    eps = importlib.metadata.entry_points()
    for entry_point in eps.get("streamz.sources", []):
        try_register(cls, entry_point, staticmethod)
    for entry_point in eps.get("streamz.nodes", []):
        try_register(cls, entry_point)
    for entry_point in eps.get("streamz.sinks", []):
        try_register(cls, entry_point)
