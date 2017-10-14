import sys

if sys.version_info[0] == 2:
    from thread import get_ident as get_thread_identity
    import __builtin__ as builtins
else:
    import builtins
    from threading import get_ident as get_thread_identity
