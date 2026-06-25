"""Runtime compatibility helpers for legacy third-party packages."""


def patch_legacy_collections():
    from collections import abc
    import collections

    for name in ("Callable", "Iterable", "Mapping", "MutableMapping", "Sequence"):
        if not hasattr(collections, name):
            setattr(collections, name, getattr(abc, name))
