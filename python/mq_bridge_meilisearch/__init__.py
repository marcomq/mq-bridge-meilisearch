"""Meilisearch endpoint plugin for mq-bridge."""

from pathlib import Path

_PACKAGE = Path(__file__).parent


def library_path() -> str:
    """Absolute path of the bundled plugin library."""
    from mq_bridge import plugin_library_path

    return plugin_library_path(_PACKAGE)


def register() -> str:
    """Register the ``meilisearch`` endpoint and return its name."""
    from mq_bridge import load_plugin_package

    return load_plugin_package(_PACKAGE)


__all__ = ["library_path", "register"]
