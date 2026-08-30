from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dbtv")
except PackageNotFoundError:
    __version__ = "0.1.0a0"

__all__ = ["__version__"]
