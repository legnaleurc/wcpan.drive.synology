from importlib.metadata import version

from .client import create_service as create_service


__version__ = version(__package__ or __name__)

__all__ = (
    "create_service",
    "__version__",
)
