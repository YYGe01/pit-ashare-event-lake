"""Project-specific exceptions."""

from __future__ import annotations

import importlib
from types import ModuleType


class PitlakeError(Exception):
    """Base exception for pitlake."""


class ConfigError(PitlakeError):
    """Raised when configuration is invalid."""


class DependencyMissingError(PitlakeError):
    """Raised when an optional runtime dependency is missing."""


def require_dependency(module_name: str, package_hint: str | None = None) -> ModuleType:
    """Import a dependency and raise a clear installation error when it is missing."""

    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        hint = package_hint or module_name
        raise DependencyMissingError(
            f"Missing dependency '{module_name}'. Activate the conda environment or install '{hint}'."
        ) from exc

