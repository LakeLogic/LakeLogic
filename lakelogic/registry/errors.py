"""Registry validation errors."""

from __future__ import annotations


class RegistryValidationError(ValueError):
    """A ``_domain.yaml`` / ``_system.yaml`` is structurally invalid.

    Carries the offending file path (when known) so a validator/CLI can point the user
    straight at it.
    """

    def __init__(self, message: str, *, path: str | None = None) -> None:
        self.path = path
        super().__init__(f"{path}: {message}" if path else message)
