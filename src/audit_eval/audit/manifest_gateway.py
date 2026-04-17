"""Manifest and formal snapshot gateway interfaces for replay queries."""

from __future__ import annotations

from typing import Any, Protocol

from audit_eval.contracts.manifest_draft import CyclePublishManifestDraft


class ManifestGateway(Protocol):
    """Load the published manifest snapshot set for one cycle."""

    def load(self, cycle_id: str) -> CyclePublishManifestDraft:
        """Return the cycle_publish_manifest row for ``cycle_id``."""


class FormalSnapshotGateway(Protocol):
    """Load formal objects only through manifest-bound snapshot refs."""

    def load_snapshot(self, snapshot_ref: str) -> dict[str, Any]:
        """Return the historical formal object for ``snapshot_ref``."""


__all__ = [
    "FormalSnapshotGateway",
    "ManifestGateway",
]
