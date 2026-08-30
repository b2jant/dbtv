from __future__ import annotations

from dataclasses import asdict

from dbtv.core.hashing import sha256_value
from dbtv.core.models import CanonicalSchema, SnapshotRequest, SourceVersion


def request_fingerprint(request: SnapshotRequest, provider: str) -> str:
    identity = request.identity_dict()
    identity.pop("query_tag", None)
    return sha256_value({"provider": provider, "request": identity})


def snapshot_key(
    request: SnapshotRequest,
    provider: str,
    schema: CanonicalSchema,
    source_version: SourceVersion | None,
    content_fingerprint: str,
) -> str:
    return sha256_value(
        {
            "request_fingerprint": request_fingerprint(request, provider),
            "schema": asdict(schema),
            "source_version": asdict(source_version) if source_version else None,
            "content_fingerprint": content_fingerprint,
        }
    )
