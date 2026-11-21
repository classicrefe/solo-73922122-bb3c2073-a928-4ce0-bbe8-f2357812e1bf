"""HTTP client for the local chain source."""

from __future__ import annotations

from typing import Any

import httpx

from .db import BlockRecord, ObservedHead, TransferRecord

_HEX = set("0123456789abcdef")


class SourceError(RuntimeError):
    """Any error that makes a round unpublishable."""


def _validate_hash(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value or value.lower() != value:
        raise SourceError(f"invalid {field}: {value!r}")
    if any(ch not in _HEX for ch in value) or len(value) % 2:
        raise SourceError(f"invalid {field}: {value!r}")
    return value


class ChainSourceClient:
    def __init__(self, base_url: str, timeout_seconds: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=httpx.Timeout(timeout_seconds, connect=min(5.0, timeout_seconds)),
            headers={"accept": "application/json"},
        )

    def close(self) -> None:
        self._client.close()

    def _get(self, path: str) -> Any:
        try:
            response = self._client.get(path)
        except httpx.HTTPError as exc:
            raise SourceError(f"GET {path} failed: {exc}") from exc
        if response.status_code != 200:
            body = response.text[:300].strip()
            raise SourceError(
                f"GET {path} returned HTTP {response.status_code}: {body}"
            )
        try:
            return response.json()
        except ValueError as exc:
            raise SourceError(f"GET {path} returned invalid JSON") from exc

    def get_head(self) -> ObservedHead:
        payload = self._get("/head")
        if not isinstance(payload, dict):
            raise SourceError("head payload is not an object")
        try:
            revision = int(payload["revision"])
            height = int(payload["height"])
            block_hash = _validate_hash(payload["hash"], "head hash")
        except (KeyError, TypeError, ValueError) as exc:
            raise SourceError(f"invalid head payload: {payload!r}") from exc
        if revision < 0 or height < 0:
            raise SourceError(f"invalid head payload: {payload!r}")
        return ObservedHead(revision=revision, height=height, hash=block_hash)

    def get_block(self, block_hash: str) -> BlockRecord:
        payload = self._get(f"/blocks/{block_hash}")
        if not isinstance(payload, dict):
            raise SourceError(f"block {block_hash} payload is not an object")
        try:
            height = int(payload["height"])
            block_hash_value = _validate_hash(payload["hash"], "block hash")
            parent_hash = _validate_hash(payload["parent_hash"], "parent_hash")
        except (KeyError, TypeError, ValueError) as exc:
            raise SourceError(f"invalid block payload for {block_hash}") from exc
        if block_hash_value != block_hash:
            raise SourceError(
                f"source returned block {block_hash_value} for request {block_hash}"
            )
        raw_transfers = payload.get("transfers", [])
        if not isinstance(raw_transfers, list):
            raise SourceError(f"block {block_hash} transfers is not a list")
        transfers: list[TransferRecord] = []
        seen_txids: set[str] = set()
        for raw in raw_transfers:
            if not isinstance(raw, dict):
                raise SourceError(f"block {block_hash} has a malformed transfer")
            try:
                txid = str(raw["txid"])
                sender = str(raw.get("from_address", raw.get("from", "")))
                receiver = str(raw.get("to_address", raw.get("to", "")))
                amount = int(raw["amount"])
            except (KeyError, TypeError, ValueError) as exc:
                raise SourceError(
                    f"block {block_hash} has a malformed transfer: {raw!r}"
                ) from exc
            if not txid or not sender or not receiver or amount < 0:
                raise SourceError(
                    f"block {block_hash} has an invalid transfer: {raw!r}"
                )
            if txid in seen_txids:
                raise SourceError(f"block {block_hash} repeats txid {txid}")
            seen_txids.add(txid)
            transfers.append(
                TransferRecord(
                    txid=txid,
                    from_address=sender,
                    to_address=receiver,
                    amount=amount,
                )
            )
        return BlockRecord(
            hash=block_hash_value,
            height=height,
            parent_hash=parent_hash,
            transfers=tuple(transfers),
        )
