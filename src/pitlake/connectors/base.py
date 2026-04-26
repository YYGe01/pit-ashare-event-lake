"""Base classes for source connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from pitlake.control.contracts import DatasetContract
from pitlake.settings import ProjectSettings
from pitlake.storage.metadata_store import MetadataStore
from pitlake.storage.raw_store import RawStore
from pitlake.utils import sha256_json


@dataclass(frozen=True)
class RequestPlan:
    url: str
    method: str = "GET"
    params: dict[str, Any] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    timeout_seconds: int = 30

    @property
    def request_hash(self) -> str:
        return sha256_json(
            {
                "url": self.url,
                "method": self.method,
                "params": self.params,
                "headers": self.headers,
                "timeout_seconds": self.timeout_seconds,
            }
        )


@dataclass(frozen=True)
class ResponsePayload:
    request: RequestPlan
    status_code: int
    content: bytes
    headers: dict[str, str]
    final_url: str
    mime_type: str


@dataclass
class RunStats:
    request_count: int = 0
    success_count: int = 0
    error_count: int = 0
    new_item_count: int = 0
    updated_item_count: int = 0
    duplicate_count: int = 0
    quarantine_count: int = 0


class BaseConnector(ABC):
    """Template for API/RSS/web/file connectors."""

    connector_version = "0.1.0"

    def __init__(
        self,
        *,
        settings: ProjectSettings,
        source_config: dict[str, Any],
        contract: DatasetContract,
        raw_store: RawStore,
        metadata_store: MetadataStore,
    ) -> None:
        self.settings = settings
        self.source_config = source_config
        self.contract = contract
        self.raw_store = raw_store
        self.metadata_store = metadata_store

    @property
    def source_id(self) -> str:
        return str(self.source_config["source_id"])

    @property
    def provider_id(self) -> str:
        return str(self.source_config["provider_id"])

    @property
    def logical_dataset(self) -> str:
        return str(self.source_config["logical_dataset"])

    @property
    def connector_name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def plan_requests(self) -> list[RequestPlan]:
        """Return requests for one connector run."""

    @abstractmethod
    def collect(self, *, run_id: str, options: dict[str, Any] | None = None) -> RunStats:
        """Execute one source collection run and persist raw/metadata records."""

    def execute_request(self, request: RequestPlan) -> ResponsePayload:
        requests = __import__("requests")
        response = requests.request(
            method=request.method,
            url=request.url,
            params=request.params,
            headers=request.headers,
            timeout=request.timeout_seconds,
        )
        return ResponsePayload(
            request=request,
            status_code=response.status_code,
            content=response.content,
            headers=dict(response.headers),
            final_url=response.url,
            mime_type=response.headers.get("content-type", "application/octet-stream"),
        )
