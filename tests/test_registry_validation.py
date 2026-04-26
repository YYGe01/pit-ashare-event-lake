from pathlib import Path

from pitlake.control.registry import ProviderRegistry, SourceRegistry


def _providers() -> ProviderRegistry:
    return ProviderRegistry(
        providers=[
            {
                "provider_id": "demo",
                "provider_name": "Demo",
                "provider_type": "test",
                "auth_method": "none",
                "storage_permission": "raw_allowed",
            }
        ],
        path=Path("provider_registry.yaml"),
    )


def _source(*, enabled: bool, implementation_status: str) -> dict[str, object]:
    return {
        "source_id": f"demo_{implementation_status}",
        "provider_id": "demo",
        "logical_dataset": "demo_dataset",
        "source_type": "python_api",
        "access_method": "test",
        "auth_type": "none",
        "priority": "P2",
        "enabled": enabled,
        "implementation_status": implementation_status,
        "adapter_class": "pitlake.connectors.missing.MissingConnector",
    }


def test_disabled_planned_source_does_not_require_importable_adapter() -> None:
    registry = SourceRegistry(
        sources=[_source(enabled=False, implementation_status="planned_entitlement_required")],
        path=Path("source_registry.yaml"),
    )

    assert registry.validate(_providers(), {"demo_dataset"}) == []


def test_enabled_source_requires_importable_adapter() -> None:
    registry = SourceRegistry(
        sources=[_source(enabled=True, implementation_status="planned_entitlement_required")],
        path=Path("source_registry.yaml"),
    )

    errors = registry.validate(_providers(), {"demo_dataset"})

    assert len(errors) == 1
    assert "adapter_class is not importable" in errors[0]


def test_disabled_active_source_requires_importable_adapter() -> None:
    registry = SourceRegistry(
        sources=[_source(enabled=False, implementation_status="active_v0")],
        path=Path("source_registry.yaml"),
    )

    errors = registry.validate(_providers(), {"demo_dataset"})

    assert len(errors) == 1
    assert "adapter_class is not importable" in errors[0]
