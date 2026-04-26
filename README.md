# PIT A-Share Event Lake

A point-in-time data collection project for China A-share research.

This repository currently focuses on the data collection layer only: source registry, crawl ledger, raw append-only storage, collection manifests, monitoring, backup, and auditability.

Design documents:

- `docs/realtime_pit_data_collection_plan_zh.md`: PIT collection implementation handbook.
- `docs/pit_data_collection_architecture_zh.md`: long-term collection architecture, source/provider abstraction, quality gates, governance, and operations.

## Environment

```powershell
conda env create -f environment.yml
conda activate pit-ashare-event-lake
```

## Scope

- Keep raw collected data immutable.
- Record `first_seen_at` for every item.
- Preserve source metadata, raw payloads, content hashes, and daily manifests.
- Keep downstream parsing, event extraction, features, models, and backtests outside the collection layer.
