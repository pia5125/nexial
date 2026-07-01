"""
Lightweight HTTP request counter for sheeel.com CF scrapers.

Track requests during a scrape run and build the request_metrics block
for the daily JSON summary uploaded to R2 json-files/.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RequestMetricsTracker:
    requests_total: int = 0
    requests_failed: int = 0
    cache_hits: int = 0
    failed_items: list[dict[str, Any]] = field(default_factory=list)
    _start_time: float = field(default_factory=time.time)

    def record_success(self) -> None:
        self.requests_total += 1

    def record_failure(
        self,
        *,
        name: str | None = None,
        slug: str | None = None,
        detail: str | None = None,
    ) -> None:
        self.requests_total += 1
        self.requests_failed += 1
        if name or slug:
            self._append_failed_item(name=name, slug=slug, detail=detail)

    def record_http_response(
        self,
        status_code: int,
        *,
        name: str | None = None,
        slug: str | None = None,
        detail: str | None = None,
    ) -> None:
        if status_code >= 400:
            self.record_failure(name=name, slug=slug, detail=detail or f"HTTP {status_code}")
        else:
            self.record_success()

    def record_cache_hit(self) -> None:
        self.cache_hits += 1

    def _append_failed_item(
        self,
        *,
        name: str | None,
        slug: str | None,
        detail: str | None,
    ) -> None:
        label = name or slug or "unknown"
        for item in self.failed_items:
            if item.get("name") == label or item.get("slug") == slug:
                item["errors"] = int(item.get("errors", 0)) + 1
                if detail and not item.get("detail"):
                    item["detail"] = detail
                return

        entry: dict[str, Any] = {"errors": 1}
        if name:
            entry["name"] = name
        if slug:
            entry["slug"] = slug
        if detail:
            entry["detail"] = detail
        self.failed_items.append(entry)

    def build_block(self) -> dict[str, Any]:
        duration_sec = max(int(time.time() - self._start_time), 0)
        rpm = (
            round(self.requests_total / (duration_sec / 60), 2)
            if duration_sec > 0
            else 0.0
        )
        block: dict[str, Any] = {
            "requests_total": self.requests_total,
            "requests_failed": self.requests_failed,
            "requests_per_min": rpm,
            "duration_sec": duration_sec,
        }
        if self.cache_hits:
            block["cache_hits"] = self.cache_hits
        if self.failed_items:
            block["failed_items"] = self.failed_items
        return block


def build_daily_summary(
    *,
    total_listings: int,
    request_metrics: dict[str, Any],
    subcategories: list[dict[str, Any]] | None = None,
    scraped_at: str | None = None,
    saved_to_s3_date: str | None = None,
) -> dict[str, Any]:
    from datetime import datetime

    now = datetime.now()
    return {
        "scraped_at": scraped_at or now.isoformat(timespec="seconds"),
        "saved_to_s3_date": saved_to_s3_date or now.strftime("%Y-%m-%d"),
        "total_listings": total_listings,
        "request_metrics": request_metrics,
        "subcategories": subcategories or [],
    }
