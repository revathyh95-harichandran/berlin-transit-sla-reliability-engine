"""Power BI import refresh: ask Power BI to reload its copy of the verified Gold data.

Think of it as pressing the "update the menu board" button in the café, but only after the chef
has tasted every dish (the dbt tests). This module is shared by the Airflow DAG and the local runner,
so both behave the same way.

Safety defaults:
  * DRY_RUN is ON unless the environment says otherwise. In dry-run mode the call is only logged.
  * No secret is written into this file. The group id, dataset id and access token come from
    environment variables (in GitHub or Airflow these are stored as secrets).
  * Nothing is sent unless the caller has already confirmed that the dbt tests passed.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from typing import Any

POWER_BI_API_BASE = "https://api.powerbi.com/v1.0/myorg"
FALSE_WORDS = ("false", "0", "no", "off")


class PowerBIConfigError(RuntimeError):
    """Raised when a real (non-dry-run) refresh is requested without the needed settings."""


def is_dry_run(env: Mapping[str, str]) -> bool:
    """Read DRY_RUN. Anything except an explicit 'false' keeps the safe dry-run mode on.

    Like a fire door that stays shut unless someone clearly opens it.
    """
    value = env.get("DRY_RUN")
    if value is None or value.strip() == "":
        return True
    return value.strip().lower() not in FALSE_WORDS


def refresh_url(group_id: str, dataset_id: str) -> str:
    """The Power BI REST address that starts a dataset refresh."""
    return f"{POWER_BI_API_BASE}/groups/{group_id}/datasets/{dataset_id}/refreshes"


def trigger_refresh(
    env: Mapping[str, str],
    tests_passed: bool,
    log: Callable[[str], None] = print,
    opener: Callable[..., Any] = urllib.request.urlopen,
) -> dict[str, Any]:
    """Start the Power BI import refresh, or log what would have happened in dry-run mode.

    Args:
        env: environment variables (os.environ in production code, a dict in tests).
        tests_passed: must be True. When False the refresh is blocked (the circuit breaker).
        log: where messages go (print by default, the Airflow logger in the DAG).
        opener: the function that sends HTTP requests (replaced by a fake in tests).

    Returns:
        A small dictionary with "outcome" set to triggered, dry_run or blocked.
    """
    if not tests_passed:
        log("Circuit breaker closed: dbt tests did not pass, so the Power BI refresh is BLOCKED.")
        return {"outcome": "blocked"}

    group_id = env.get("POWERBI_GROUP_ID", "<POWERBI_GROUP_ID not set>")
    dataset_id = env.get("POWERBI_DATASET_ID", "<POWERBI_DATASET_ID not set>")
    url = refresh_url(group_id, dataset_id)
    body = {"notifyOption": "MailOnFailure"}

    if is_dry_run(env):
        # Dry run: show the exact call that would be made, but send nothing.
        log(f"DRY_RUN is on. Would POST {url} with body {json.dumps(body)}. No request was sent.")
        return {"outcome": "dry_run"}

    token = env.get("POWERBI_ACCESS_TOKEN", "").strip()
    if not group_id or group_id.startswith("<") or not dataset_id or dataset_id.startswith("<") or not token:
        raise PowerBIConfigError(
            "Real refresh requested but POWERBI_GROUP_ID, POWERBI_DATASET_ID or POWERBI_ACCESS_TOKEN is missing."
        )

    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with opener(request, timeout=30) as response:
            # Power BI answers 202 Accepted when the refresh has been queued.
            log(f"Power BI accepted the refresh request (HTTP {response.status}).")
    except urllib.error.HTTPError as error:
        log(f"Power BI refused the refresh request (HTTP {error.code}).")
        return {"outcome": "failed"}
    return {"outcome": "triggered"}
