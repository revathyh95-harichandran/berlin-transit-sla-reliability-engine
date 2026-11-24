"""Load the official boundaries of Berlin's 12 districts (Bezirke).

Source: Berlin Geoportal, WFS layer "alkis_bezirke:bezirksgrenzen" (official cadastre data).
Each district shape is stored as GeoJSON text. The warehouse turns it back into a shape
and checks which district each station lies in (point-in-polygon), like putting a pin
on a paper map and seeing which coloured area it lands in.

The boundaries rarely change, so the table is fully replaced on each run.
A safety check refuses to load unless exactly 12 districts arrive, so a broken
download can never wipe out good data.

Run it:
    python -m ingestion.bezirke --target dev
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import dlt
import httpx

from ingestion.common import build_arg_parser, build_pipeline, utc_now

WFS_URL = "https://gdi.berlin.de/services/wfs/alkis_bezirke"
WFS_PARAMS = {
    "SERVICE": "WFS",
    "VERSION": "2.0.0",
    "REQUEST": "GetFeature",
    "TYPENAMES": "alkis_bezirke:bezirksgrenzen",
    "OUTPUTFORMAT": "application/json",
    "SRSNAME": "EPSG:4326",  # Ordinary GPS latitude/longitude
}
EXPECTED_DISTRICTS = 12


def fetch_bezirke(client: httpx.Client) -> dict[str, Any]:
    """Download the district boundaries as GeoJSON."""
    response = client.get(WFS_URL, params=WFS_PARAMS)
    response.raise_for_status()
    # Read the bytes as UTF-8 explicitly so names like "Neukölln" stay intact.
    return json.loads(response.content.decode("utf-8"))


def parse_bezirke(geojson: dict[str, Any], fetched_at: datetime) -> list[dict[str, Any]]:
    """Turn the GeoJSON features into one row per district, and check there are exactly 12."""
    rows = []
    for feature in geojson.get("features", []):
        properties = feature.get("properties") or {}
        name = (properties.get("namgem") or "").strip()
        geometry = feature.get("geometry")
        if not name or not geometry:
            continue
        rows.append({
            "bezirk_code": properties.get("gem"),  # "001" = Mitte ... "012" = Reinickendorf
            "bezirk_name": name,
            "geometry_geojson": json.dumps(geometry, ensure_ascii=False),
            "fetched_at_utc": fetched_at,
        })

    if len(rows) != EXPECTED_DISTRICTS:
        raise ValueError(f"expected {EXPECTED_DISTRICTS} districts, got {len(rows)}; not loading")
    return rows


def run(target: str, client: httpx.Client | None = None, local_db_path: Path | None = None,
        pipelines_dir: Path | None = None) -> dict[str, int]:
    """Fetch, check and load the district boundaries."""
    own_client = client is None
    if own_client:
        client = httpx.Client(timeout=60, headers={"User-Agent": "berlin-transit-sla-collector/1.0"})
    try:
        rows = parse_bezirke(fetch_bezirke(client), utc_now())
    finally:
        if own_client:
            client.close()

    pipeline = build_pipeline(target, "bezirke", local_db_path, pipelines_dir)
    pipeline.run(dlt.resource(rows, name="bezirke", write_disposition="replace"))
    return {"districts_loaded": len(rows)}


def main() -> int:
    args = build_arg_parser("Load Berlin district (Bezirk) boundaries.").parse_args()
    print(f"Bezirke load finished: {run(args.target)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
