# app/services/gis_client.py

import httpx
from pydantic import BaseModel

class TerrainAttributes(BaseModel):
    elevation: float | None = None
    slope_deg: float | None = None

GIS_API_BASE = "http://146.190.136.189:8000"  # your DigitalOcean GIS API

def fetch_terrain(lat: float, lon: float) -> TerrainAttributes:
    """
    Call the remote GIS API to get basic terrain attributes
    for this lat/lon. Synchronous on purpose so we don't
    have to change your FastAPI endpoints to async.
    """
    url = f"{GIS_API_BASE}/debug/terrain"
    params = {"lat": lat, "lon": lon}

    try:
        resp = httpx.get(url, params=params, timeout=8.0)
        resp.raise_for_status()
        data = resp.json()

        print("[GIS DEBUG] terrain response:", data)

        # Try explicit keys first
        elev = (
            data.get("elevation_raw")
            or data.get("elevation")
            or data.get("elev")
        )

        # Fallback: any numeric field with 'elev' in the name
        if elev is None:
            for k, v in data.items():
                if "elev" in k.lower():
                    if isinstance(v, (int, float)):
                        elev = float(v)
                        break

        return TerrainAttributes(
            elevation=elev,
            slope_deg=data.get("slope_deg"),
        )

    except Exception as e:
        print(f"[GIS ERROR] {e!r}")
        # If the GIS service is down or times out, don't break the app
        return TerrainAttributes()
