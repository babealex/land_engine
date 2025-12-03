# app/services/gis_client.py

from pydantic import BaseModel
import httpx

class TerrainAttributes(BaseModel):
    elevation: float | None = None
    slope_deg: float | None = None


GIS_API_BASE = "http://146.190.136.189:8000"  # DigitalOcean GIS API base URL


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
        # TEMP: debug logging so we can see what's happening in Render logs
        print("GIS API response:", resp.status_code, resp.text[:200])
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        # TEMP: log the error in Render logs
        print(f"GIS fetch failed: {e}")
        return TerrainAttributes()

    elev = (
        data.get("elevation_raw")
        or data.get("elevation")
        or data.get("elev")
    )
    slope = (
        data.get("slope_deg")
        or data.get("slope")
        or data.get("slope_raw")
    )

    return TerrainAttributes(
        elevation=elev,
        slope_deg=slope,
    )
