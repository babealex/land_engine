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
        resp.raise_for_status()
        data = resp.json()

        # Debugging – you can keep or remove later
        print("DEBUG Terrain JSON keys:", list(data.keys()))
        print("DEBUG Terrain JSON sample:", {
            k: data.get(k) for k in list(data.keys())[:6]
        })
    except Exception as e:
        print(f"GIS fetch failed: {e}")
        return TerrainAttributes()

    # Elevation: now includes elevation_m
    elev = None
    for key in (
        "elevation_m",
        "elevation_raw",
        "elevation",
        "elev",
        "elev_m",
        "z",
    ):
        if key in data and data[key] is not None:
            elev = data[key]
            break

    # Slope: slope_deg is already what your API returns
    slope = None
    for key in (
        "slope_deg",
        "slope",
        "slope_raw",
        "slope_degrees",
    ):
        if key in data and data[key] is not None:
            slope = data[key]
            break

    return TerrainAttributes(
        elevation=elev,
        slope_deg=slope,
    )
