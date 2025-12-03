# =============================
# GIS PROCESSOR (FAKE VERSION)
# =============================

from typing import Any, Dict, Optional, Tuple

def _parse_point_from_geom(parcel_geom: Any) -> Tuple[Optional[float], Optional[float]]:
    if isinstance(parcel_geom, (list, tuple)) and len(parcel_geom) == 2:
        try:
            lon = float(parcel_geom[0])
            lat = float(parcel_geom[1])
            return lat, lon
        except:
            return None, None

    if isinstance(parcel_geom, dict):
        if "lat" in parcel_geom and "lon" in parcel_geom:
            try:
                return float(parcel_geom["lat"]), float(parcel_geom["lon"])
            except:
                return None, None

        if parcel_geom.get("type") == "Point":
            coords = parcel_geom.get("coordinates", [])
            if len(coords) == 2:
                try:
                    return float(coords[1]), float(coords[0])
                except:
                    return None, None

    if isinstance(parcel_geom, str) and parcel_geom.upper().startswith("POINT("):
        body = parcel_geom[6:-1].split()
        if len(body) == 2:
            try:
                lon = float(body[0])
                lat = float(body[1])
                return lat, lon
            except:
                return None, None

    return None, None


def _classify_land_cover(state: str, county: str, lat: Optional[float]) -> str:
    s = (state or "").lower()
    if "michigan" in s:
        if lat and lat > 44.5:
            return "forest"
        return "mixed_agriculture"
    if "iowa" in s or "illinois" in s:
        return "cropland"
    if "texas" in s:
        return "rangeland"
    return "cropland"


def _estimate_slope_percent(lat: Optional[float], lon: Optional[float], state: str) -> float:
    if lat is None or lon is None:
        return 2.0
    base = 2.0
    if state.lower() in {"colorado", "montana", "utah", "wyoming"}:
        base = 8.0
    return base + (abs(lon) % 3)


def _estimate_hydric_percent(state: str, county: str) -> float:
    key = (state + county).lower()
    return float((sum(ord(c) for c in key) % 60))


def _classify_nwi(lat: Optional[float], hydric: float) -> str:
    if hydric > 40: return "PEM1A"
    if hydric > 20: return "PUBH"
    return "NONE"


def _estimate_floodplain(hydric: float, slope: float) -> bool:
    return hydric > 35 and slope < 4


def _estimate_distance_to_stream_m(lat: Optional[float], lon: Optional[float]) -> float:
    if lat is None or lon is None:
        return 500
    return 100 + (abs(lat * lon) % 900)


def process_parcel_geometry(parcel_geom: Any, state: str, county: str) -> Dict[str, Any]:
    lat, lon = _parse_point_from_geom(parcel_geom)

    land_cover = _classify_land_cover(state, county, lat)
    slope = _estimate_slope_percent(lat, lon, state)
    hydric = _estimate_hydric_percent(state, county)
    nwi = _classify_nwi(lat, hydric)
    flood = _estimate_floodplain(hydric, slope)
    dist = _estimate_distance_to_stream_m(lat, lon)

    return {
        "lat": lat,
        "lon": lon,
        "land_cover": land_cover,
        "slope_percent": float(round(slope, 1)),
        "hydric_percent": float(round(hydric, 1)),
        "nwi_class": nwi,
        "in_100yr_floodplain": flood,
        "distance_to_stream_m": float(round(dist, 1)),
    }
