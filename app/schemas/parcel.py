from pydantic import BaseModel

class ParcelInput(BaseModel):
    parcel_id: str
    state: str
    county: str
    acres: float

    land_cover: str = None
    land_cover_code: int = None
    slope_percent: float = None
    soil_hydric_percent: float = None
    soil_group: str = None
    floodplain: bool = None
    nwi_wetland_type: str = None
    distance_to_stream_m: float = None
    in_riparian_buffer: bool = None
    huc12: str = None

    crp_rate_GEN: float = None
    crp_rate_GRASS: float = None
