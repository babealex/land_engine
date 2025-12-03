import os
import sqlite3
import csv
from io import StringIO
from typing import List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query, UploadFile, File
from fastapi.responses import Response
from pydantic import BaseModel, Field

from .services.gis_client import fetch_terrain


# Program models
from .programs.models_eqip import EqipQuoteResponse
from .programs.models_crp import CrpQuoteResponse
from .programs.models_csp import CspQuoteResponse

# Service layers
from .services.eqip_quote import quote_eqip
from .services.crp_quote import quote_crp
from .services.crp_schedule import list_crp_counties_for_state
from .services.csp_quote import quote_csp

# Engine layers
from .engine.gis_processor import process_parcel_geometry
from .engine.eligibility_crp import build_crp_rule_map, eligible_crp_practices
from app.engine.gis_processor import process_parcel_geometry


# =======================================================
# Paths and database setup
# =======================================================

BASE_DIR = os.path.dirname(__file__)

# SQLite DB lives one level up: root/parcels.db
DB_PATH = os.path.abspath(os.path.join(BASE_DIR, "..", "parcels.db"))

# Single shared connection for this process (FastAPI dev / small-scale use)
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row


def init_db() -> None:
    """
    Ensure the parcels table exists.
    """
    with conn:
        conn.execute(
            """
           CREATE TABLE IF NOT EXISTS parcels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state TEXT NOT NULL,
    county TEXT NOT NULL,
    acres REAL NOT NULL,
    purchase_price_per_acre REAL NOT NULL,
    expected_payment_per_acre_year1 REAL NOT NULL,
    risk_score REAL NOT NULL,
    las_score REAL NOT NULL,
    expected_year1_payout REAL NOT NULL,
    raw_yield_percent REAL NOT NULL,
    lat REAL,
    lon REAL,
    land_cover TEXT,
    slope_percent REAL,
    hydric_percent REAL,
    nwi_class TEXT,
    in_100yr_floodplain INTEGER,
    distance_to_stream_m REAL
);

            """
        )


# =======================================================
# Pydantic models (parcels + stats)
# =======================================================

class ParcelBase(BaseModel):
    state: str
    county: str
    acres: float = Field(..., gt=0)
    purchase_price_per_acre: float = Field(..., gt=0)
    expected_payment_per_acre_year1: float = Field(..., ge=0)
    risk_score: float = Field(..., ge=0, le=1)

    # Optional GIS fields (can be null in DB)
    lat: Optional[float] = None
    lon: Optional[float] = None
    land_cover: Optional[str] = None
    slope_percent: Optional[float] = None
    hydric_percent: Optional[float] = None
    nwi_class: Optional[str] = None
    in_100yr_floodplain: Optional[bool] = None
    distance_to_stream_m: Optional[float] = None



class ParcelCreate(ParcelBase):
    pass


class ParcelUpdate(BaseModel):
    state: Optional[str] = None
    county: Optional[str] = None
    acres: Optional[float] = Field(None, gt=0)
    purchase_price_per_acre: Optional[float] = Field(None, gt=0)
    expected_payment_per_acre_year1: Optional[float] = Field(None, ge=0)
    risk_score: Optional[float] = Field(None, ge=0, le=1)


class ParcelOut(ParcelBase):
    id: int
    las_score: float
    expected_year1_payout: float
    raw_yield_percent: float

    # Remote GIS (DigitalOcean) extras – not stored in DB
    gis_elevation: Optional[float] = None
    gis_slope: Optional[float] = None



class CountyStats(BaseModel):
    state: str
    county: str
    parcel_count: int
    avg_las_score: float
    avg_raw_yield_percent: float
    total_expected_year1_payout: float
    

class ProgramsQuoteAllResponse(BaseModel):
    """
    Combined quote across CRP, EQIP, and CSP for a given state/county/acres.
    Each nested object is the same shape as the standalone /crp/quote,
/eqip/quote, /csp/quote responses.
    """
    state: str
    county: str
    acres: float

    crp: CrpQuoteResponse
    eqip: EqipQuoteResponse
    csp: CspQuoteResponse
    

class GISAttributes(BaseModel):
    lat: Optional[float]
    lon: Optional[float]
    land_cover: str
    slope_percent: float
    hydric_percent: float
    nwi_class: str
    in_100yr_floodplain: bool
    distance_to_stream_m: float


class ProgramsQuoteAllGISResponse(BaseModel):
    state: str
    county: str
    acres: float

    gis: GISAttributes
    crp: CrpQuoteResponse
    eqip: EqipQuoteResponse
    csp: CspQuoteResponse

# =======================================================
# Core helpers
# =======================================================

def compute_metrics(
    acres: float,
    price_acre: float,
    pay_acre: float,
    risk: float,
) -> Tuple[float, float, float]:
    """
    Core LAS metrics for parcels:
      - expected payout (acres * pay_acre)
      - raw yield percent (pay_acre / price_acre * 100)
      - las_score = raw_yield_percent * (1 - risk) * 20
    """
    expected_payout = acres * pay_acre
    raw_yield_percent = (pay_acre / price_acre) * 100 if price_acre else 0.0
    las_score = raw_yield_percent * (1 - risk) * 20.0
    return (
        round(las_score, 2),
        round(expected_payout, 2),
        round(raw_yield_percent, 2),
    )


def row_to_parcel(row: sqlite3.Row) -> ParcelOut:
    """
    Convert a parcels row into a ParcelOut model.
    """
    return ParcelOut(
        id=row["id"],
        state=row["state"],
        county=row["county"],
        acres=row["acres"],
        purchase_price_per_acre=row["purchase_price_per_acre"],
        expected_payment_per_acre_year1=row["expected_payment_per_acre_year1"],
        risk_score=row["risk_score"],
        las_score=row["las_score"],
        expected_year1_payout=row["expected_year1_payout"],
        raw_yield_percent=row["raw_yield_percent"],
        lat=row["lat"],
        lon=row["lon"],
        land_cover=row["land_cover"],
        slope_percent=row["slope_percent"],
        hydric_percent=row["hydric_percent"],
        nwi_class=row["nwi_class"],
        in_100yr_floodplain=(
            bool(row["in_100yr_floodplain"]) if row["in_100yr_floodplain"] is not None else None
        ),
        distance_to_stream_m=row["distance_to_stream_m"],
    )



# Common CSV columns for LAS scoring
LAS_REQUIRED_COLUMNS = {
    "state",
    "county",
    "acres",
    "purchase_price_per_acre",
    "expected_payment_per_acre_year1",
    "risk_score",
}


# =======================================================
# FastAPI app
# =======================================================

app = FastAPI(title="LAS Parcel API", version="1.4")


@app.on_event("startup")
def on_startup() -> None:
    init_db()


@app.get("/")
def root():
    return {
        "status": "ok",
        "message": "LAS Parcel API running with SQLite + CRP/EQIP/CSP quotes",
    }


# =======================================================
# POST /parcels/single
# =======================================================

@app.post("/parcels/single", response_model=ParcelOut)
def create_single(parcel: ParcelCreate):
    # 1) LAS metrics
    las, payout, raw = compute_metrics(
        parcel.acres,
        parcel.purchase_price_per_acre,
        parcel.expected_payment_per_acre_year1,
        parcel.risk_score,
    )

    # 2) GIS enrichment (compute if we have coordinates, otherwise use any provided values)
    gis = {
        "lat": parcel.lat,
        "lon": parcel.lon,
        "land_cover": parcel.land_cover,
        "slope_percent": parcel.slope_percent,
        "hydric_percent": parcel.hydric_percent,
        "nwi_class": parcel.nwi_class,
        "in_100yr_floodplain": parcel.in_100yr_floodplain,
        "distance_to_stream_m": parcel.distance_to_stream_m,
    }

    if parcel.lat is not None and parcel.lon is not None:
        # Let the GIS engine fill/override these based on lat/lon + state/county
        auto = process_parcel_geometry(
            {"lat": parcel.lat, "lon": parcel.lon},
            parcel.state,
            parcel.county,
        )
        gis.update(auto)

    lat = gis.get("lat")
    lon = gis.get("lon")
    land_cover = gis.get("land_cover")
    slope_percent = gis.get("slope_percent")
    hydric_percent = gis.get("hydric_percent")
    nwi_class = gis.get("nwi_class")
    in_100yr = gis.get("in_100yr_floodplain")
    dist_stream = gis.get("distance_to_stream_m")

    # 3) Insert into DB (now including GIS columns)
    with conn:
        cur = conn.execute(
            """
            INSERT INTO parcels (
                state, county, acres,
                purchase_price_per_acre,
                expected_payment_per_acre_year1,
                risk_score,
                las_score,
                expected_year1_payout,
                raw_yield_percent,
                lat,
                lon,
                land_cover,
                slope_percent,
                hydric_percent,
                nwi_class,
                in_100yr_floodplain,
                distance_to_stream_m
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                parcel.state,
                parcel.county,
                parcel.acres,
                parcel.purchase_price_per_acre,
                parcel.expected_payment_per_acre_year1,
                parcel.risk_score,
                las,
                payout,
                raw,
                lat,
                lon,
                land_cover,
                slope_percent,
                hydric_percent,
                nwi_class,
                1 if in_100yr else 0 if in_100yr is not None else None,
                dist_stream,
            ),
        )
        new_id = cur.lastrowid

    row = conn.execute("SELECT * FROM parcels WHERE id = ?", (new_id,)).fetchone()
    parcel_out = row_to_parcel(row)

    # 4) Remote GIS (DigitalOcean) – attach elevation & slope to response only
    if parcel.lat is not None and parcel.lon is not None:
        terrain = fetch_terrain(lat=parcel.lat, lon=parcel.lon)
        parcel_out.gis_elevation = terrain.elevation
        parcel_out.gis_slope = terrain.slope_deg

    return parcel_out



# =======================================================
# POST /parcels (bulk)
# =======================================================

@app.post("/parcels", response_model=List[ParcelOut])
def create_bulk(parcels: List[ParcelCreate]):
    created: List[ParcelOut] = []

    with conn:
        for p in parcels:
            las, payout, raw = compute_metrics(
                p.acres,
                p.purchase_price_per_acre,
                p.expected_payment_per_acre_year1,
                p.risk_score,
            )

            cur = conn.execute(
                """
                INSERT INTO parcels (
                    state, county, acres,
                    purchase_price_per_acre,
                    expected_payment_per_acre_year1,
                    risk_score,
                    las_score,
                    expected_year1_payout,
                    raw_yield_percent
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    p.state,
                    p.county,
                    p.acres,
                    p.purchase_price_per_acre,
                    p.expected_payment_per_acre_year1,
                    p.risk_score,
                    las,
                    payout,
                    raw,
                ),
            )

            db_row = conn.execute(
                "SELECT * FROM parcels WHERE id = ?",
                (cur.lastrowid,),
            ).fetchone()

            created.append(row_to_parcel(db_row))

    return created


# =======================================================
# CSV IMPORTER: insert + score
# =======================================================

@app.post("/parcels/import_csv", response_model=List[ParcelOut])
async def import_parcels_csv(file: UploadFile = File(...)):
    """
    Import parcels from a CSV with headers:
    state, county, acres, purchase_price_per_acre,
    expected_payment_per_acre_year1, risk_score
    """
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a .csv")

    content = (await file.read()).decode("utf-8", errors="ignore")
    reader = csv.DictReader(StringIO(content))

    if not LAS_REQUIRED_COLUMNS.issubset(reader.fieldnames or []):
        raise HTTPException(
            status_code=400,
            detail=f"CSV must contain: {', '.join(sorted(LAS_REQUIRED_COLUMNS))}",
        )

    created: List[ParcelOut] = []

    with conn:
        for row in reader:
            try:
                state = row["state"]
                county = row["county"]
                acres = float(row["acres"])
                price_acre = float(row["purchase_price_per_acre"])
                pay_acre = float(row["expected_payment_per_acre_year1"])
                risk = float(row["risk_score"])

                las, payout, raw = compute_metrics(acres, price_acre, pay_acre, risk)

                cur = conn.execute(
                    """
                    INSERT INTO parcels (
                        state, county, acres,
                        purchase_price_per_acre,
                        expected_payment_per_acre_year1,
                        risk_score,
                        las_score,
                        expected_year1_payout,
                        raw_yield_percent
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (state, county, acres, price_acre, pay_acre, risk, las, payout, raw),
                )

                new_id = cur.lastrowid
                db_row = conn.execute(
                    "SELECT * FROM parcels WHERE id = ?",
                    (new_id,),
                ).fetchone()

                created.append(row_to_parcel(db_row))

            except Exception as e:
                # Log but keep importing remaining rows
                print(f"Skipping row in import_csv: {e} — {row!r}")
                continue

    if not created:
        raise HTTPException(status_code=400, detail="No valid rows imported")

    return created


# =======================================================
# CSV RANKER: score only, no DB write
# =======================================================

@app.post("/parcels/rank_csv")
async def rank_parcels_csv(file: UploadFile = File(...)):
    """
    Accept a CSV, compute LAS metrics for each row,
    and return a ranked CSV (no DB insert).
    """
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a .csv")

    content = (await file.read()).decode("utf-8", errors="ignore")
    reader = csv.DictReader(StringIO(content))

    if not LAS_REQUIRED_COLUMNS.issubset(reader.fieldnames or []):
        raise HTTPException(
            status_code=400,
            detail=f"CSV must contain: {', '.join(sorted(LAS_REQUIRED_COLUMNS))}",
        )

    rows_with_scores = []

    for row in reader:
        try:
            acres = float(row["acres"])
            price_acre = float(row["purchase_price_per_acre"])
            pay_acre = float(row["expected_payment_per_acre_year1"])
            risk = float(row["risk_score"])

            las, payout, raw = compute_metrics(acres, price_acre, pay_acre, risk)

            enriched = dict(row)
            enriched["las_score"] = las
            enriched["expected_year1_payout"] = payout
            enriched["raw_yield_percent"] = raw
            rows_with_scores.append(enriched)
        except Exception as e:
            print(f"Skipping row in rank_csv: {e} — {row!r}")
            continue

    if not rows_with_scores:
        raise HTTPException(status_code=400, detail="No valid rows in CSV")

    # Rank by LAS score
    rows_with_scores.sort(key=lambda r: r["las_score"], reverse=True)
    for i, r in enumerate(rows_with_scores, start=1):
        r["rank"] = i

    fieldnames = list(rows_with_scores[0].keys())
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows_with_scores)

    csv_text = output.getvalue()
    return Response(
        content=csv_text,
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="ranked_parcels.csv"'},
    )


# =======================================================
# GET /parcels
# =======================================================

@app.get("/parcels", response_model=List[ParcelOut])
def list_parcels():
    rows = conn.execute("SELECT * FROM parcels ORDER BY id ASC").fetchall()
    return [row_to_parcel(r) for r in rows]


# =======================================================
# GET /parcels/top
# =======================================================

@app.get("/parcels/top", response_model=List[ParcelOut])
def top_parcels(limit: int = Query(10, gt=0, le=100)):
    rows = conn.execute(
        "SELECT * FROM parcels ORDER BY las_score DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [row_to_parcel(r) for r in rows]


# =======================================================
# GET /parcels/{id}
# =======================================================

@app.get("/parcels/{parcel_id}", response_model=ParcelOut)
def get_parcel(parcel_id: int):
    row = conn.execute(
        "SELECT * FROM parcels WHERE id = ?",
        (parcel_id,),
    ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Parcel not found")

    return row_to_parcel(row)


# =======================================================
# COUNTY STATS
# =======================================================

@app.get("/parcels/county_stats", response_model=List[CountyStats])
def county_stats(state: Optional[str] = Query(None, description="Filter by state")):
    params: List[str] = []
    where_clause = ""

    if state:
        where_clause = "WHERE state = ?"
        params.append(state)

    sql = f"""
        SELECT
            state,
            county,
            COUNT(*) AS parcel_count,
            AVG(las_score) AS avg_las_score,
            AVG(raw_yield_percent) AS avg_raw_yield_percent,
            SUM(expected_year1_payout) AS total_expected_year1_payout
        FROM parcels
        {where_clause}
        GROUP BY state, county
        ORDER BY avg_las_score DESC
    """

    rows = conn.execute(sql, params).fetchall()

    stats: List[CountyStats] = []
    for row in rows:
        stats.append(
            CountyStats(
                state=row["state"],
                county=row["county"],
                parcel_count=row["parcel_count"],
                avg_las_score=round(row["avg_las_score"], 2)
                if row["avg_las_score"] is not None
                else 0.0,
                avg_raw_yield_percent=round(row["avg_raw_yield_percent"], 2)
                if row["avg_raw_yield_percent"] is not None
                else 0.0,
                total_expected_year1_payout=round(row["total_expected_year1_payout"], 2)
                if row["total_expected_year1_payout"] is not None
                else 0.0,
            )
        )

    return stats


# =======================================================
# UPDATE /parcels/{id}
# =======================================================

@app.put("/parcels/{parcel_id}", response_model=ParcelOut)
def update_parcel(parcel_id: int, updates: ParcelUpdate):
    row = conn.execute("SELECT * FROM parcels WHERE id = ?", (parcel_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Parcel not found")

    state = updates.state or row["state"]
    county = updates.county or row["county"]
    acres = updates.acres if updates.acres is not None else row["acres"]
    price_acre = (
        updates.purchase_price_per_acre
        if updates.purchase_price_per_acre is not None
        else row["purchase_price_per_acre"]
    )
    pay_acre = (
        updates.expected_payment_per_acre_year1
        if updates.expected_payment_per_acre_year1 is not None
        else row["expected_payment_per_acre_year1"]
    )
    risk = (
        updates.risk_score
        if updates.risk_score is not None
        else row["risk_score"]
    )

    las, payout, raw = compute_metrics(acres, price_acre, pay_acre, risk)

    with conn:
        conn.execute(
            """
            UPDATE parcels
            SET state = ?, county = ?, acres = ?, purchase_price_per_acre = ?,
                expected_payment_per_acre_year1 = ?, risk_score = ?,
                las_score = ?, expected_year1_payout = ?, raw_yield_percent = ?
            WHERE id = ?
            """,
            (state, county, acres, price_acre, pay_acre, risk, las, payout, raw, parcel_id),
        )

    updated = conn.execute(
        "SELECT * FROM parcels WHERE id = ?",
        (parcel_id,),
    ).fetchone()

    return row_to_parcel(updated)


# =======================================================
# DELETE single /parcels/{id}
# =======================================================

@app.delete("/parcels/{parcel_id}")
def delete_parcel(parcel_id: int):
    with conn:
        cur = conn.execute("DELETE FROM parcels WHERE id = ?", (parcel_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Parcel not found")

    return {"deleted": True, "id": parcel_id}


# =======================================================
# DELETE /parcels/reset
# =======================================================

@app.delete("/parcels/reset")
def reset_parcels():
    with conn:
        cur = conn.execute("DELETE FROM parcels")
        deleted = cur.rowcount
    return {"deleted_rows": deleted}


# =======================================================
# GIS
# =======================================================

@app.get("/programs/quote_all_gis", response_model=ProgramsQuoteAllGISResponse)
def programs_quote_all_gis(
    state: str = Query(..., description="State abbrev or name, e.g. MI or Michigan"),
    county: str = Query(..., description="County name (case-insensitive, 'County' suffix optional)"),
    acres: float = Query(..., gt=0, description="Acres you are considering"),
    lat: float = Query(..., description="Latitude of parcel centroid"),
    lon: float = Query(..., description="Longitude of parcel centroid"),
):
    """
    One-shot GIS + program quote, with GIS-aware filtering:

    1. Run GIS on (lat, lon, state, county)
    2. Get CRP, EQIP, CSP quotes for state/county/acres
    3. Filter those quotes using GIS attributes
    """

    # 1) GIS
    gis_dict = process_parcel_geometry({"lat": lat, "lon": lon}, state, county)
    gis = GISAttributes(**gis_dict)

    # 2) Raw program quotes
    crp_raw = quote_crp(state=state, county=county, acres=acres)
    eqip_raw = quote_eqip(state=state, county=county, acres=acres)
    csp_raw = quote_csp(state=state, county=county, acres=acres)

    # 3) GIS-aware filtering
    crp_filtered = filter_crp_by_gis(crp_raw, gis)
    eqip_filtered = filter_eqip_by_gis(eqip_raw, gis)
    csp_filtered = filter_csp_by_gis(csp_raw, gis)

    return ProgramsQuoteAllGISResponse(
        state=state,
        county=county,
        acres=acres,
        gis=gis,
        crp=crp_filtered,
        eqip=eqip_filtered,
        csp=csp_filtered,
    )

# =======================================================
# GIS-based filtering for program practices
# =======================================================

def _is_wetland_like_name(name: str) -> bool:
    n = name.lower()
    wet_keywords = ["wetland", "marsh", "bog", "floodplain", "riparian", "stream buffer"]
    return any(k in n for k in wet_keywords)


def _is_erosion_like_name(name: str) -> bool:
    n = name.lower()
    eros_keywords = ["erosion", "gully", "terrace", "waterway", "grade stabilization", "diversion"]
    return any(k in n for k in eros_keywords)


def _is_pasture_like_name(name: str) -> bool:
    n = name.lower()
    pasture_keywords = ["pasture", "grazing", "rangeland", "forage"]
    return any(k in n for k in pasture_keywords)


def filter_crp_by_gis(crp: CrpQuoteResponse, gis: GISAttributes) -> CrpQuoteResponse:
    """
    GIS-aware and rule-aware filter for CRP:

    1. Build a parcel dict that includes GIS attributes.
    2. Use CRP eligibility rules (if any) to restrict to eligible practice codes.
    3. Apply simple GIS heuristics:
       - If land_cover is 'developed', drop everything.
       - On obviously dry upland (no wetland signal, low hydric), drop clearly wetland-focused practices.
    """
    if crp is None:
        return crp

    # If the site is obviously developed, assume no CRP enrollment.
    if gis.land_cover.lower() == "developed":
        return CrpQuoteResponse(
            state=crp.state,
            county=crp.county,
            acres=crp.acres,
            practices=[],
        )

    # Build parcel dict for rules
    parcel = {
        "state": crp.state,
        "county": crp.county,
        "acres": crp.acres,
        "land_cover": gis.land_cover,
        "slope_percent": gis.slope_percent,
        "hydric_percent": gis.hydric_percent,
        "nwi_class": gis.nwi_class,
        "in_100yr_floodplain": gis.in_100yr_floodplain,
        "distance_to_stream_m": gis.distance_to_stream_m,
    }

    rule_map = get_crp_rule_map()
    eligible_codes = eligible_crp_practices(parcel, rule_map) if rule_map else None

    # Simple dryness check for additional filtering
    dry_upland = (gis.nwi_class == "NONE") and (gis.hydric_percent < 20.0)

    filtered = []
    for p in crp.practices:
        # 1) If rules are defined, skip practices that are not eligible.
        if eligible_codes is not None and len(eligible_codes) > 0:
            if p.crp_practice_code not in eligible_codes:
                continue

        name = p.crp_practice_name or ""

        # 2) Drop obviously wetland-focused practices on very dry upland sites.
        if dry_upland and _is_wetland_like_name(name):
            continue

        filtered.append(p)

    return CrpQuoteResponse(
        state=crp.state,
        county=crp.county,
        acres=crp.acres,
        practices=filtered,
    )



def filter_eqip_by_gis(eqip: EqipQuoteResponse, gis: GISAttributes) -> EqipQuoteResponse:
    """
    Simple GIS-aware filter for EQIP:
    - Wetland/stream practices only if there's some wetland signal.
    - Erosion-focused practices only if slope is meaningful.
    """

    if eqip is None:
        return eqip

    has_wet_signal = (gis.nwi_class != "NONE") or (gis.hydric_percent >= 20.0)
    low_slope = gis.slope_percent < 2.0

    filtered = []
    for p in eqip.practices:
        name = p.scenario_name or ""

        if _is_wetland_like_name(name) and not has_wet_signal:
            # Drop wetland practices on obviously dry sites
            continue

        if _is_erosion_like_name(name) and low_slope:
            # Drop erosion-control practices on basically flat land
            continue

        filtered.append(p)

    return EqipQuoteResponse(
        state=eqip.state,
        county=eqip.county,
        acres=eqip.acres,
        practices=filtered,
    )


def filter_csp_by_gis(csp: CspQuoteResponse, gis: GISAttributes) -> CspQuoteResponse:
    """
    Simple GIS-aware filter for CSP:
    - Same basic rules as EQIP for now (wetland + erosion driven).
    """

    if csp is None:
        return csp

    has_wet_signal = (gis.nwi_class != "NONE") or (gis.hydric_percent >= 20.0)
    low_slope = gis.slope_percent < 2.0

    filtered = []
    for p in csp.practices:
        name = p.scenario_name or ""

        if _is_wetland_like_name(name) and not has_wet_signal:
            continue

        if _is_erosion_like_name(name) and low_slope:
            continue

        filtered.append(p)

    return CspQuoteResponse(
        state=csp.state,
        county=csp.county,
        acres=csp.acres,
        practices=filtered,
    )

# =======================================================
# CRP eligibility rules (Excel-backed)
# =======================================================

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "data")
CRP_MASTER_XLSX = os.path.join(DATA_DIR, "USDA_CONSERVATION_MASTER.xlsx")
CRP_RULES_SHEET = "CRP_eligibility_rules"

_crp_rule_map = None

def get_crp_rule_map():
    """
    Lazy-load and cache the CRP eligibility rule map from the Excel workbook.

    If the sheet is missing or not in the expected format, an empty map is returned,
    and CRP eligibility rules are effectively disabled (no additional filtering).
    """
    global _crp_rule_map
    if _crp_rule_map is not None:
        return _crp_rule_map

    try:
        df_rules = pd.read_excel(CRP_MASTER_XLSX, sheet_name=CRP_RULES_SHEET)
    except Exception:
        _crp_rule_map = {}
        return _crp_rule_map

    _crp_rule_map = build_crp_rule_map(df_rules)
    return _crp_rule_map


# =======================================================
# CRP utilities (Excel-backed via services)
# =======================================================

@app.get("/crp/counties")
def crp_counties(
    state: str = Query(..., description="State abbrev or full name, e.g. MI or Michigan"),
):
    """
    List all counties in the CRP schedule for a given state.
    """
    state_abbr, counties = list_crp_counties_for_state(state)

    if state_abbr is None or not counties:
        raise HTTPException(
            status_code=404,
            detail="No CRP entries found for this state.",
        )

    return {"state": state_abbr, "counties": counties, "count": len(counties)}


# =======================================================
# CRP QUOTE (Excel-backed, no DB write)
# =======================================================

@app.get("/crp/quote", response_model=CrpQuoteResponse)
def crp_quote(
    state: str = Query(..., description="State abbrev or name, e.g. MI or Michigan"),
    county: str = Query(..., description="County name (case-insensitive, no 'County' needed)"),
    acres: float = Query(..., gt=0, description="Acres you are considering"),
):
    """
    Return CRP rental revenue estimates for a given state/county/acres,
    using Excel-backed CRP schedule (via services.crp_quote).
    """
    quote = quote_crp(state=state, county=county, acres=acres)

    if not quote.practices:
        raise HTTPException(
            status_code=404,
            detail=(
                "No CRP rates found for this state/county. "
                "Use /crp/counties?state=XX to see valid counties."
            ),
        )

    return quote


# =======================================================
# EQIP QUOTE (Excel-backed, no DB write)
# =======================================================

@app.get("/eqip/quote", response_model=EqipQuoteResponse)
def eqip_quote(
    state: str = Query(..., description="State abbrev or name, e.g. MI or Michigan"),
    county: str = Query(..., description="County name (case-insensitive, no 'County' needed)"),
    acres: float = Query(..., gt=0, description="Acres you are considering"),
):
    """
    EQIP quote based on Excel-backed EQIP schedule (via services.eqip_quote).

    - Per-acre practices (unit in PER_ACRE_UNITS) scale with acres
    - Flat-rate / other units are treated as one-time payments
    """
    quote = quote_eqip(state=state, county=county, acres=acres)

    if not quote.practices:
        raise HTTPException(
            status_code=404,
            detail="No EQIP practices found for this state (and future: county).",
        )

    return quote


# =======================================================
# CSP QUOTE (Excel-backed, no DB write)
# =======================================================

@app.get("/csp/quote", response_model=CspQuoteResponse)
def csp_quote(
    state: str = Query(..., description="State abbrev or name, e.g. MI or Michigan"),
    county: str = Query(..., description="County name (case-insensitive, kept for symmetry)"),
    acres: float = Query(..., gt=0, description="Acres you are considering"),
):
    """
    CSP quote based on Excel-backed CSP schedule (via services.csp_quote).

    - Per-acre practices (unit in PER_ACRE_UNITS) scale with acres
    - Flat-rate / other units are treated as one-time payments

    Note: CSP is state-level in your spreadsheet, so `county` is currently
    passed through but not used in the lookup.
    """
    quote = quote_csp(state=state, county=county, acres=acres)

    # If no practices are found, just return an empty list instead of 404.
    # This keeps the API consistent with EQIP/CRP behavior.
    return quote

# =======================================================
# COMBINED PROGRAMS QUOTE (CRP + EQIP + CSP)
# =======================================================

@app.get("/programs/quote_all", response_model=ProgramsQuoteAllResponse)
def programs_quote_all(
    state: str = Query(..., description="State abbrev or name, e.g. MI or Michigan"),
    county: str = Query(..., description="County name (case-insensitive, 'County' suffix optional)"),
    acres: float = Query(..., gt=0, description="Acres you are considering"),
):
    """
    One-shot quote across all three programs for a given state/county/acres.

    This calls the same underlying service functions as:
      - GET /crp/quote
      - GET /eqip/quote
      - GET /csp/quote

    but does NOT raise 404s if a particular program has no practices —
    you just get an empty .practices list for that program.
    """

    crp_quote_res = quote_crp(state=state, county=county, acres=acres)
    eqip_quote_res = quote_eqip(state=state, county=county, acres=acres)
    csp_quote_res = quote_csp(state=state, county=county, acres=acres)

    return ProgramsQuoteAllResponse(
        state=state,
        county=county,
        acres=acres,
        crp=crp_quote_res,
        eqip=eqip_quote_res,
        csp=csp_quote_res,
    )
