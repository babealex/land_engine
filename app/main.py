import os
import sqlite3
import csv
from io import StringIO
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, UploadFile, File
from fastapi.responses import Response
from pydantic import BaseModel, Field

# -------------------------------------------------------
# Database setup
# -------------------------------------------------------

BASE_DIR = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE_DIR, "..", "parcels.db")

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row


def init_db():
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
                raw_yield_percent REAL NOT NULL
            );
            """
        )


# -------------------------------------------------------
# Pydantic models
# -------------------------------------------------------

class ParcelBase(BaseModel):
    state: str
    county: str
    acres: float = Field(..., gt=0)
    purchase_price_per_acre: float = Field(..., gt=0)
    expected_payment_per_acre_year1: float = Field(..., ge=0)
    risk_score: float = Field(..., ge=0, le=1)


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


class CountyStats(BaseModel):
    state: str
    county: str
    parcel_count: int
    avg_las_score: float
    avg_raw_yield_percent: float
    total_expected_year1_payout: float


# -------------------------------------------------------
# Helpers
# -------------------------------------------------------

def compute_metrics(acres, price_acre, pay_acre, risk):
    expected_payout = acres * pay_acre
    raw_yield_percent = (pay_acre / price_acre) * 100 if price_acre else 0.0
    las_score = raw_yield_percent * (1 - risk) * 20.0
    return (
        round(las_score, 2),
        round(expected_payout, 2),
        round(raw_yield_percent, 2),
    )


def row_to_parcel(row):
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
    )


# -------------------------------------------------------
# FastAPI app
# -------------------------------------------------------

app = FastAPI(title="LAS Parcel API", version="1.1")


@app.on_event("startup")
def startup():
    init_db()


@app.get("/")
def root():
    return {"status": "ok", "message": "LAS Parcel API running with SQLite"}


# -------------------------------------------------------
# POST /parcels/single
# -------------------------------------------------------

@app.post("/parcels/single", response_model=ParcelOut)
def create_single(parcel: ParcelCreate):
    las, payout, raw = compute_metrics(
        parcel.acres,
        parcel.purchase_price_per_acre,
        parcel.expected_payment_per_acre_year1,
        parcel.risk_score,
    )

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
                raw_yield_percent
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ),
        )
        new_id = cur.lastrowid

    row = conn.execute("SELECT * FROM parcels WHERE id=?", (new_id,)).fetchone()
    return row_to_parcel(row)


# -------------------------------------------------------
# POST /parcels (bulk)
# -------------------------------------------------------

@app.post("/parcels", response_model=List[ParcelOut])
def create_bulk(parcels: List[ParcelCreate]):
    created = []

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
                "SELECT * FROM parcels WHERE id=?",
                (cur.lastrowid,),
            ).fetchone()

            created.append(row_to_parcel(db_row))

    return created


# -------------------------------------------------------
# CSV IMPORTER: insert + score
# -------------------------------------------------------

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

    required = {
        "state",
        "county",
        "acres",
        "purchase_price_per_acre",
        "expected_payment_per_acre_year1",
        "risk_score",
    }

    if not required.issubset(reader.fieldnames or []):
        raise HTTPException(
            status_code=400,
            detail=f"CSV must contain: {', '.join(sorted(required))}",
        )

    created = []

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
                    "SELECT * FROM parcels WHERE id=?",
                    (new_id,),
                ).fetchone()

                created.append(row_to_parcel(db_row))

            except Exception as e:
                print(f"Skipping row: {e} — {row!r}")
                continue

    if not created:
        raise HTTPException(status_code=400, detail="No valid rows imported")

    return created


# -------------------------------------------------------
# CSV RANKER: score only, no DB write
# -------------------------------------------------------

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

    required = {
        "state",
        "county",
        "acres",
        "purchase_price_per_acre",
        "expected_payment_per_acre_year1",
        "risk_score",
    }

    if not required.issubset(reader.fieldnames or []):
        raise HTTPException(
            status_code=400,
            detail=f"CSV must contain: {', '.join(sorted(required))}",
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

    # Sort by LAS descending
    rows_with_scores.sort(key=lambda r: r["las_score"], reverse=True)

    # Add rank
    for i, r in enumerate(rows_with_scores, start=1):
        r["rank"] = i

    # Build CSV output
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


# -------------------------------------------------------
# GET /parcels
# -------------------------------------------------------

@app.get("/parcels", response_model=List[ParcelOut])
def list_parcels():
    rows = conn.execute("SELECT * FROM parcels ORDER BY id ASC").fetchall()
    return [row_to_parcel(r) for r in rows]


# -------------------------------------------------------
# GET /parcels/top
# -------------------------------------------------------

@app.get("/parcels/top", response_model=List[ParcelOut])
def top_parcels(limit: int = Query(10, gt=0, le=100)):
    rows = conn.execute(
        "SELECT * FROM parcels ORDER BY las_score DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [row_to_parcel(r) for r in rows]


# -------------------------------------------------------
# GET /parcels/{id}
# -------------------------------------------------------

@app.get("/parcels/{parcel_id}", response_model=ParcelOut)
def get_parcel(parcel_id: int):
    row = conn.execute(
        "SELECT * FROM parcels WHERE id=?",
        (parcel_id,),
    ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Parcel not found")

    return row_to_parcel(row)


# -------------------------------------------------------
# COUNTY STATS
# -------------------------------------------------------

@app.get("/parcels/county_stats", response_model=List[CountyStats])
def county_stats(state: Optional[str] = Query(None, description="Filter by state")):
    params = []
    where = ""
    if state:
        where = "WHERE state = ?"
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
        {where}
        GROUP BY state, county
        ORDER BY avg_las_score DESC
    """

    rows = conn.execute(sql, params).fetchall()

    return [
        CountyStats(
            state=row["state"],
            county=row["county"],
            parcel_count=row["parcel_count"],
            avg_las_score=round(row["avg_las_score"], 2) if row["avg_las_score"] is not None else 0.0,
            avg_raw_yield_percent=round(row["avg_raw_yield_percent"], 2) if row["avg_raw_yield_percent"] is not None else 0.0,
            total_expected_year1_payout=round(row["total_expected_year1_payout"], 2) if row["total_expected_year1_payout"] is not None else 0.0,
        )
        for row in rows
    ]


# -------------------------------------------------------
# UPDATE
# -------------------------------------------------------

@app.put("/parcels/{parcel_id}", response_model=ParcelOut)
def update_parcel(parcel_id: int, updates: ParcelUpdate):
    row = conn.execute("SELECT * FROM parcels WHERE id=?", (parcel_id,)).fetchone()
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
    risk = updates.risk_score if updates.risk_score is not None else row["risk_score"]

    las, payout, raw = compute_metrics(acres, price_acre, pay_acre, risk)

    with conn:
        conn.execute(
            """
            UPDATE parcels
            SET state=?, county=?, acres=?, purchase_price_per_acre=?,
                expected_payment_per_acre_year1=?, risk_score=?,
                las_score=?, expected_year1_payout=?, raw_yield_percent=?
            WHERE id=?
            """,
            (state, county, acres, price_acre, pay_acre, risk, las, payout, raw, parcel_id),
        )

    updated = conn.execute(
        "SELECT * FROM parcels WHERE id=?", (parcel_id,)
    ).fetchone()

    return row_to_parcel(updated)


# -------------------------------------------------------
# DELETE single
# -------------------------------------------------------

@app.delete("/parcels/{parcel_id}")
def delete_parcel(parcel_id: int):
    with conn:
        cur = conn.execute("DELETE FROM parcels WHERE id=?", (parcel_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="Parcel not found")

    return {"deleted": True, "id": parcel_id}


# -------------------------------------------------------
# DELETE /parcels/reset
# -------------------------------------------------------

@app.delete("/parcels/reset")
def reset_parcels():
    with conn:
        cur = conn.execute("DELETE FROM parcels")
        deleted = cur.rowcount
    return {"deleted_rows": deleted}
