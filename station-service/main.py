from pydantic import BaseModel
import os
import psycopg2
from fastapi import FastAPI, HTTPException

app = FastAPI(title="ChargeFlow Station Service")
class ChargerStatusUpdate(BaseModel):
    status: str

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("DB_NAME", "chargeflow"),
        user=os.getenv("DB_USER", "chargeflow"),
        password=os.environ["DB_PASSWORD"]
    )


@app.get("/")
def home():
    return {"message": "ChargeFlow Station Service is running"}


@app.get("/health")
def health():
    return {"status": "healthy", "service": "station"}


@app.get("/stations")
def get_stations():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT station_id, name, location FROM stations ORDER BY station_id"
                )
                rows = cursor.fetchall()

        return {
            "stations": [
                {"station_id": r[0], "name": r[1], "location": r[2]}
                for r in rows
            ]
        }
    except psycopg2.Error:
        raise HTTPException(
            status_code=503,
            detail="Station database temporarily unavailable"
        )


@app.get("/stations/{station_id}/chargers")
def get_chargers(station_id: int):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT charger_id, status
                    FROM chargers
                    WHERE station_id = %s
                    ORDER BY charger_id
                    """,
                    (station_id,)
                )
                rows = cursor.fetchall()

        return {
            "station_id": station_id,
            "chargers": [
                {"charger_id": r[0], "status": r[1]}
                for r in rows
            ]
        }
    except psycopg2.Error:
        raise HTTPException(
            status_code=503,
            detail="Charger database temporarily unavailable"
        )


@app.patch("/chargers/{charger_id}/status")
def update_charger_status(
    charger_id: int,
    update: ChargerStatusUpdate
):
    allowed_statuses = {
        "available", "reserved", "charging", "offline"
    }

    if update.status not in allowed_statuses:
        raise HTTPException(
            status_code=400,
            detail="Invalid charger status"
        )

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE chargers
                    SET status = %s
                    WHERE charger_id = %s
                    RETURNING charger_id, station_id, status
                    """,
                    (update.status, charger_id)
                )
                row = cursor.fetchone()

        if row is None:
            raise HTTPException(
                status_code=404,
                detail="Charger not found"
            )

        return {
            "charger_id": row[0],
            "station_id": row[1],
            "status": row[2],
            "message": "Charger status updated"
        }

    except psycopg2.Error:
        raise HTTPException(
            status_code=503,
            detail="Station database temporarily unavailable"
        )
