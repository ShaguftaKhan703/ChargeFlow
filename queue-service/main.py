
import os
import time

import httpx
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.errors import UniqueViolation
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="ChargeFlow Queue Service")

STATION_SERVICE_URL = os.getenv(
    "STATION_SERVICE_URL",
    "http://station-service:8000"
)


class JoinQueueRequest(BaseModel):
    user_id: int


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("DB_NAME", "chargeflow"),
        user=os.getenv("DB_USER", "chargeflow"),
        password=os.environ["DB_PASSWORD"],
    )


def call_station_service(path):
    url = f"{STATION_SERVICE_URL}{path}"

    for attempt in range(3):
        try:
            response = httpx.get(url, timeout=1.0)

            if response.status_code in (502, 503, 504):
                raise httpx.HTTPStatusError(
                    "Temporary upstream failure",
                    request=response.request,
                    response=response,
                )

            response.raise_for_status()
            return response.json()

        except (httpx.RequestError, httpx.HTTPStatusError):
            if attempt == 2:
                raise HTTPException(
                    status_code=503,
                    detail="Station Service temporarily unavailable",
                )
            time.sleep(0.2 * (attempt + 1))


@app.get("/")
def home():
    return {"message": "ChargeFlow Queue Service is running"}


@app.get("/health")
def health():
    return {"status": "healthy", "service": "queue"}


@app.get("/queues")
def get_queues():
    """List all drivers currently waiting in queues."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        q.queue_id,
                        q.user_id,
                        u.name AS user_name,
                        q.station_id,
                        q.joined_at,
                        q.status
                    FROM queue_entries q
                    JOIN users u ON u.user_id = q.user_id
                    WHERE q.status = 'waiting'
                    ORDER BY q.station_id, q.joined_at, q.queue_id
                    """
                )
                queues = cursor.fetchall()

        return {"queues": [dict(row) for row in queues]}

    except psycopg2.Error:
        raise HTTPException(
            status_code=503,
            detail="Queue database temporarily unavailable",
        )


@app.get("/queues/stations")
def get_station_information():
    return {
        "source": "Station Service",
        "data": call_station_service("/stations"),
    }


@app.get("/queues/stations/{station_id}/chargers")
def get_station_chargers(station_id: int):
    return {
        "source": "Station Service",
        "data": call_station_service(
            f"/stations/{station_id}/chargers"
        ),
    }


@app.get("/queues/{station_id}")
def get_station_queue(station_id: int):
    """View waiting drivers at one station in queue order."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT station_id FROM stations WHERE station_id = %s",
                    (station_id,),
                )
                if cursor.fetchone() is None:
                    raise HTTPException(
                        status_code=404,
                        detail="Station not found",
                    )

                cursor.execute(
                    """
                    SELECT
                        q.queue_id,
                        q.user_id,
                        u.name AS user_name,
                        q.station_id,
                        q.joined_at,
                        q.status
                    FROM queue_entries q
                    JOIN users u ON u.user_id = q.user_id
                    WHERE q.station_id = %s
                      AND q.status = 'waiting'
                    ORDER BY q.joined_at, q.queue_id
                    """,
                    (station_id,),
                )
                rows = cursor.fetchall()

        return {
            "station_id": station_id,
            "queue": [
                {**dict(row), "position": index}
                for index, row in enumerate(rows, start=1)
            ],
        }

    except psycopg2.Error:
        raise HTTPException(
            status_code=503,
            detail="Queue database temporarily unavailable",
        )


@app.post("/queues/{station_id}/join", status_code=201)
def join_queue(station_id: int, request: JoinQueueRequest):
    """Add a driver to a station's waiting queue."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT station_id FROM stations WHERE station_id = %s",
                    (station_id,),
                )
                if cursor.fetchone() is None:
                    raise HTTPException(
                        status_code=404,
                        detail="Station not found",
                    )

                cursor.execute(
                    "SELECT user_id FROM users WHERE user_id = %s",
                    (request.user_id,),
                )
                if cursor.fetchone() is None:
                    raise HTTPException(
                        status_code=404,
                        detail="User not found",
                    )

                cursor.execute(
                    """
                    INSERT INTO queue_entries (user_id, station_id)
                    VALUES (%s, %s)
                    RETURNING queue_id, user_id, station_id, joined_at, status
                    """,
                    (request.user_id, station_id),
                )
                entry = cursor.fetchone()

        return {
            "message": "Successfully joined queue",
            "queue_entry": dict(entry),
        }

    except UniqueViolation:
        raise HTTPException(
            status_code=409,
            detail="User is already waiting at this station",
        )
    except psycopg2.Error:
        raise HTTPException(
            status_code=503,
            detail="Queue database temporarily unavailable",
        )


@app.delete("/queues/{station_id}/leave")
def leave_queue(station_id: int, user_id: int):
    """Cancel a driver's waiting entry without deleting its history."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    UPDATE queue_entries
                    SET status = 'cancelled'
                    WHERE station_id = %s
                      AND user_id = %s
                      AND status = 'waiting'
                    RETURNING queue_id, user_id, station_id, joined_at, status
                    """,
                    (station_id, user_id),
                )
                entry = cursor.fetchone()

                if entry is None:
                    raise HTTPException(
                        status_code=404,
                        detail="No waiting queue entry found for this user at this station",
                    )

        return {
            "message": "Successfully left queue",
            "queue_entry": dict(entry),
        }

    except psycopg2.Error:
        raise HTTPException(
            status_code=503,
            detail="Queue database temporarily unavailable",
        )
