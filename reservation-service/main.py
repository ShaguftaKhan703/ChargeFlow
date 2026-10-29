import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
import psycopg2
from psycopg2 import errors, pool
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, model_validator

app = FastAPI(title="ChargeFlow Reservation Service")

STATION_SERVICE_URL = os.getenv(
    "STATION_SERVICE_URL",
    "http://station-service:8000"
)

MAX_DURATION = timedelta(hours=8)
START_GRACE = timedelta(minutes=1)

COLUMNS = "reservation_id, user_id, charger_id, start_time, end_time, status"

POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))
POOL_WAIT_SECONDS = 5

_pool = None
# ThreadedConnectionPool raises when exhausted instead of waiting, so
# requests queue on this semaphore for a free connection.
_slots = threading.BoundedSemaphore(POOL_SIZE)


def get_pool():
    global _pool
    if _pool is None:
        _pool = pool.ThreadedConnectionPool(
            1,
            POOL_SIZE,
            host=os.getenv("DB_HOST", "postgres"),
            database=os.getenv("DB_NAME", "chargeflow"),
            user=os.getenv("DB_USER", "chargeflow"),
            password=os.environ["DB_PASSWORD"],
        )
    return _pool


@contextmanager
def db_cursor():
    """One transaction per request: commit on success, rollback on error."""
    if not _slots.acquire(timeout=POOL_WAIT_SECONDS):
        raise HTTPException(
            status_code=503,
            detail="Reservation service busy, please retry",
        )
    try:
        p = get_pool()
        conn = p.getconn()
        try:
            with conn:
                with conn.cursor() as cursor:
                    yield cursor
        finally:
            p.putconn(conn)
    finally:
        _slots.release()


def row_to_dict(row):
    return {
        "reservation_id": row[0],
        "user_id": row[1],
        "charger_id": row[2],
        "start_time": row[3].isoformat(),
        "end_time": row[4].isoformat(),
        "status": row[5],
    }


class ReservationCreate(BaseModel):
    user_id: int
    # Provide exactly one: a specific charger, or a station (the service
    # picks a free charger there, used by the Queue Service).
    charger_id: Optional[int] = None
    station_id: Optional[int] = None
    start_time: datetime
    end_time: datetime

    @model_validator(mode="after")
    def check_fields(self):
        if (self.charger_id is None) == (self.station_id is None):
            raise ValueError("Provide exactly one of charger_id or station_id")
        if self.start_time.tzinfo is None or self.end_time.tzinfo is None:
            raise ValueError("start_time and end_time must include a timezone")
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")
        if self.end_time - self.start_time > MAX_DURATION:
            raise ValueError("Reservation cannot exceed 8 hours")
        if self.start_time < datetime.now(timezone.utc) - START_GRACE:
            raise ValueError("start_time cannot be in the past")
        return self


class StatusUpdate(BaseModel):
    status: str


def get_station_chargers(station_id):
    """Ask the Station Service (owner of charger data) for a station's chargers."""
    url = f"{STATION_SERVICE_URL}/stations/{station_id}/chargers"

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
            return response.json()["chargers"]
        except (httpx.RequestError, httpx.HTTPStatusError):
            if attempt == 2:
                raise HTTPException(
                    status_code=503,
                    detail="Station Service temporarily unavailable",
                )
            time.sleep(0.2 * (attempt + 1))


def insert_reservation(user_id, charger_id, start_time, end_time):
    """Insert a confirmed reservation.

    Double booking is prevented by the database exclusion constraint
    (no_overlapping_confirmed_reservations), not by a check-then-insert,
    so it holds across concurrent requests and multiple service instances.
    Returns the row, or None if the charger is already booked for that time.
    """
    try:
        with db_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO reservations
                    (user_id, charger_id, start_time, end_time, status)
                VALUES (%s, %s, %s, %s, 'confirmed')
                RETURNING {COLUMNS}
                """,
                (user_id, charger_id, start_time, end_time),
            )
            return cursor.fetchone()
    except errors.ExclusionViolation:
        return None
    except errors.ForeignKeyViolation as e:
        constraint = e.diag.constraint_name or ""
        what = "User" if "user" in constraint else "Charger"
        raise HTTPException(status_code=404, detail=f"{what} not found")


@app.get("/")
def home():
    return {"message": "ChargeFlow Reservation Service is running"}


@app.get("/health")
def health():
    return {"status": "healthy", "service": "reservation"}


@app.post("/reservations", status_code=201)
def create_reservation(body: ReservationCreate):
    try:
        if body.charger_id is not None:
            row = insert_reservation(
                body.user_id, body.charger_id, body.start_time, body.end_time
            )
            if row is None:
                raise HTTPException(
                    status_code=409,
                    detail="Charger is already reserved for that time period",
                )
            return row_to_dict(row)

        chargers = get_station_chargers(body.station_id)
        if not chargers:
            raise HTTPException(status_code=404, detail="Station not found")

        for charger in chargers:
            if charger["status"] == "offline":
                continue
            row = insert_reservation(
                body.user_id, charger["charger_id"],
                body.start_time, body.end_time,
            )
            if row is not None:
                return row_to_dict(row)

        raise HTTPException(
            status_code=409,
            detail="No charger at this station is free for that time period",
        )
    except psycopg2.Error:
        raise HTTPException(
            status_code=503,
            detail="Reservation database temporarily unavailable",
        )


@app.get("/reservations")
def list_reservations(
    user_id: Optional[int] = None,
    charger_id: Optional[int] = None,
    status: Optional[str] = Query(None, pattern="^(confirmed|cancelled|completed)$"),
    limit: int = Query(100, ge=1, le=500),
):
    filters, params = [], []
    for column, value in (
        ("user_id", user_id), ("charger_id", charger_id), ("status", status)
    ):
        if value is not None:
            filters.append(f"{column} = %s")
            params.append(value)
    where = "WHERE " + " AND ".join(filters) if filters else ""

    try:
        with db_cursor() as cursor:
            cursor.execute(
                f"""
                SELECT {COLUMNS} FROM reservations
                {where}
                ORDER BY start_time DESC
                LIMIT %s
                """,
                (*params, limit),
            )
            rows = cursor.fetchall()
        return {"reservations": [row_to_dict(r) for r in rows]}
    except psycopg2.Error:
        raise HTTPException(
            status_code=503,
            detail="Reservation database temporarily unavailable",
        )


@app.get("/reservations/{reservation_id}")
def get_reservation(reservation_id: int):
    try:
        with db_cursor() as cursor:
            cursor.execute(
                f"SELECT {COLUMNS} FROM reservations WHERE reservation_id = %s",
                (reservation_id,),
            )
            row = cursor.fetchone()
    except psycopg2.Error:
        raise HTTPException(
            status_code=503,
            detail="Reservation database temporarily unavailable",
        )

    if row is None:
        raise HTTPException(status_code=404, detail="Reservation not found")
    return row_to_dict(row)


def transition(reservation_id, new_status):
    """Move a confirmed reservation to new_status.

    Only confirmed reservations can change state; the UPDATE is conditional
    so concurrent cancel/complete requests cannot both win.
    """
    try:
        with db_cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE reservations SET status = %s
                WHERE reservation_id = %s AND status = 'confirmed'
                RETURNING {COLUMNS}
                """,
                (new_status, reservation_id),
            )
            row = cursor.fetchone()
            if row is not None:
                return row_to_dict(row)

            cursor.execute(
                f"SELECT {COLUMNS} FROM reservations WHERE reservation_id = %s",
                (reservation_id,),
            )
            current = cursor.fetchone()
    except psycopg2.Error:
        raise HTTPException(
            status_code=503,
            detail="Reservation database temporarily unavailable",
        )

    if current is None:
        raise HTTPException(status_code=404, detail="Reservation not found")
    if current[5] == new_status:
        return row_to_dict(current)  # idempotent retry
    raise HTTPException(
        status_code=409,
        detail=f"Reservation is already {current[5]}",
    )


@app.delete("/reservations/{reservation_id}")
def cancel_reservation(reservation_id: int):
    reservation = transition(reservation_id, "cancelled")
    return {"message": "Reservation cancelled", "reservation": reservation}


@app.patch("/reservations/{reservation_id}/status")
def update_reservation_status(reservation_id: int, update: StatusUpdate):
    if update.status not in ("cancelled", "completed"):
        raise HTTPException(
            status_code=400,
            detail="Status must be 'cancelled' or 'completed'",
        )
    return transition(reservation_id, update.status)
