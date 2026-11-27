
import os
import time
import uuid
from datetime import datetime

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
RESERVATION_SERVICE_URL = os.getenv(
    "RESERVATION_SERVICE_URL",
    "http://reservation-service:8000"
)


class JoinQueueRequest(BaseModel):
    user_id: int


class AssignQueueRequest(BaseModel):
    start_time: datetime
    end_time: datetime


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


def call_reservation_service(payload):
    try:
        response = httpx.post(
            f"{RESERVATION_SERVICE_URL}/reservations",
            json=payload,
            timeout=5.0,
        )
        response.raise_for_status()
        return response.json()

    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code

        if status_code in (404, 409):
            try:
                detail = exc.response.json().get(
                    "detail",
                    "Reservation could not be created",
                )
            except ValueError:
                detail = "Reservation could not be created"

            raise HTTPException(
                status_code=status_code,
                detail=detail,
            )

        raise HTTPException(
            status_code=503,
            detail="Reservation Service temporarily unavailable",
        )

    except httpx.RequestError:
        raise HTTPException(
            status_code=503,
            detail="Reservation Service temporarily unavailable",
        )


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

    except HTTPException:
        raise
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
    except HTTPException:
        raise
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

    except HTTPException:
        raise
    except psycopg2.Error:
        raise HTTPException(
            status_code=503,
            detail="Queue database temporarily unavailable",
        )


@app.post("/queues/{station_id}/assign")
def assign_next_driver(
    station_id: int,
    request: AssignQueueRequest,
):
    """
    Assign the earliest queue entry to a charger.

    Persist the assignment ID and reservation time window before calling
    Reservation Service. Temporary failures can then be safely retried.
    """

    if (
        request.start_time.tzinfo is None
        or request.end_time.tzinfo is None
    ):
        raise HTTPException(
            status_code=400,
            detail="Reservation times must include a timezone",
        )

    if request.end_time <= request.start_time:
        raise HTTPException(
            status_code=400,
            detail="end_time must be after start_time",
        )

    try:
        # Step 1: Persist or recover an assignment.
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:

                # Serialize assignment selection for this station.
                cursor.execute(
                    """
                    SELECT station_id
                    FROM stations
                    WHERE station_id = %s
                    FOR UPDATE
                    """,
                    (station_id,),
                )

                if cursor.fetchone() is None:
                    raise HTTPException(
                        status_code=404,
                        detail="Station not found",
                    )

                # Recover an interrupted assignment first.
                cursor.execute(
                    """
                    SELECT
                        queue_id,
                        user_id,
                        station_id,
                        assignment_id,
                        assignment_start_time,
                        assignment_end_time
                    FROM queue_entries
                    WHERE station_id = %s
                      AND status = 'assigning'
                    ORDER BY joined_at, queue_id
                    LIMIT 1
                    FOR UPDATE
                    """,
                    (station_id,),
                )

                entry = cursor.fetchone()

                if entry is None:
                    # Otherwise select the earliest waiting driver.
                    cursor.execute(
                        """
                        SELECT queue_id, user_id, station_id
                        FROM queue_entries
                        WHERE station_id = %s
                          AND status = 'waiting'
                        ORDER BY joined_at, queue_id
                        LIMIT 1
                        FOR UPDATE
                        """,
                        (station_id,),
                    )

                    waiting_entry = cursor.fetchone()

                    if waiting_entry is None:
                        raise HTTPException(
                            status_code=404,
                            detail="No drivers are waiting at this station",
                        )

                    assignment_id = uuid.uuid4()

                    cursor.execute(
                        """
                        UPDATE queue_entries
                        SET
                            status = 'assigning',
                            assignment_id = %s,
                            assignment_start_time = %s,
                            assignment_end_time = %s
                        WHERE queue_id = %s
                          AND status = 'waiting'
                        RETURNING
                            queue_id,
                            user_id,
                            station_id,
                            assignment_id,
                            assignment_start_time,
                            assignment_end_time
                        """,
                        (
                            str(assignment_id),
                            request.start_time,
                            request.end_time,
                            waiting_entry["queue_id"],
                        ),
                    )

                    entry = cursor.fetchone()

                    if entry is None:
                        raise HTTPException(
                            status_code=409,
                            detail="Queue entry was already being assigned",
                        )

        # Step 2: Call Reservation Service outside the DB transaction.
        # Reuse the saved ID and original time window on retries.
        reservation_payload = {
            "user_id": entry["user_id"],
            "station_id": entry["station_id"],
            "assignment_id": str(entry["assignment_id"]),
            "start_time": entry["assignment_start_time"].isoformat(),
            "end_time": entry["assignment_end_time"].isoformat(),
        }

        try:
            reservation = call_reservation_service(reservation_payload)

        except HTTPException as exc:
            # If no charger is available, release this assignment so it
            # can be attempted again later with a new assignment ID.
            if (
                exc.status_code == 409
                and "No charger at this station is free" in str(exc.detail)
            ):
                with get_db_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            """
                            UPDATE queue_entries
                            SET
                                status = 'waiting',
                                assignment_id = NULL,
                                assignment_start_time = NULL,
                                assignment_end_time = NULL,
                                reservation_id = NULL
                            WHERE queue_id = %s
                              AND status = 'assigning'
                              AND assignment_id = %s
                            """,
                            (
                                entry["queue_id"],
                                entry["assignment_id"],
                            ),
                        )

            # Temporary failures and other conflicts preserve the
            # assignment ID so that the same request can be recovered.
            raise

        # Step 3: Save the successful reservation.
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    UPDATE queue_entries
                    SET
                        status = 'assigned',
                        reservation_id = %s
                    WHERE queue_id = %s
                      AND status = 'assigning'
                      AND assignment_id = %s
                    RETURNING
                        queue_id,
                        user_id,
                        station_id,
                        status,
                        assignment_id,
                        reservation_id
                    """,
                    (
                        reservation["reservation_id"],
                        entry["queue_id"],
                        entry["assignment_id"],
                    ),
                )

                updated_entry = cursor.fetchone()

                if updated_entry is None:
                    # Another concurrent request may already have
                    # completed this exact assignment.
                    cursor.execute(
                        """
                        SELECT
                            queue_id,
                            user_id,
                            station_id,
                            status,
                            assignment_id,
                            reservation_id
                        FROM queue_entries
                        WHERE queue_id = %s
                          AND status = 'assigned'
                          AND assignment_id = %s
                          AND reservation_id = %s
                        """,
                        (
                            entry["queue_id"],
                            entry["assignment_id"],
                            reservation["reservation_id"],
                        ),
                    )

                    updated_entry = cursor.fetchone()

                if updated_entry is None:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "Reservation was created, but the queue entry "
                            "could not be updated. Retry the assignment."
                        ),
                    )

        return {
            "message": "Reservation created for the next waiting driver",
            "queue_entry": dict(updated_entry),
            "reservation": reservation,
        }

    except HTTPException:
        raise
    except psycopg2.Error:
        raise HTTPException(
            status_code=503,
            detail="Queue database temporarily unavailable",
        )
