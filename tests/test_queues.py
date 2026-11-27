
"""Queue Service integration tests against the live Docker stack."""

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import psycopg2
import pytest

QUEUE_URL = os.getenv("QUEUE_URL", "http://localhost:8003")
RESERVATION_URL = os.getenv(
    "RESERVATION_URL", "http://localhost:8002"
)


def get_db_password():
    """Read DB_PASSWORD from the environment or local .env file."""
    password = os.getenv("DB_PASSWORD")
    if password:
        return password

    env_file = Path(".env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.strip().startswith("DB_PASSWORD="):
                return line.split("=", 1)[1].strip().strip("\"'")

    raise RuntimeError("DB_PASSWORD is not configured")


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_TEST_HOST", "localhost"),
        port=int(os.getenv("DB_TEST_PORT", "5433")),
        database=os.getenv("DB_NAME", "chargeflow"),
        user=os.getenv("DB_USER", "chargeflow"),
        password=get_db_password(),
    )


@pytest.fixture
def slot():
    """A unique future reservation window."""
    start = datetime.now(timezone.utc) + timedelta(
        days=3,
        minutes=uuid.uuid4().int % 100000,
    )
    return start, start + timedelta(hours=1)


@pytest.fixture
def client():
    with httpx.Client(timeout=20) as c:
        yield c


def join_test_queue(client, user_id=1):
    response = client.post(
        f"{QUEUE_URL}/queues/1/join",
        json={"user_id": user_id},
    )
    assert response.status_code == 201, response.text
    return response.json()["queue_entry"]


def assign(client, slot):
    start, end = slot
    return client.post(
        f"{QUEUE_URL}/queues/1/assign",
        json={
            "start_time": start.isoformat(),
            "end_time": end.isoformat(),
        },
    )


def get_queue_entry(queue_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, assignment_id, reservation_id,
                       assignment_start_time, assignment_end_time
                FROM queue_entries
                WHERE queue_id = %s
                """,
                (queue_id,),
            )
            return cursor.fetchone()


def simulate_interrupted_assignment(queue_id):
    """Simulate a crash after reservation creation but before queue update."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE queue_entries
                SET status = 'assigning',
                    reservation_id = NULL
                WHERE queue_id = %s
                """,
                (queue_id,),
            )


def count_reservations(assignment_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM reservations
                WHERE assignment_id = %s
                """,
                (str(assignment_id),),
            )
            return cursor.fetchone()[0]


def cleanup_reservation(client, reservation_id):
    response = client.delete(
        f"{RESERVATION_URL}/reservations/{reservation_id}"
    )
    assert response.status_code == 200, response.text


def test_queue_assignment_creates_reservation(client, slot):
    entry = join_test_queue(client)

    response = assign(client, slot)
    assert response.status_code == 200, response.text

    result = response.json()
    queue_entry = result["queue_entry"]
    reservation = result["reservation"]

    assert queue_entry["queue_id"] == entry["queue_id"]
    assert queue_entry["status"] == "assigned"
    assert queue_entry["assignment_id"]
    assert queue_entry["reservation_id"] == reservation["reservation_id"]
    assert reservation["status"] == "confirmed"

    saved = get_queue_entry(entry["queue_id"])
    assert saved[0] == "assigned"
    assert str(saved[1]) == queue_entry["assignment_id"]
    assert saved[2] == reservation["reservation_id"]

    cleanup_reservation(client, reservation["reservation_id"])


def test_interrupted_assignment_recovers_existing_reservation(client, slot):
    entry = join_test_queue(client)

    first = assign(client, slot)
    assert first.status_code == 200, first.text

    original = first.json()
    original_reservation_id = original["reservation"]["reservation_id"]
    assignment_id = original["queue_entry"]["assignment_id"]

    # Simulate the queue update being interrupted after reservation creation.
    simulate_interrupted_assignment(entry["queue_id"])

    retry = assign(client, slot)
    assert retry.status_code == 200, retry.text

    recovered = retry.json()

    assert recovered["queue_entry"]["status"] == "assigned"
    assert recovered["queue_entry"]["assignment_id"] == assignment_id
    assert recovered["reservation"]["reservation_id"] == original_reservation_id

    assert count_reservations(assignment_id) == 1

    cleanup_reservation(client, original_reservation_id)


def test_concurrent_assignment_requests_are_idempotent(client, slot):
    entry = join_test_queue(client)

    def attempt():
        with httpx.Client(timeout=20) as concurrent_client:
            return assign(concurrent_client, slot)

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda _: attempt(), range(2)))

    for response in responses:
        assert response.status_code == 200, response.text

    results = [response.json() for response in responses]

    reservation_ids = {
        result["reservation"]["reservation_id"]
        for result in results
    }
    assignment_ids = {
        result["queue_entry"]["assignment_id"]
        for result in results
    }

    assert len(reservation_ids) == 1
    assert len(assignment_ids) == 1

    reservation_id = next(iter(reservation_ids))
    assignment_id = next(iter(assignment_ids))

    assert count_reservations(assignment_id) == 1

    saved = get_queue_entry(entry["queue_id"])
    assert saved[0] == "assigned"
    assert saved[2] == reservation_id

    cleanup_reservation(client, reservation_id)
