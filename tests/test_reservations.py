"""Reservation Service tests. Run against the live stack:

    docker compose up -d --build
    pip install pytest httpx
    pytest tests/test_reservations.py -v

Override the target with RESERVATION_URL (default http://localhost:8002).
"""
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx
import pytest

BASE = os.getenv("RESERVATION_URL", "http://localhost:8002")


@pytest.fixture
def client():
    with httpx.Client(base_url=BASE, timeout=10) as c:
        yield c


@pytest.fixture
def slot():
    """A unique future time window so test runs don't collide."""
    offset = uuid.uuid4().int % (24 * 365)
    start = datetime.now(timezone.utc) + timedelta(days=2, hours=offset)
    return start, start + timedelta(hours=1)


def body(slot, user_id=1, charger_id=1, **extra):
    start, end = slot
    return {"user_id": user_id, "charger_id": charger_id,
            "start_time": start.isoformat(), "end_time": end.isoformat(), **extra}


def test_create_get_cancel(client, slot):
    r = client.post("/reservations", json=body(slot))
    assert r.status_code == 201
    rid = r.json()["reservation_id"]
    assert client.get(f"/reservations/{rid}").json()["status"] == "confirmed"
    assert client.delete(f"/reservations/{rid}").status_code == 200
    assert client.delete(f"/reservations/{rid}").status_code == 200  # idempotent
    assert client.get(f"/reservations/{rid}").json()["status"] == "cancelled"


def test_overlap_rejected_then_allowed_after_cancel(client, slot):
    first = client.post("/reservations", json=body(slot, user_id=1)).json()
    assert client.post("/reservations", json=body(slot, user_id=2)).status_code == 409
    client.delete(f"/reservations/{first['reservation_id']}")
    again = client.post("/reservations", json=body(slot, user_id=2))
    assert again.status_code == 201
    client.delete(f"/reservations/{again.json()['reservation_id']}")


def test_back_to_back_is_allowed(client, slot):
    start, end = slot
    a = client.post("/reservations", json=body(slot))
    b = client.post("/reservations", json=body((end, end + timedelta(hours=1))))
    assert a.status_code == b.status_code == 201
    for r in (a, b):
        client.delete(f"/reservations/{r.json()['reservation_id']}")


def test_validation(client, slot):
    start, end = slot
    assert client.post("/reservations", json=body((end, start))).status_code == 422
    past = datetime.now(timezone.utc) - timedelta(days=1)
    assert client.post("/reservations", json=body((past, past + timedelta(hours=1)))).status_code == 422
    assert client.post("/reservations", json=body(slot, charger_id=999999)).status_code == 404
    assert client.post("/reservations", json=body(slot, user_id=999999)).status_code == 404
    assert client.get("/reservations/999999").status_code == 404


def test_concurrent_requests_never_double_book(client, slot):
    """50 drivers race for the same charger and time: exactly one wins."""
    def attempt(i):
        with httpx.Client(base_url=BASE, timeout=20) as c:
            return c.post("/reservations", json=body(slot, user_id=(i % 20) + 1))

    with ThreadPoolExecutor(max_workers=50) as pool:
        results = list(pool.map(attempt, range(50)))

    codes = [r.status_code for r in results]
    assert codes.count(201) == 1
    assert codes.count(409) == 49
    winner = next(r for r in results if r.status_code == 201).json()
    client.delete(f"/reservations/{winner['reservation_id']}")


def test_station_mode_fills_chargers_then_conflicts(client, slot):
    """Station 1 has 2 chargers: third request for the same window gets 409."""
    start, end = slot
    req = {"user_id": 1, "station_id": 1,
           "start_time": start.isoformat(), "end_time": end.isoformat()}
    made = [client.post("/reservations", json=req) for _ in range(3)]
    assert [r.status_code for r in made] == [201, 201, 409]
    assert len({r.json()["charger_id"] for r in made[:2]}) == 2
    for r in made[:2]:
        client.delete(f"/reservations/{r.json()['reservation_id']}")
