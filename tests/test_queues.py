
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import httpx
import pytest


QUEUE_URL = "http://localhost:8003"
RESERVATION_URL = "http://localhost:8002"

STATION_ID = 3
TEST_USERS = [19, 20]


@pytest.fixture
def client():
    with httpx.Client(base_url=QUEUE_URL, timeout=20) as test_client:
        yield test_client


def join_queue(client, user_id):
    return client.post(
        f"/queues/{STATION_ID}/join",
        json={"user_id": user_id},
    )


def leave_queue(client, user_id):
    return client.delete(
        f"/queues/{STATION_ID}/leave",
        params={"user_id": user_id},
    )


def cleanup_waiting_entries(client):
    """Cancel only waiting entries belonging to our test users."""
    for user_id in TEST_USERS:
        leave_queue(client, user_id)


def cancel_reservations(reservation_ids):
    """Cancel reservations created during tests."""
    with httpx.Client(
        base_url=RESERVATION_URL,
        timeout=20,
    ) as reservation_client:
        for reservation_id in reservation_ids:
            reservation_client.delete(
                f"/reservations/{reservation_id}"
            )


def test_join_queue(client):
    try:
        response = join_queue(client, 19)

        assert response.status_code == 201
        assert response.json()["queue_entry"]["user_id"] == 19
        assert response.json()["queue_entry"]["station_id"] == STATION_ID
        assert response.json()["queue_entry"]["status"] == "waiting"

    finally:
        cleanup_waiting_entries(client)


def test_duplicate_join_rejected(client):
    try:
        first = join_queue(client, 19)
        second = join_queue(client, 19)

        assert first.status_code == 201
        assert second.status_code == 409

    finally:
        cleanup_waiting_entries(client)


def test_leave_queue(client):
    try:
        joined = join_queue(client, 19)
        assert joined.status_code == 201

        response = leave_queue(client, 19)

        assert response.status_code == 200
        assert response.json()["queue_entry"]["status"] == "cancelled"

        # Leaving again should fail because no waiting entry remains.
        second_leave = leave_queue(client, 19)
        assert second_leave.status_code == 404

    finally:
        cleanup_waiting_entries(client)


def test_queue_order(client):
    try:
        first = join_queue(client, 19)
        second = join_queue(client, 20)

        assert first.status_code == 201
        assert second.status_code == 201

        response = client.get(f"/queues/{STATION_ID}")

        assert response.status_code == 200

        queue = response.json()["queue"]

        # Check the relative order of our two test users.
        test_user_positions = {
            entry["user_id"]: entry["position"]
            for entry in queue
            if entry["user_id"] in TEST_USERS
        }

        assert set(test_user_positions) == set(TEST_USERS)
        assert test_user_positions[19] < test_user_positions[20]

    finally:
        cleanup_waiting_entries(client)


def test_invalid_user_and_station(client):
    invalid_user = client.post(
        f"/queues/{STATION_ID}/join",
        json={"user_id": 999999},
    )

    invalid_station = client.post(
        "/queues/999999/join",
        json={"user_id": 19},
    )

    assert invalid_user.status_code == 404
    assert invalid_station.status_code == 404


def test_assign_next_driver(client):
    reservation_id = None

    try:
        joined = join_queue(client, 20)
        assert joined.status_code == 201

        start = datetime.now(timezone.utc) + timedelta(days=1)
        end = start + timedelta(hours=1)

        response = client.post(
            f"/queues/{STATION_ID}/assign",
            json={
                "start_time": start.isoformat(),
                "end_time": end.isoformat(),
            },
        )

        assert response.status_code == 200

        result = response.json()

        assert result["queue_entry"]["user_id"] == 20
        assert result["queue_entry"]["status"] == "assigned"

        reservation_id = result["reservation"]["reservation_id"]
        assert reservation_id is not None

    finally:
        cleanup_waiting_entries(client)

        if reservation_id is not None:
            cancel_reservations([reservation_id])


def test_concurrent_queue_assignments(client):
    """Verify simultaneous assignments process different waiting drivers."""
    reservation_ids = []
    barrier = Barrier(2)

    try:
        # Add two drivers to the same station.
        first = join_queue(client, 19)
        second = join_queue(client, 20)

        assert first.status_code == 201
        assert second.status_code == 201

        start = datetime.now(timezone.utc) + timedelta(days=3)
        end = start + timedelta(hours=1)

        payload = {
            "start_time": start.isoformat(),
            "end_time": end.isoformat(),
        }

        def make_assignment_request():
            # Release both requests at approximately the same time.
            barrier.wait()

            with httpx.Client(
                base_url=QUEUE_URL,
                timeout=30,
            ) as concurrent_client:
                return concurrent_client.post(
                    f"/queues/{STATION_ID}/assign",
                    json=payload,
                )

        # Submit both requests simultaneously.
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(make_assignment_request)
                for _ in range(2)
            ]

            responses = [
                future.result()
                for future in futures
            ]

        # Collect successful reservation IDs for cleanup.
        results = []

        for response in responses:
            if response.status_code == 200:
                result = response.json()
                results.append(result)

                reservation_ids.append(
                    result["reservation"]["reservation_id"]
                )

        # Both requests should succeed.
        assert len(results) == 2
        assert all(response.status_code == 200 for response in responses)

        assigned_users = {
            result["queue_entry"]["user_id"]
            for result in results
        }

        # Both different drivers must be assigned.
        assert assigned_users == {19, 20}

        # Both reservations must have different IDs.
        assert len(set(reservation_ids)) == 2

        # Both queue entries must have assigned status.
        assert all(
            result["queue_entry"]["status"] == "assigned"
            for result in results
        )

    finally:
        cleanup_waiting_entries(client)

        if reservation_ids:
            cancel_reservations(reservation_ids)
