

import os
import time
import httpx
from fastapi import FastAPI, HTTPException

app = FastAPI(title="ChargeFlow Queue Service")

STATION_SERVICE_URL = os.getenv(
    "STATION_SERVICE_URL",
    "http://station-service:8000"
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
                    response=response
                )

            response.raise_for_status()
            return response.json()

        except (httpx.RequestError, httpx.HTTPStatusError):
            if attempt == 2:
                raise HTTPException(
                    status_code=503,
                    detail="Station Service temporarily unavailable"
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
    return {"queues": [], "message": "Queue API is ready"}


@app.get("/queues/stations")
def get_station_information():
    return {
        "source": "Station Service",
        "data": call_station_service("/stations")
    }


@app.get("/queues/stations/{station_id}/chargers")
def get_station_chargers(station_id: int):
    return {
        "source": "Station Service",
        "data": call_station_service(
            f"/stations/{station_id}/chargers"
        )
    }

