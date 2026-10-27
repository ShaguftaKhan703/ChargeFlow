
from fastapi import FastAPI

app = FastAPI(title="ChargeFlow Reservation Service")

@app.get("/")
def home():
    return {"message": "ChargeFlow Reservation Service is running"}

@app.get("/health")
def health():
    return {"status": "healthy", "service": "reservation"}

@app.get("/reservations")
def get_reservations():
    return {"reservations": [], "message": "Reservation API is ready"}
