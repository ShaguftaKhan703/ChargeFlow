# ChargeFlow ⚡
### Distributed EV Charging Queue and Reservation System

ChargeFlow is a cloud computing project developed for **CS 218 at San José State University**.

The system allows EV drivers to find charging stations, check charger availability, reserve chargers, and join waiting queues.

The project focuses on **microservices, scalability, fault tolerance, and data consistency**.

## Technologies Used

- **Backend:** Python, FastAPI
- **Database:** PostgreSQL
- **Containerization:** Docker, Docker Compose
- **Communication:** REST APIs (HTTP)
- **Testing:** Swagger UI, Locust (planned)
- **Version Control:** GitHub

## Project Structure

```text
ChargeFlow/
├── station-service/
├── reservation-service/
├── queue-service/
├── database/
│   ├── init.sql
│   └── seed.sql
├── tests/
├── docker-compose.yml
├── .env.example
└── README.md
```

Each service has its own `main.py`, `requirements.txt`, and `Dockerfile`.

## How to Run the Project

**1. Install prerequisites**

Install Docker Desktop and Git. Make sure Docker Desktop is running.

**2. Clone the repository**

```bash
git clone https://github.com/ShaguftaKhan703/ChargeFlow.git
cd ChargeFlow
```

**3. Configure the database password**

Copy `.env.example` to `.env`:

```powershell
Copy-Item .env.example .env
```

Edit `.env` and set:

```dotenv
DB_PASSWORD=your_local_password
```

**4. Start the application**

```bash
docker compose up -d --build
```

**5. Verify the containers**

```bash
docker compose ps
```

## API Endpoints

| Service | URL |
|---|---|
| Station Service | http://localhost:8001/docs |
| Reservation Service | http://localhost:8002/docs |
| Queue Service | http://localhost:8003/docs |

## Current Progress

- Docker Compose and PostgreSQL setup completed.
- Station Service connected to PostgreSQL.
- Station listing and charger status updates working.
- Queue Service communicates with Station Service.
- Reservation and queue management logic are under development.

## Team Responsibilities

| Member | Responsibility |
|---|---|
| Member 1 | Reservation Service |
| Member 2 | Station Service |
| Member 3 | Queue Service and infrastructure |

All members contribute to testing, integration, and documentation.

## Useful Commands

```bash
docker compose up -d --build
docker compose ps
docker compose logs
docker compose down
```

`docker compose down` stops the application without deleting the database volume.

**Note:** On a fresh database, `init.sql` and `seed.sql` must be executed. They run automatically only if configured as PostgreSQL initialization mounts in `docker-compose.yml`.

## Project Goal

Build a distributed EV charging system that supports reliable reservations, prevents double booking, survives service failures, and scales under increased load.
