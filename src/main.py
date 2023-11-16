import time

import sqlalchemy as sa


def database_connection() -> sa.Connection:
    engine = sa.create_engine("postgresql://postgres:postgres@postgres:5432/postgres")

    for attempt in range(5):
        try:
            conn = engine.connect()
        except sa.exc.OperationalError as e:
            if attempt == 4:
                raise e
            time.sleep(1)

    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS events "
            "(id SERIAL PRIMARY KEY, time TIMESTAMP WITH TIME ZONE, type VARCHAR)"
        )
    )

    return conn


def ingest_data(conn: sa.Connection, timestamp: str, event_type: str):
    ...


def aggregate_events(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    return {
        "people": [
            ("2023-08-10T10:00:00", "2023-08-10T10:02:00"),
            ("2023-08-10T10:04:00", "2023-08-10T10:05:00"),
        ],
        "vehicles": [
            ("2023-08-10T10:00:00", "2023-08-10T10:02:00"),
            ("2023-08-10T10:05:00", "2023-08-10T10:07:00"),
        ],
    }


def main():
    conn = database_connection()

    # Simulate real-time events every 30 seconds
    events = [
        ("2023-08-10T18:30:30", "pedestrian"),
        ("2023-08-10T18:31:00", "pedestrian"),
        ("2023-08-10T18:31:00", "car"),
        ("2023-08-10T18:31:30", "pedestrian"),
        ("2023-08-10T18:35:00", "pedestrian"),
        ("2023-08-10T18:35:30", "pedestrian"),
        ("2023-08-10T18:36:00", "pedestrian"),
        ("2023-08-10T18:37:00", "pedestrian"),
        ("2023-08-10T18:37:30", "pedestrian"),
    ]

    for timestamp, event_type in events:
        ingest_data(conn, timestamp, event_type)

    aggregate_results = aggregate_events(conn)
    print(aggregate_results)


if __name__ == "__main__":
    main()
