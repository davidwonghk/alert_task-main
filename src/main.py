import time

import sqlalchemy as sa
from collections import defaultdict


DATABASE_URL = "postgresql://postgres:postgres@postgres:5432/postgres"
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


def database_connection(database_url:str, num_trial:int = 5) -> sa.Connection:
    engine = sa.create_engine(database_url)

    for attempt in range(num_trial):
        try:
            conn = engine.connect()
        except sa.exc.OperationalError as e:
            if attempt == num_trial - 1:
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
    conn.execute(
        sa.text(
            "INSERT INTO events "
            f"(time, type) VALUES ('{timestamp}', '{event_type}')"
        )
    )


def aggregate_events(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    # the whole aggregation logic is in SQL
    stmt = """
        WITH
            event_intervals AS (
                SELECT
                    id,
                    time,
                    type,
                    LAG(time) OVER (PARTITION BY type ORDER BY time) AS prev_time
                FROM events
            ),
            event_flags AS (
                SELECT
                    type,
                    time,
                    CASE WHEN prev_time IS NULL OR time - prev_time > INTERVAL '1 minute'
                        THEN 1
                        ELSE 0
                    END AS new_interval_flag
                FROM event_intervals
            ),
            event_partitions AS (
                SELECT
                    type,
                    time,
                    new_interval_flag,
                    ROW_NUMBER() OVER (PARTITION BY type ORDER BY time) AS order_id,
                    ROW_NUMBER() OVER (PARTITION BY type, new_interval_flag ORDER BY time) AS partition_id
                FROM event_flags
            )
        SELECT
            type,
            MIN(time) as start_time,
            MAX(time) as end_time
        FROM
            event_partitions
        GROUP BY
            type,
            CASE WHEN new_interval_flag = 1
                THEN partition_id
                ELSE order_id - partition_id
            END
        ORDER BY
            start_time
    """
    res = defaultdict(list)
    output = conn.execute(sa.text(stmt))
    for event_type, start_time, end_time in output.fetchall():
        start = start_time.strftime(DATETIME_FORMAT)
        end = end_time.strftime(DATETIME_FORMAT)
        res[event_type].append((start, end))
    return res


def main():
    conn = database_connection(DATABASE_URL)

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
