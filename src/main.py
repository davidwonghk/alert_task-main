import time

import sqlalchemy as sa
from collections import defaultdict


DATABASE_URL = "postgresql://postgres:postgres@postgres:5432/postgres"
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
FETCH_BATCH_SIZE = 1000
CONSECUTIVE_EVENTS = 5


def fire_alert(person: str, count: int) -> None:
    print(f"person '{person}' is detected in {count} consecutive events")


class PersonAlerter:
    def __init__(self, conn: sa.Connection, alert_count: int):
        self._last = None
        self._count = 0
        self._alert_count = alert_count

    def detect(self, event_type: str):
        if event_type == self._last:
            self._count += 1
        else:
            self._count = 1
        self._last = event_type

        if self._count == self._alert_count:
            fire_alert(event_type, self._count)
            self._count = 0


def database_connection(database_url:str, num_trial:int = 5) -> sa.Connection:
    engine = sa.create_engine(database_url)

    for attempt in range(num_trial):
        try:
            conn = engine.connect()
        except sa.exc.OperationalError as e:
            if attempt == num_trial - 1:
                raise e
            time.sleep(1)

    # events table
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS events "
            "(id SERIAL PRIMARY KEY, time TIMESTAMP WITH TIME ZONE, type VARCHAR)"
        )
    )
    
    # types table
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS types "
            "(id SERIAL PRIMARY KEY, type VARCHAR, category VARCHAR);"
        )
    )
    conn.execute(
        sa.text(
            """
            TRUNCATE TABLE types;
            INSERT INTO types (type, category) VALUES
                ('pedestrian', 'people'),
                ('bicycle', 'people'),
                ('car', 'vehicles'),
                ('truck', 'vehicles'),
                ('van', 'vehicles')
            """
        )
    )

    return conn


def ingest_data(conn: sa.Connection, timestamp: str, event_type: str, alerter: PersonAlerter):
    conn.execute(
        sa.text(
            "INSERT INTO events "
            f"(time, type) VALUES ('{timestamp}', '{event_type}')"
        )
    )
    alerter.detect(event_type)


def aggregate_events(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    # the whole aggregation logic is in SQL
    stmt = """
        WITH
            event_catetoreis AS (
                SELECT
                    time,
                    category
                FROM 
                    events,
                    types
                WHERE
                    events.type = types.type
            ),
            event_intervals AS (
                SELECT
                    time,
                    category,
                    LAG(time) OVER (PARTITION BY category ORDER BY time) AS prev_time
                FROM event_catetoreis
            ),
            event_flags AS (
                SELECT
                    category,
                    time,
                    CASE WHEN prev_time IS NULL OR time - prev_time > INTERVAL '1 minute'
                        THEN 1
                        ELSE 0
                    END AS new_interval_flag
                FROM event_intervals
            ),
            event_partitions AS (
                SELECT
                    category,
                    time,
                    new_interval_flag,
                    ROW_NUMBER() OVER (PARTITION BY category ORDER BY time) AS order_id,
                    ROW_NUMBER() OVER (PARTITION BY category, new_interval_flag ORDER BY time) AS partition_id
                FROM event_flags
            )
        SELECT
            category,
            MIN(time) as start_time,
            MAX(time) as end_time
        FROM
            event_partitions
        GROUP BY
            category,
            CASE WHEN new_interval_flag = 1
                THEN partition_id
                ELSE order_id - partition_id
            END
        ORDER BY
            start_time
    """
    res = defaultdict(list)
    output = conn.execute(sa.text(stmt))
    while True:
        batch = output.fetchmany(FETCH_BATCH_SIZE)
        if not batch:
            break
        for category, start_time, end_time in batch:
            start = start_time.strftime(DATETIME_FORMAT)
            end = end_time.strftime(DATETIME_FORMAT)
            res[category].append((start, end))
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

    alerter = PersonAlerter(conn, CONSECUTIVE_EVENTS)
    for timestamp, event_type in events:
        ingest_data(conn, timestamp, event_type, alerter)

    aggregate_results = aggregate_events(conn)
    print(aggregate_results)


if __name__ == "__main__":
    main()
