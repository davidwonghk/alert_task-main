import time

import sqlalchemy as sa

from collections import defaultdict
from itertools import zip_longest
from typing import Callable, Iterable, Optional


# --------------------------------------------------
# constants definationin
# in proudction these should be read from config
DATABASE_URL = "postgresql://postgres:postgres@postgres:5432/postgres"
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
INSERT_BATCH_SIZE = 100
FETCH_BATCH_SIZE = 1000
CONSECUTIVE_EVENTS_TO_ALERT = 5

# --------------------------------------------------
# type definationin
Event = tuple[str, str]
Alerter = Callable[[Event], Optional[str]]

# --------------------------------------------------

def batch(iterable: Iterable, batch_size: int) -> Iterable:
    args = [iter(iterable)] * batch_size
    for abatch in zip_longest(*args):
        yield [a for a in abatch if a is not None]
    

def build_consecutive_alter(alert_types: list[str], alert_count: int) -> Alerter:
    """
    assuming all the event.type of people cateogory
    is refering to the same person
    """
    count = 0
    last = None
    def alerter(event: Event) -> Optional[str]:
        nonlocal count, last
        _, event_type = event

        count = count*int(event_type == last) + 1
        last = event_type

        if count == alert_count:
            count = 0
            if event_type in alert_types:
                return f"person '{event_type}' is detected in {alert_count} consecutive events"
        return None
        
    return alerter


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


def ingest_data(conn: sa.Connection, events: Iterable[Event], alerters: Iterable[Alerter]):
    stmt = "INSERT INTO events (time, type) VALUES "
    stmt += ",".join(f"('{timestamp}', '{event_type}')" for timestamp, event_type in events)
    conn.execute(sa.text(stmt))
    for event in events:
        for alerter in alerters:
            alert_msg = alerter(event)
            if alert_msg:
                print(alert_msg)


def aggregate_events(conn: sa.Connection) -> dict[str, list[Event]]:
    """
    as per requested the whole aggregation logic is in SQL
    the algorithm read the whole "events" table at first
    and process them as a pipeline of multiple temporary views:
        1. event_categories: map type to category by "types" table
        2. event_intervals: add the previous timestamp(prev_time) from the previous event of the same category
        3. event_flags: add a new_interval_flag to indicate if the current row should start a new interval
           (ie. >1 min from the previous event of the same category)
        4. event_partitions: add two sequence ids
            - order_id = sequence id for events of the same category order by timestamp
            - partition_id = sequence id for events of the same category and same new_interval_flag, order by timestamp
        5. the desired result: group by category and calculated group_id, base on
            if new_interval_flag is 1, a new group_id is assigned,
                ie. group_id = partition_id
            otherwise, since the difference of the order_id and partition_id is equals to the number of assigned groups,
                ie. group_id = order_id - partition_id

          eg. for the timestamps of the same category
            time       prev_time  new_interval_flag  order_id  partition_id  group_id
            18:30:30   none       1                  1         1             1
            18:31:00   18:30:30   0                  2         1             1
            18:31:30   18:31:30   0                  3         2             1
            18:35:00   18:35:00   1                  4         2             2
            18:35:30   18:35:30   0                  5         3             2
    """

    stmt = """
        WITH
            event_categoreis AS (
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
                FROM event_categoreis
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
        aggregate_batch = output.fetchmany(FETCH_BATCH_SIZE)
        if not aggregate_batch:
            break
        for category, start_time, end_time in aggregate_batch:
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

    alerters = [
            build_consecutive_alter(['pedestrian', 'bicycle'], CONSECUTIVE_EVENTS_TO_ALERT)
    ]
    for event_batch in batch(events, INSERT_BATCH_SIZE):
        ingest_data(conn, event_batch, alerters)

    aggregate_results = aggregate_events(conn)
    print(aggregate_results)


if __name__ == "__main__":
    main()
