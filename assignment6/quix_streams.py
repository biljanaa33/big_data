import math
import argparse

from datetime import timedelta

from quixstreams import Application

from config import *

FIELDS = ["trip_distance", "fare_amount", "tip_amount"]

borough_seen = 0
location_seen = 0
location_matched = 0


def initializer(value):
    """Create the first accumulator for each borough/location in each window."""

    acc = {"n": 0}
    for f in FIELDS:

        acc[f"sum_{f}"] = 0.0
        acc[f"ssq_{f}"] = 0.0
        acc[f"min_{f}"] = float("inf")
        acc[f"max_{f}"] = float("-inf")

    return reducer(acc, value)


def reducer(acc, value):
    """Update the accumulator with one incoming taxi trip."""

    acc["n"] += 1
    for f in FIELDS:

        v = value.get(f) or 0.0

        acc[f"sum_{f}"] += v
        acc[f"ssq_{f}"] += v**2
        acc[f"min_{f}"] = min(acc[f"min_{f}"], v)
        acc[f"max_{f}"] = max(acc[f"max_{f}"], v)

    return acc


def finalize(window_result):

    """Calculate mean, standard deviation, min, and max for a completed window."""
    acc = window_result["value"]
    n = acc["n"]
    out = {
        "count": n,
        "window_start": str(window_result["start"]),
        "window_end": str(window_result["end"]),
    }

    for f in FIELDS:

        mean = acc[f"sum_{f}"] / n
        var = acc[f"ssq_{f}"] / n - mean**2
        out[f"{f}_mean"] = round(mean, 4)
        out[f"{f}_std"] = round(math.sqrt(max(var, 0)), 4)
        out[f"{f}_min"] = round(acc[f"min_{f}"], 4)
        out[f"{f}_max"] = round(acc[f"max_{f}"], 4)

    return out


def print_borough_progress(row):
    global borough_seen

    borough_seen += 1

    if borough_seen % 5000 == 0:
        print(f"Borough pipeline consumed {borough_seen} taxi events")


def print_location_progress(row):
    global location_seen

    location_seen += 1

    if location_seen % 5000 == 0:
        print(f"Location pipeline consumed {location_seen} taxi events before filter")


def print_location_match_progress(row):
    global location_matched

    location_matched += 1

    if location_matched % 1000 == 0:
        print(f"Location pipeline matched {location_matched} top-location taxi events")


def is_top_location(row):
    try:
        location_id = int(row["PULocationID"])
    except (KeyError, TypeError, ValueError):
        return False

    return location_id in TOP10_LOCATIONS


def stringify_location_id(row):
    return {**row, "PULocationID": str(int(row["PULocationID"]))}




def run_borough_stats():

    app = Application(
        broker_address=BOOTSTRAP,
        consumer_group="borough-stats",
        auto_offset_reset="earliest",
    )

    # Input topic with raw taxi events
    src = app.topic(TOPIC_RAW, value_deserializer="json")
    # Output topic for borough statistics
    dst = app.topic(TOPIC_BOROUGH_STATS, value_serializer="json")
    sdf = app.dataframe(src)
    sdf = sdf.update(print_borough_progress)

    # Group taxi trips by pickup borough
    sdf = sdf.group_by("PU_Borough") 
    # We use non-overlapping time window (30s) to aggregate statistics for each borough
    sdf = (
        sdf.tumbling_window(timedelta(seconds=WINDOW_SECONDS))
        .reduce(reducer=reducer, initializer=initializer)
        .final()
    )

    sdf = sdf.apply(finalize)
    # One message per borough per completed window
    sdf.to_topic(dst)

    print("Running borough statistics...")

    app.run()




def run_location_stats():

    app = Application(
        broker_address=BOOTSTRAP,
        consumer_group="location-stats",
        auto_offset_reset="earliest",
    )

    src = app.topic(TOPIC_RAW, value_deserializer="json")
    dst = app.topic(TOPIC_LOCATION_STATS, value_serializer="json")
    sdf = app.dataframe(src)
    sdf = sdf.update(print_location_progress)
    sdf = sdf.filter(is_top_location)
    sdf = sdf.update(print_location_match_progress)
    sdf = sdf.apply(stringify_location_id)
    sdf = sdf.group_by("PULocationID")

    sdf = (
        sdf.tumbling_window(timedelta(seconds=WINDOW_SECONDS))
        .reduce(reducer=reducer, initializer=initializer)
        .final()
    )

    sdf = sdf.apply(finalize)
    sdf.to_topic(dst)

    print("Running location statistics...")

    app.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run one Quix Streams pipeline.")
    parser.add_argument(
        "pipeline",
        choices=["borough", "location"],
        help="Pipeline to run. Start both in separate terminals.",
    )
    args = parser.parse_args()

    if args.pipeline == "borough":
        run_borough_stats()
    else:
        run_location_stats()
