import math
import threading

from datetime import timedelta

from quixstreams import Application

from config import *

FIELDS = ["trip_distance", "fare_amount", "tip_amount"]

# --------------------------------------------------
# REDUCTION LOGIC
# --------------------------------------------------


def initializer(value):

    acc = {"n": 0}

    for f in FIELDS:

        acc[f"sum_{f}"] = 0.0
        acc[f"ssq_{f}"] = 0.0

        acc[f"min_{f}"] = float("inf")
        acc[f"max_{f}"] = float("-inf")

    return reducer(acc, value)


def reducer(acc, value):

    acc["n"] += 1

    for f in FIELDS:

        v = value.get(f) or 0.0

        acc[f"sum_{f}"] += v
        acc[f"ssq_{f}"] += v**2

        acc[f"min_{f}"] = min(acc[f"min_{f}"], v)

        acc[f"max_{f}"] = max(acc[f"max_{f}"], v)

    return acc


def finalize(window_result):

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


# --------------------------------------------------
# BOROUGH STATS
# --------------------------------------------------


def run_borough_stats():

    app = Application(
        broker_address=BOOTSTRAP,
        consumer_group="borough-stats",
        auto_offset_reset="earliest",
    )

    src = app.topic(TOPIC_RAW, value_deserializer="json")

    dst = app.topic(TOPIC_BOROUGH_STATS, value_serializer="json")

    sdf = app.dataframe(src)

    sdf = sdf.group_by("PU_Borough")

    sdf = (
        sdf.tumbling_window(timedelta(seconds=WINDOW_SECONDS))
        .reduce(reducer=reducer, initializer=initializer)
        .final()
    )

    sdf = sdf.apply(finalize)

    sdf.to_topic(dst)

    print("Running borough statistics...")

    app.run(sdf)


# --------------------------------------------------
# TOP LOCATION STATS
# --------------------------------------------------


def run_location_stats():

    app = Application(
        broker_address=BOOTSTRAP,
        consumer_group="location-stats",
        auto_offset_reset="earliest",
    )

    src = app.topic(TOPIC_RAW, value_deserializer="json")

    dst = app.topic(TOPIC_LOCATION_STATS, value_serializer="json")

    sdf = app.dataframe(src)

    sdf = sdf.filter(lambda r: r["PULocationID"] in TOP10_LOCATIONS)

    sdf = sdf.group_by("PULocationID")

    sdf = (
        sdf.tumbling_window(timedelta(seconds=WINDOW_SECONDS))
        .reduce(reducer=reducer, initializer=initializer)
        .final()
    )

    sdf = sdf.apply(finalize)

    sdf.to_topic(dst)

    print("Running location statistics...")

    app.run(sdf)


# --------------------------------------------------
# START BOTH PIPELINES
# --------------------------------------------------

if __name__ == "__main__":

    t1 = threading.Thread(target=run_borough_stats, daemon=True)

    t2 = threading.Thread(target=run_location_stats, daemon=True)

    t1.start()
    t2.start()

    t1.join()
    t2.join()
