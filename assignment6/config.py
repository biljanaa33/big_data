BOOTSTRAP = "localhost:10000,localhost:10001"

TOPIC_RAW = "yellow-taxi-events"
TOPIC_BOROUGH_STATS = "taxi-borough-stats"
TOPIC_LOCATION_STATS = "taxi-location-stats"
TOPIC_PY_BOROUGH_STATS = "taxi-borough-stats-python"
TOPIC_PY_LOCATION_STATS = "taxi-location-stats-python"
TOPIC_CLUSTERS = "taxi-clusters"

WINDOW_SECONDS = 30

TOP10_LOCATIONS = [186, 48, 132, 230, 161, 170, 79, 68, 236, 162]

STATS_FIELDS = ["trip_distance", "fare_amount", "tip_amount"]

CLUSTER_FEATURES = [
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "pickup_business_count",
    "dropoff_business_count",
]

N_CLUSTERS = 5
