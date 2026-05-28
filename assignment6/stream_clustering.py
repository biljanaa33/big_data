"""
    Consume taxi trip events from Kafka, apply online k-means clustering, and write
    the assigned cluster for each trip back to a Kafka topic.
"""

import json
import math
import time

from confluent_kafka import Consumer, Producer

from config import *


FEATURE_SCALES = {
    "trip_distance": 12.0,
    "fare_amount": 40.0,
    "tip_amount": 8.0,
    "total_amount": 50.0,
    "pickup_business_count": 400.0,
    "dropoff_business_count": 400.0,
}


class OnlineKMeans:
    def __init__(self, n_clusters):
        self.n_clusters = n_clusters
        self.centers = []
        self.counts = []

    def update(self, x):
        """Assign one feature vector to the nearest cluster and update its center."""
        if len(self.centers) < self.n_clusters:
            cluster_id = len(self.centers)
            self.centers.append(list(x))
            self.counts.append(1)
            return cluster_id, 0.0

        distances = [self._distance(x, center) for center in self.centers]
        cluster_id = min(range(self.n_clusters), key=distances.__getitem__)
        self.counts[cluster_id] += 1

        # Update the cluster center 
        # Use a learning rate, the cluster center moves less with each new point assigned to it
        learning_rate = 1.0 / self.counts[cluster_id]
        center = self.centers[cluster_id]

        for i, value in enumerate(x):
            center[i] += learning_rate * (value - center[i])

        return cluster_id, distances[cluster_id]

    @staticmethod
    def _distance(x, y):
        return math.sqrt(sum((a - b) ** 2 for a, b in zip(x, y)))


def clean_number(value):
    """Convert invalid or missing numeric values to 0.0."""
    if value is None:
        return 0.0

    try:
        value = float(value)
    except (TypeError, ValueError):
        return 0.0

    if math.isnan(value) or math.isinf(value):
        return 0.0

    return value


def feature_vector(event):
    """Create a scaled numeric feature vector from one taxi trip event."""
    return [clean_number(event.get(feature)) / FEATURE_SCALES.get(feature, 1.0) for feature in CLUSTER_FEATURES]


def cluster_payload(event, cluster_id, distance):
    """Create the output message containing the cluster assignment and trip data."""
    return {
        "cluster_id": cluster_id,
        "cluster_distance": round(distance, 6),
        "pickup_datetime": event.get("tpep_pickup_datetime"),
        "dropoff_datetime": event.get("tpep_dropoff_datetime"),
        "PULocationID": event.get("PULocationID"),
        "DOLocationID": event.get("DOLocationID"),
        "PU_Borough": event.get("PU_Borough"),
        "PU_Zone": event.get("PU_Zone"),
        "DO_Borough": event.get("DO_Borough"),
        "DO_Zone": event.get("DO_Zone"),
        "features": {
            feature: clean_number(event.get(feature))
            for feature in CLUSTER_FEATURES
        },
    }


def main():
    """Run the Kafka consumer-producer loop for online stream clustering."""
    consumer_group = f"stream-clustering-{int(time.time())}"

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": consumer_group,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    model = OnlineKMeans(N_CLUSTERS)

    consumer.subscribe([TOPIC_RAW])

    print(f"Running online k-means clustering into topic '{TOPIC_CLUSTERS}'...")
    print(f"Consumer group: {consumer_group}")

    count = 0
    last_wait_message = 0

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            now = time.time()

            if now - last_wait_message >= 10:
                print("Waiting for taxi events from Kafka...")
                last_wait_message = now

            continue

        if msg.error():
            print(msg.error())
            continue

        event = json.loads(msg.value().decode("utf-8"))

        vector = feature_vector(event)
        cluster_id, distance = model.update(vector)

        payload = cluster_payload(event, cluster_id, distance)

        producer.produce(
            TOPIC_CLUSTERS,
            key=str(cluster_id),
            value=json.dumps(payload),
        )

        count += 1

        # This is only for debugging
        if count <= 5:
            print(
                f"Clustered event {count}: "
                f"cluster={cluster_id}, "
                f"zone={payload.get('PU_Zone')}, "
                f"distance={payload['features']['trip_distance']}, "
                f"fare={payload['features']['fare_amount']}"
            )

        if count % 1000 == 0:
            producer.poll(0)
            print(f"Clustered {count} events")
    


if __name__ == "__main__":
    main()
