from confluent_kafka import Consumer

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:10000",
        "group.id": "test-group",
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe(["test-topic"])

print("Waiting for messages...")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    print(msg.value().decode())
