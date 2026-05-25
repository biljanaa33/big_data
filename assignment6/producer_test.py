from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:10000"})

producer.produce("test-topic", key="1", value="hello kafka")

producer.flush()

print("Message sent.")
