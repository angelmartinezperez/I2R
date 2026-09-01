from kafka import KafkaProducer

# 1. Configure the producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# 2. Send message (async by default)
future = producer.send('quickstart-events', value=b'Hello Kafka!')

# 3. (Optional) Wait synchronously for the result
result = future.get(timeout=60)
print(f"Message delivered to {result.topic} [{result.partition}]")

# 4. Close the producer connection
producer.close()