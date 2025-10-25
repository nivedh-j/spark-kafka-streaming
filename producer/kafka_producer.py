# kafka_producer.py
from kafka import KafkaProducer
import json
import time
import random
import socket

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "stream-topic"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        linger_ms=10
    )

def main():
    producer = create_producer()
    print(f"Producer started. Sending to {TOPIC} on {BOOTSTRAP_SERVERS}")

    try:
        while True:
            data = {
                "host": socket.gethostname(),
                "user_id": random.randint(100, 999),
                "amount": round(random.uniform(10.0, 500.0), 2),
                "ts": int(time.time())
            }
            # send asynchronously
            producer.send(TOPIC, value=data)
            producer.flush()  # ensure it's sent (useful for small tests)
            print("Sent:", data)
            time.sleep(2)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
