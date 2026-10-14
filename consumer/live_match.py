
import json
import os
from confluent_kafka import Consumer, KafkaError


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
TOPIC = os.getenv("KAFKA_TOPIC", "match-events")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "goalstream-live-score",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe([TOPIC])

print(f"Listening for soccer events on {TOPIC}...")

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue

            print(f"Kafka error: {message.error()}")
            continue

        event = json.loads(message.value().decode("utf-8"))

        home = event.get("homeScore", 0)
        away = event.get("awayScore", 0)
        event_type = event.get("type", "UNKNOWN")
        minute = event.get("minute", "?")
        team = event.get("team")
        player = event.get("player")
        timestamp = event.get("timestamp")

        if timestamp is None:
            timestamp = "timestamp unavailable"

        print(
            f"[{minute:>2}''] {event_type:<14} "
            f"{home}-{away} | {timestamp}"
            f"{team or 'Match'}"
            f"{' — ' + player if player else ''}"
        )

        # Commit only after this event has been processed.
        consumer.commit(message=message, asynchronous=False)

except KeyboardInterrupt:
    print("\nStopping live score consumer.")

finally:
    consumer.close()
