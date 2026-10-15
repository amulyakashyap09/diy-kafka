import json
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "goalstream-partition-demo",
    "acks": "all",
})

TOPIC = "multiple-match-events"

events = [
    {"matchId": "match-101", "minute": 1, "type": "KICKOFF"},
    {"matchId": "match-202", "minute": 1, "type": "KICKOFF"},
    {"matchId": "match-303", "minute": 1, "type": "KICKOFF"},
    {"matchId": "match-101", "minute": 17, "type": "GOAL"},
    {"matchId": "match-202", "minute": 25, "type": "YELLOW_CARD"},
    {"matchId": "match-303", "minute": 32, "type": "GOAL"},
    {"matchId": "match-101", "minute": 45, "type": "HALF_TIME"},
    {"matchId": "match-202", "minute": 60, "type": "GOAL"},
    {"matchId": "match-303", "minute": 90, "type": "FULL_TIME"},
]

def delivery_report(err, message):
    if err:
        print(f"FAILED: {err}")
    else:
        print(
            f"DELIVERED key={message.key().decode()} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )

for event in events:
    producer.produce(
        topic=TOPIC,
        key=event["matchId"].encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )
    #  A non-blocking call. It checks existing response buffers immediately, 
    # serves any pending callbacks that are ready, and returns right away. 
    # Often called right after produce() to process old (already acknowledged) events.
    producer.poll(0)

# The producer.flush() method in Apache Kafka forces all buffered and 
# in-flight producer records to be sent immediately to the broker and blocks execution until 
# they are acknowledged or time out.
producer.flush(10)

# producer.poll() does not guarantee that all newly queued messages are flushed to the broker 
# before your application exits. Use producer.flush() before shutting down to ensure every 
# message and callback finishes.

