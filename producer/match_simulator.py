
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)
TOPIC = os.getenv("KAFKA_TOPIC", "match-events")

MATCH_ID = "match-101"
HOME_TEAM = "Manchester City"
AWAY_TEAM = "Liverpool"

# One simulated match minute lasts this many real seconds.
SECONDS_PER_MINUTE = 0.5

"""
1. bootstrap.servers tells the producer where to connect.
2. client.id helps identify the application in logs and metrics.
3. acks=all requests acknowledgment from the partition's current in-sync replicas. With your single-broker setup, there is only one replica, so this does not provide multi-broker redundancy.
"""
producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": "goalstream-match-simulator",
    "acks": "all",
})


def delivery_report(err, message):
    """Called when Kafka confirms delivery or reports a failure."""
    if err is not None:
        print(f"DELIVERY FAILED: {err}")
        return

    print(
        f"DELIVERED topic={message.topic()} "
        f"partition={message.partition()} "
        f"offset={message.offset()}"
    )


def publish_event(
    event_type,
    minute,
    team=None,
    player=None,
    home_score=0,
    away_score=0,
):
    event = {
        "eventId": str(uuid.uuid4()),
        "matchId": MATCH_ID,
        "type": event_type,
        "minute": minute,
        "team": team,
        "player": player,
        "homeScore": home_score,
        "awayScore": away_score,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    payload = json.dumps(event).encode("utf-8")

    producer.produce(
        topic=TOPIC,
        key=MATCH_ID.encode("utf-8"),
        value=payload,
        callback=delivery_report,
    )

    """
    We need because These operations have different responsibilities:
        - producer.produce() — queues the message for asynchronous delivery. It doesn't necessarily mean Kafka has received it.
        - producer.poll(0) — serves delivery callbacks and other client events without waiting for additional time.
        - producer.flush() — waits for queued messages to finish delivering, up to the specified timeout.

        Do production systems call it after every message?
            Not necessarily. There are three common patterns.
        - Pattern A: Poll after each message
        - Pattern B: Poll periodically or in batches
        - Pattern C: Continuous production with a delivery loop
    """
    producer.poll(0)

    print(f"MATCH EVENT: {json.dumps(event)}")


def main():
    home_score = 0
    away_score = 0

    timeline = [
        (1, "KICKOFF", None, None),
        (17, "GOAL", HOME_TEAM, "Erling Haaland"),
        (24, "YELLOW_CARD", AWAY_TEAM, "Alexis Mac Allister"),
        (38, "GOAL", AWAY_TEAM, "Mohamed Salah"),
        (45, "HALF_TIME", None, None),
        (55, "SUBSTITUTION", HOME_TEAM, "Phil Foden"),
        (67, "GOAL", HOME_TEAM, "Phil Foden"),
        (83, "RED_CARD", AWAY_TEAM, "Virgil van Dijk"),
        (90, "FULL_TIME", None, None),
    ]

    print(f"Starting match: {HOME_TEAM} vs {AWAY_TEAM}")

    try:
        previous_minute = 0

        for minute, event_type, team, player in timeline:
            elapsed_minutes = minute - previous_minute
            time.sleep(elapsed_minutes * SECONDS_PER_MINUTE)
            previous_minute = minute

            if event_type == "GOAL":
                if team == HOME_TEAM:
                    home_score += 1
                else:
                    away_score += 1

            publish_event(
                event_type=event_type,
                minute=minute,
                team=team,
                player=player,
                home_score=home_score,
                away_score=away_score,
            )

    except KeyboardInterrupt:
        print("\nMatch simulation interrupted.")

    finally:
        remaining = producer.flush(timeout=10)

        if remaining:
            print(f"WARNING: {remaining} message(s) remain undelivered.")
        else:
            print("All queued messages have been delivered.")

    print(f"Final score: {HOME_TEAM} {home_score}-{away_score} {AWAY_TEAM}")


if __name__ == "__main__":
    main()
