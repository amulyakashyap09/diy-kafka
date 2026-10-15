# Kafka

## Run Locally

#### Start Kafka

```
docker compose up -d
docker compose logs -f kafka
```

#### Wait for the broker to start. In another terminal, check that it responds:

```
docker exec goalstream-kafka \
  /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

### Create your first topic

- A topic is a named stream of records. Think of it as a category of events, not a database table.
- We'll create a topic called match-events to carry every event from every soccer match.

```
docker exec goalstream-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic match-events \
  --partitions 1 \
  --replication-factor 1
```

#### Verify it:
```
docker exec goalstream-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic match-events
```

#### List Topics:
```
docker exec goalstream-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### 5. Publish your first goal ⚽

- A producer writes records to Kafka. A record consists of a value and can also include a key, timestamp, and headers.

**Run:**
```
docker exec -it goalstream-kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic match-events
```

#### Publish Event 1

**Paste this JSON as one line:**

`{"eventId":"evt-001","matchId":"match-101","type":"GOAL","team":"Manchester City","player":"Erling Haaland","minute":17,"homeScore":1,"awayScore":0}`

#### Publish Event 2
**Paste this JSON as one line:**

`{"eventId":"evt-002","matchId":"match-101","type":"YELLOW_CARD","team":"Liverpool","player":"Alexis Mac Allister","minute":24}`

> `Press Ctrl+C to exit the producer.`

### Read the events with a consumer

- A consumer reads records from a topic.

```
docker exec -it goalstream-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic match-events \
  --from-beginning
```

### Delete the topic

```
docker exec goalstream-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic match-events
```

## Multi partition Tutorial

- Topic : multiple-match-events
- replication-factor: 1
- Parititon : 3

#### Create the topic

```
docker exec goalstream-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic multiple-match-events \
  --partitions 3 \
  --replication-factor 1
```

#### Verify the topic

```
docker exec goalstream-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic multiple-match-events
```

#### Run

```
cd ~/goalstream
source .venv/bin/activate
python producer/partition_demo.py
```

#### Inspect the records
```
docker exec goalstream-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic multiple-match-events \
  --from-beginning \
  --formatter-property print.key=true \
  --formatter-property print.partition=true \
  --formatter-property print.offset=true
```

### Run 3 consumers

**Step A — Start Consumer 1**
```
docker exec -it goalstream-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic multiple-match-events \
  --group goalstream-cg-lab \
  --property print.key=true
```

**Step B — Start Consumer 2**
```
docker exec -it goalstream-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic multiple-match-events \
  --group goalstream-cg-lab \
  --property print.key=true
```

**Step C — Start Consumer 3**
```
docker exec -it goalstream-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic multiple-match-events \
  --group goalstream-cg-lab \
  --property print.key=true
```

> Note: all consumers in group `goalstream-cg-lab` use same id

### Step C — Publish fresh events

```
cd ~/goalstream
source .venv/bin/activate
python producer/partition_demo.py
```

> Watch all 3 consumers , how and what they recieve as messages

### What is rebalancing?

A rebalance occurs when Kafka redistributes partition ownership within a consumer group. Rebalancing can happen when:
- A consumer starts or shuts down.
- A consumer fails or stops sending heartbeats.
- The topic's partition count changes.
- The group membership changes.

## Delivery guarantees, duplicates, and idempotency

### 1. The real-world problem: a goal counted twice

```
1. Kafka delivers goal event =>
2. Score changes from 0 → 1 =>
3. Consumer crashes before offset commit =>
4. Kafka redelivers the event
5. Score becomes 2 — incorrect!
```

### 2. Three delivery guarantees

=> **At-most-once** - A message is processed zero or one time. Duplicates are avoided, but an event can be lost from the application's processing if you commit the offset before successfully handling it.

=> **At-least-once** - Retry until processing succeeds, and commit the offset after successful processing. Events won't be silently skipped under the intended retry/recovery setup, but processing may happen more than once.

=> **Exactly-once** - The same logical result is reflected once, even when failures and retries occur. Achieving this across Kafka and an external database requires more than just enabling a Kafka setting.

> **A crucial distinction: Kafka producer idempotence helps prevent duplicate writes to Kafka caused by producer retries. It does not automatically make your consumer's database updates idempotent.**

### 3. Idempotent consumers: the production solution

- An operation is idempotent if repeating it produces the same final result as doing it once.
- For GoalStream, processing goal-101 once should increment the score. Processing goal-101 five times should still increment it only once.
- **The usual solution is to give every event a stable, unique eventId and record which events have already been applied.**
    - **Store processed event IDs**
        - For a relational database, you could create a table like this:
        ```
        CREATE TABLE processed_events (
            event_id   VARCHAR(100) PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        ```
        - **The `primary key guarantees` that the same event ID cannot be inserted twice.**
    - **Deduplicate and update the score in one transaction**
        - The deduplication record and score update must be committed atomically in the same database transaction.
        ```
        BEGIN;

        INSERT INTO processed_events (event_id)
        VALUES ('goal-101')
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id;
        ```
        - If _goal-101_ was inserted, this is the first successful processing attempt. Increment the score in the same transaction.
        - If no row was returned, the event was already processed. Skip the score update.
        ```
        -- Run only when the INSERT returned event_id.
        UPDATE match_scores
        SET man_city_score = man_city_score + 1
        WHERE match_id = 'match-101';

        COMMIT;
        ```
### 4. Where should the Kafka offset commit happen?

1. Read event - Consume the goal record from Kafka.
2. Process transactionally - Deduplicate by eventId and update the score in one database transaction.
3. Commit Kafka offset - Commit only after the database transaction succeeds.

| Failure point | What happens on retry? |
|---------------| ---------------|
| Before the database transaction succeeds | The event is processed again.
| After the database commits, before the Kafka offset commits | The event may be redelivered, but the eventId check prevents a second score increment. |
| After both commits succeed | Kafka resumes from the committed position. |


### 5. Producer idempotence versus consumer idempotence

| Mechanism | Protects against | Does not guarantee |
|---------------| ---------------| -----------------|
|Producer idempotence|Duplicate Kafka records caused by producer retries within the supported producer session/protocol|Deduplication of business events produced independently, or duplicate database side effects|
|Consumer idempotency|Reapplying the same logical event to a business system|That the event is delivered only once|
|Kafka transactions|Atomic writes across Kafka partitions/topics and, when configured correctly, consumed offsets|Atomic updates to an arbitrary external database|
|Database transaction + unique event ID|Applying the same event's database effects more than once|Exactly-once effects in unrelated external systems|

> For a confluent-kafka Python producer, explicitly setting `enable.idempotence=True` makes your intent clear:
```
producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "goalstream-producer",
    "acks": "all",
    "enable.idempotence": True,
})
```