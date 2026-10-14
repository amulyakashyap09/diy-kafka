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