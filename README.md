# ATM Fraud Detection

A real-time ATM fraud detection system using **Apache Kafka, Apache Spark Structured Streaming, and Apache HBase**.

## Problem Statement

An ATM network generates real-time withdrawal events. The system detects potentially suspicious behavior from streaming transactions and stores detected suspicious activity in HBase.

### Example

```text
ACC101 → Hyderabad → ₹20,000
ACC101 → Mumbai    → ₹25,000
ACC101 → Delhi     → ₹30,000
```

Transactions from the same account across multiple cities within a short time can be flagged as suspicious.

## Technologies

- Apache Kafka
- Apache Spark Structured Streaming
- Apache HBase
- Scala 2.12.18
- Python
- SBT

## Architecture

```text
ATM Transactions
       |
       v
Kafka Producer
       |
       v
Kafka Topic: bank-transactions
       |
       |  3 partitions
       v
Spark Structured Streaming
       |
       +--------------------------+
       |                          |
       v                          v
Transaction Stream        Fraud Detection Rules
                                  |
              +-------------------+-------------------+
              |                   |                   |
              v                   v                   v
       High-value          Multiple withdrawals   Multiple cities
       withdrawal             (>3 / 5 min)          (>3 / 5 min)
                                  |
                                  v
                         Repeated failed PIN
                           (>3 / 5 min)
                                  |
                                  v
                               HBase
                     suspicious_transactions
```

## Fraud Detection Rules

| Rule | Condition |
|---|---|
| HIGH_VALUE_WITHDRAWAL | Withdrawal amount > ₹50,000 |
| MULTIPLE_WITHDRAWALS | More than 3 withdrawals in a 5-minute window |
| MULTIPLE_CITIES | More than 3 distinct cities in a 5-minute window |
| REPEATED_FAILED_PIN | More than 3 failed PIN attempts in a 5-minute window |

Spark uses a 2-minute event-time watermark and 5-minute windows for the stateful fraud-detection aggregations.

## Kafka Partitions

The Kafka topic is configured with **3 partitions**.

Create the topic before starting the Spark application:

```bash
bash scripts/create_kafka_topic.sh
```

Verify the partition count:

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic bank-transactions
```

The producer uses `accountId` as the Kafka message key. Kafka therefore routes messages with the same account key consistently to the same partition.

## Project Structure

```text
atm-fraud-detection/
├── README.md
├── .gitignore
├── build.sbt
├── kafka_fraud_producer.py
├── scripts/
│   └── create_kafka_topic.sh
├── data/
│   └── input/
├── src/
│   └── main/
│       └── scala/
│           ├── Models.scala
│           ├── KafkaStreamingProcessor.scala
│           └── HBaseWriter.scala
└── docs/
    ├── execution-output.txt
    └── outputs/
```

## Run

### 1. Start Kafka

Make sure ZooKeeper and Kafka are running.

### 2. Create/verify the Kafka topic

```bash
bash scripts/create_kafka_topic.sh
```

### 3. Start Spark Structured Streaming

```bash
sbt run
```

### 4. Send ATM transactions

In another terminal:

```bash
python3 kafka_fraud_producer.py
```

### 5. Check HBase

```bash
/opt/hbase-2.5.15-hadoop3/bin/hbase shell
```

Then:

```text
scan 'suspicious_transactions'
```

## Test Transactions

The included producer generates test data for all four rules:

- **ACC101** — multiple withdrawals and four cities: Hyderabad, Mumbai, Delhi, Chennai
- **ACC102** — ₹75,000 withdrawal
- **ACC103** — four failed PIN attempts

## Output

The execution produces suspicious records in the HBase table:

```text
suspicious_transactions
├── HIGH_VALUE_WITHDRAWAL
├── MULTIPLE_WITHDRAWALS
├── MULTIPLE_CITIES
└── REPEATED_FAILED_PIN
```

See [execution-output.txt](docs/execution-output.txt) for the recorded execution output.

### Output Screenshots

Output screenshots are stored under [docs/outputs](docs/outputs/).

- Kafka producer output
- SBT compilation output
- Spark fraud-detection output
- HIGH_VALUE_WITHDRAWAL
- MULTIPLE_WITHDRAWALS
- MULTIPLE_CITIES
- REPEATED_FAILED_PIN

## HBase Storage

The HBase table is:

```text
suspicious_transactions
```

Fraud records include the account, detection reason, detection time, batch ID, and rule-specific information such as amount, withdrawal count, city count, cities, or failed PIN attempts.

## Notes

- Kafka: `localhost:9092`
- Topic: `bank-transactions`
- Partitions: **3**
- Spark: **3.5.6**
- Scala: **2.12.18**
- HBase: **2.5.15-hadoop3**
- Java: **17**
