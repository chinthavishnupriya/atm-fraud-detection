# ATM Fraud Detection

A real-time ATM fraud detection system using **Apache Kafka, Apache Spark Structured Streaming, Scala, Python, and Apache HBase**.

The project simulates ATM withdrawal events, publishes them to Kafka, processes them as a live stream, detects suspicious patterns using event-time windows, and stores the detected suspicious activity in HBase.

---

## 1. Problem Statement

An ATM network continuously generates withdrawal events. The system must detect potentially suspicious behavior from these real-time events.

Example:

```text
ACC101 → Hyderabad → ₹20,000
ACC101 → Mumbai    → ₹25,000
ACC101 → Delhi     → ₹30,000
```

The same account appearing in multiple locations within a short period may be suspicious.

### Required detections

1. Multiple withdrawals
2. High-value withdrawals
3. Withdrawals from multiple cities
4. Repeated failed PIN attempts

Suspicious transactions are stored in the HBase table:

```text
suspicious_transactions
```

---

## 2. Project Objectives

This project demonstrates a complete real-time big-data pipeline:

- Generate ATM transaction events.
- Publish events to Apache Kafka.
- Use a Kafka topic with 3 partitions.
- Consume the stream with Spark Structured Streaming.
- Parse JSON transaction data.
- Process events using event time.
- Use a 2-minute watermark.
- Use 5-minute stateful windows.
- Apply four fraud-detection rules.
- Store suspicious results in HBase.
- Verify the results through HBase Shell.

---

## 3. Technology Stack

| Technology | Version / Purpose |
|---|---|
| Apache Kafka | 3.4.1 |
| Apache Spark | 3.5.6 |
| Spark Structured Streaming | Real-time processing |
| Apache HBase | 2.5.15-hadoop3 |
| Hadoop | 3.3.6 |
| Scala | 2.12.18 |
| Python | Kafka producer |
| SBT | 2.0.7 |
| Java | 17 |
| ZooKeeper | Kafka coordination |

---

## 4. Architecture

```text
                  ATM TRANSACTION EVENTS
                           |
                           v
                 Python Kafka Producer
                           |
                           v
              Kafka: bank-transactions
                           |
              +------------+------------+
              |            |            |
          Partition 0  Partition 1  Partition 2
              |            |            |
              +------------+------------+
                           |
                           v
              Spark Structured Streaming
                           |
                    JSON Parsing
                           |
                     Event Time
                           |
                   2-Minute Watermark
                           |
                   5-Minute Windows
                           |
                           v
                 Fraud Detection Rules
                           |
       +-------------------+-------------------+
       |                   |                   |
       v                   v                   v
 High-value          Multiple withdrawals   Multiple cities
 withdrawal             (>3 / window)       (>3 cities/window)
       |                   |                   |
       +-------------------+-------------------+
                           |
                    Repeated failed PIN
                       (>3 / window)
                           |
                           v
                      HBase Writer
                           |
                           v
               suspicious_transactions
```

---

## 5. End-to-End Data Flow

### Step 1: Generate transactions

`kafka_fraud_producer.py` creates JSON events containing:

- transaction ID
- account ID
- customer ID
- transaction type
- amount
- timestamp
- location
- PIN status

Example:

```json
{
  "transactionId": "FRAUD005",
  "accountId": "ACC102",
  "customerId": "CUST102",
  "transactionType": "WITHDRAWAL",
  "amount": 75000,
  "timestamp": "2026-09-22T17:57:15",
  "location": "Hyderabad",
  "pinStatus": "SUCCESS"
}
```

### Step 2: Send to Kafka

The producer sends the JSON event to:

```text
Broker: localhost:9092
Topic: bank-transactions
Partitions: 3
```

The Kafka message key is `accountId`. Therefore, records with the same account key are consistently routed according to Kafka's key-based partitioning.

### Step 3: Process with Spark

Spark:

1. Reads the Kafka stream.
2. Extracts the JSON value.
3. Converts it into a structured transaction.
4. Converts the transaction timestamp into event time.
5. Filters invalid records.
6. Applies a 2-minute watermark.
7. Creates 5-minute windows.
8. Applies the fraud rules.
9. Sends suspicious results to HBase.

### Step 4: Store in HBase

The detected results are written to:

```text
suspicious_transactions
```

Each record contains common fraud information and fields specific to the detection rule.

---

## 6. Fraud Detection Rules

### 6.1 HIGH_VALUE_WITHDRAWAL

Condition:

```text
transactionType = WITHDRAWAL
AND amount > ₹50,000
```

Test:

```text
ACC102 → ₹75,000 → Hyderabad
```

Output:

```text
HIGH_VALUE_WITHDRAWAL
```

---

### 6.2 MULTIPLE_WITHDRAWALS

Condition:

```text
More than 3 withdrawals
for the same account
within a 5-minute window
```

Example:

```text
ACC101
Withdrawal 1
Withdrawal 2
Withdrawal 3
Withdrawal 4
```

Output:

```text
MULTIPLE_WITHDRAWALS
```

---

### 6.3 MULTIPLE_CITIES

Condition:

```text
More than 3 distinct cities
for the same account
within a 5-minute window
```

Test data:

```text
ACC101
Hyderabad
Mumbai
Delhi
Chennai
```

Output:

```text
MULTIPLE_CITIES
```

**Implementation note:** Spark Structured Streaming does not support the required exact distinct aggregation in this streaming query, so the implementation uses `approx_count_distinct(location)`.

---

### 6.4 REPEATED_FAILED_PIN

Condition:

```text
More than 3 failed PIN attempts
for the same account
within a 5-minute window
```

Test:

```text
ACC103
FAILED
FAILED
FAILED
FAILED
```

Output:

```text
REPEATED_FAILED_PIN
```

---

## 7. Event-Time and Stateful Processing

The application uses:

```text
Watermark: 2 minutes
Window:    5 minutes
```

### Why event time?

The transaction timestamp represents when the ATM event occurred. Event-time processing allows Spark to group transactions according to the event timestamp rather than only the time at which Spark receives the message.

### Why a watermark?

The watermark gives Spark a boundary for handling late events and managing streaming state.

### Why a 5-minute window?

The assignment requires suspicious behavior occurring within a short period. A 5-minute event-time window is used for the demonstration.

---

## 8. Kafka Configuration

Topic:

```text
bank-transactions
```

Broker:

```text
localhost:9092
```

Partitions:

```text
3
```

Create or verify:

```bash
bash scripts/create_kafka_topic.sh
```

Verify directly:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic bank-transactions
```

Expected:

```text
PartitionCount: 3
ReplicationFactor: 1
Partition: 0
Partition: 1
Partition: 2
```

The 3 partitions were verified during project execution.

---

## 9. HBase Design

### Table

```text
suspicious_transactions
```

### Column families

```text
transaction
fraud
```

### Common fields

```text
transaction:accountId
transaction:timestamp

fraud:reason
fraud:detectedAt
fraud:batchId
```

### High-value fields

```text
transactionId
customerId
transactionType
amount
location
pinStatus
```

### Multiple-withdrawal fields

```text
withdrawalCount
totalAmount
```

### Multiple-city fields

```text
cityCount
cities
```

### Failed-PIN fields

```text
failedPinAttempts
```

The row key contains the account, fraud reason, window information and Spark batch ID.

---

## 10. Source Files

### `kafka_fraud_producer.py`

Creates the demonstration transactions and sends them to Kafka.

Responsibilities:

- Build transaction JSON.
- Use `accountId` as the Kafka key.
- Send records to `bank-transactions`.
- Flush messages so they are published immediately.

### `Models.scala`

Defines the structured `BankTransaction` model.

Fields:

```text
transactionId
accountId
customerId
transactionType
amount
timestamp
location
pinStatus
```

### `KafkaStreamingProcessor.scala`

Main Spark application.

Responsibilities:

- Start Spark.
- Connect to Kafka.
- Parse JSON.
- Process event time.
- Apply watermarking.
- Create 5-minute windows.
- Detect all four fraud conditions.
- Start streaming queries.
- Send suspicious results to HBase.

### `HBaseWriter.scala`

Responsible for:

- Loading HBase configuration.
- Connecting to HBase.
- Opening `suspicious_transactions`.
- Writing common transaction/fraud fields.
- Writing rule-specific fields.
- Closing HBase resources.

### `scripts/create_kafka_topic.sh`

Creates or verifies the Kafka topic with 3 partitions.

---

## 11. Project Structure

```text
atm-fraud-detection/
│
├── README.md
├── .gitignore
├── build.sbt
├── project/
│   └── build.properties
│
├── kafka_fraud_producer.py
│
├── scripts/
│   └── create_kafka_topic.sh
│
├── src/
│   └── main/
│       └── scala/
│           ├── Models.scala
│           ├── KafkaStreamingProcessor.scala
│           └── HBaseWriter.scala
│
├── data/
│   └── input/
│
└── docs/
    ├── execution-output.txt
    └── outputs/
        ├── HIGH_VALUE_WITHDRAWAL.png
        ├── KAFKA_PRODUCER_OUTPUT.png.png
        ├── MULTIPLE_CITIES.png
        ├── MULTIPLE_WITHDRAWALS.png
        ├── REPEATED_FAILED_PIN.png
        ├── SBT_COMPILE_OUTPUT.png.png
        └── SPARK_FRAUD_OUTPUT.png.png
```

---

## 12. Environment Setup

The demonstrated environment contains:

```text
Java 17
Hadoop 3.3.6
Kafka 3.4.1
Spark 3.5.6
Scala 2.12.18
HBase 2.5.15-hadoop3
SBT 2.0.7
```

Required services:

- Hadoop/HDFS
- HBase Master
- HBase RegionServer
- Kafka ZooKeeper
- Kafka broker

The Python environment also needs the Kafka client package used by the producer.

---

## 13. Running the Project

### Terminal 1: Check services

```bash
jps
```

Make sure the required Hadoop, HBase, ZooKeeper and Kafka services are running.

### Terminal 2: Prepare the project

```bash
cd ~/atm-fraud-detection
```

Verify Kafka:

```bash
bash scripts/create_kafka_topic.sh
```

Compile:

```bash
sbt compile
```

Start Spark:

```bash
sbt run
```

**Important:** Spark uses `startingOffsets=latest`, so start Spark before running the producer.

### Terminal 3: Send transactions

```bash
cd ~/atm-fraud-detection
python3 kafka_fraud_producer.py
```

The producer sends the prepared test transactions.

### Terminal 4: Verify HBase

```bash
/opt/hbase-2.5.15-hadoop3/bin/hbase shell
```

Then:

```text
scan 'suspicious_transactions'
```

---

## 14. Test Transactions

### ACC101 — multiple withdrawals and cities

```text
FRAUD001 → ₹5,000  → Hyderabad → SUCCESS
FRAUD002 → ₹10,000 → Hyderabad → SUCCESS
FRAUD003 → ₹15,000 → Mumbai    → SUCCESS
FRAUD004 → ₹20,000 → Delhi     → SUCCESS
FRAUD010 → ₹12,000 → Chennai   → SUCCESS
```

This account is used for multiple-withdrawal and multiple-city detection.

### ACC102 — high value

```text
FRAUD005 → ₹75,000 → Hyderabad → SUCCESS
```

This triggers the high-value rule.

### ACC103 — failed PIN attempts

```text
FRAUD006 → ₹0 → Hyderabad → FAILED
FRAUD007 → ₹0 → Hyderabad → FAILED
FRAUD008 → ₹0 → Hyderabad → FAILED
FRAUD009 → ₹0 → Hyderabad → FAILED
```

This triggers the repeated failed-PIN rule.

---

## 15. HBase Verification Commands

Run these focused scans to verify each rule:

### Multiple withdrawals

```text
scan 'suspicious_transactions', {STARTROW => 'ACC101_MULTIPLE_WITHDRAWALS_', LIMIT => 3}
```

Expected important fields:

```text
accountId       ACC101
withdrawalCount 4
totalAmount     50000.0
reason          MULTIPLE_WITHDRAWALS
```

### Multiple cities

```text
scan 'suspicious_transactions', {STARTROW => 'ACC101_MULTIPLE_CITIES_', LIMIT => 3}
```

Expected:

```text
accountId ACC101
cityCount 4
cities    Chennai,Delhi,Mumbai,Hyderabad
reason    MULTIPLE_CITIES
```

### High-value withdrawal

```text
scan 'suspicious_transactions', {STARTROW => 'ACC102_HIGH_VALUE_WITHDRAWAL_', LIMIT => 3}
```

Expected:

```text
accountId ACC102
amount    75000.0
location  Hyderabad
reason    HIGH_VALUE_WITHDRAWAL
```

### Repeated failed PIN

```text
scan 'suspicious_transactions', {STARTROW => 'ACC103_REPEATED_FAILED_PIN_', LIMIT => 3}
```

Expected:

```text
accountId         ACC103
failedPinAttempts 4
reason            REPEATED_FAILED_PIN
```

---

## 16. Verified Results

The final HBase verification successfully found all four required detection categories.

- **ACC101:** multiple withdrawals with a withdrawal count of 4 and total amount ₹50,000.
- **ACC101:** activity across 4 cities — Chennai, Delhi, Mumbai and Hyderabad.
- **ACC102:** ₹75,000 withdrawal flagged as high value.
- **ACC103:** 4 failed PIN attempts flagged as repeated failed PIN activity.

These results were verified directly using HBase Shell.

---

## 17. Execution Evidence

The repository contains seven output screenshots in `docs/outputs/`:

| File | Evidence |
|---|---|
| `KAFKA_PRODUCER_OUTPUT.png.png` | Producer successfully sends transactions |
| `SBT_COMPILE_OUTPUT.png.png` | Scala project compilation |
| `SPARK_FRAUD_OUTPUT.png.png` | Spark streaming/fraud processing |
| `HIGH_VALUE_WITHDRAWAL.png` | High-value HBase result |
| `MULTIPLE_WITHDRAWALS.png` | Multiple-withdrawal HBase result |
| `MULTIPLE_CITIES.png` | Multiple-city HBase result |
| `REPEATED_FAILED_PIN.png` | Failed-PIN HBase result |

Historical execution text is stored in:

```text
docs/execution-output.txt
```

---

## 18. Assignment Requirement Mapping

| Assignment requirement | Implementation |
|---|---|
| Real-time ATM withdrawal events | Python producer |
| Kafka producer | `kafka_fraud_producer.py` |
| Kafka partitions | 3 partitions |
| Spark Structured Streaming | `KafkaStreamingProcessor.scala` |
| Stateful/window processing | 5-minute event-time windows |
| Watermark/state management | 2-minute watermark |
| Multiple withdrawals | Implemented |
| High-value withdrawals | Implemented |
| Multiple cities | Implemented |
| Repeated failed PIN attempts | Implemented |
| HBase storage | `HBaseWriter.scala` |
| Suspicious transaction table | `suspicious_transactions` |
| Verification | HBase Shell + screenshots |

---

## 19. Important Implementation Notes

### Kafka starting offset

The Spark Kafka source uses:

```text
startingOffsets = latest
```

Therefore:

```text
Start Spark → then start Producer
```

### Multiple-city aggregation

The implementation uses:

```text
approx_count_distinct(location)
```

instead of exact `countDistinct`, because of Spark Structured Streaming aggregation limitations.

### HBase update mode

The aggregation queries use update mode. The HBase row key includes the Spark batch ID. Consequently, multiple rows can exist for different streaming updates of the same account/window. This is expected for the current demonstration.

### Kafka replication

The demonstrated Kafka topic uses:

```text
ReplicationFactor: 1
```

This is suitable for the local academic/demo environment, not a production Kafka cluster.

---

## 20. Troubleshooting

### Kafka topic problem

```bash
bash scripts/create_kafka_topic.sh
```

Then:

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic bank-transactions
```

### Spark receives no transactions

Check:

1. Kafka broker is running.
2. Topic is `bank-transactions`.
3. Broker is `localhost:9092`.
4. Spark was started before the producer.
5. Producer completed successfully.

### HBase contains no new records

Check:

1. HBase Master is running.
2. RegionServer is running.
3. `suspicious_transactions` exists.
4. Spark is running.
5. Kafka producer sent the transactions.

### SBT compilation

Use separate commands:

```bash
sbt clean
sbt compile
```

This is more reliable in the demonstrated environment than combining the two commands.

---

## 21. Limitations

This is an academic/demo rule-based streaming system.

It does **not** implement:

- Machine-learning fraud scoring
- Production authentication
- Multi-broker Kafka replication
- Production HBase clustering
- Email/SMS alerts
- Web dashboard
- Fraud case-management workflow

The four rules are deterministic so the assignment scenarios can be reproduced and verified.

---

## 22. Quick Start

```bash
cd ~/atm-fraud-detection

bash scripts/create_kafka_topic.sh

sbt compile

sbt run
```

In another terminal:

```bash
cd ~/atm-fraud-detection
python3 kafka_fraud_producer.py
```

Then:

```bash
/opt/hbase-2.5.15-hadoop3/bin/hbase shell
```

And:

```text
scan 'suspicious_transactions'
```

---

## 23. Final Pipeline

```text
ATM Events
    ↓
Python Producer
    ↓
Kafka
    ↓
bank-transactions
    ↓
3 Kafka Partitions
    ↓
Spark Structured Streaming
    ↓
JSON Parsing
    ↓
Event Time
    ↓
2-Minute Watermark
    ↓
5-Minute Windows
    ↓
4 Fraud Detection Rules
    ↓
HBase Writer
    ↓
suspicious_transactions
```

### Final status

```text
Kafka Producer                 ✓
Kafka Topic                    ✓
3 Kafka Partitions             ✓
Spark Structured Streaming     ✓
Event-time processing          ✓
Stateful/window processing     ✓
Multiple withdrawals           ✓
High-value withdrawal          ✓
Multiple cities                ✓
Repeated failed PIN            ✓
HBase storage                  ✓
HBase verification             ✓
Compilation                    ✓
Execution evidence             ✓
Documentation                  ✓
```

**Project status: COMPLETE**
