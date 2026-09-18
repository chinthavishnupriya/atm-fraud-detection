# ATM Fraud Detection

A real-time ATM fraud detection system using **Apache Kafka, Apache Spark Structured Streaming, and Apache HBase**.

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
