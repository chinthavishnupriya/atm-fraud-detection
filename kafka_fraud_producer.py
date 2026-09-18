from kafka import KafkaProducer
import json
import time
from datetime import datetime


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def send_transaction(
    transaction_id,
    account_id,
    customer_id,
    transaction_type,
    amount,
    location,
    pin_status="SUCCESS"
):
    transaction = {
        "transactionId": transaction_id,
        "accountId": account_id,
        "customerId": customer_id,
        "transactionType": transaction_type,
        "amount": amount,
        "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "location": location,
        "pinStatus": pin_status
    }

    producer.send(
        "bank-transactions",
        key=account_id.encode("utf-8"),
        value=transaction
    )

    producer.flush()

    print("Sent:", transaction)


print("ATM Fraud Detection Kafka Producer")
print("Sending test transactions...\n")


# ==========================================================
# ACC101 - Multiple withdrawals + Multiple cities
# ==========================================================

# 1. Withdrawal - Hyderabad
send_transaction(
    "FRAUD001",
    "ACC101",
    "CUST101",
    "WITHDRAWAL",
    5000,
    "Hyderabad"
)

time.sleep(2)


# 2. Withdrawal - Hyderabad
send_transaction(
    "FRAUD002",
    "ACC101",
    "CUST101",
    "WITHDRAWAL",
    10000,
    "Hyderabad"
)

time.sleep(2)


# 3. Withdrawal - Mumbai
send_transaction(
    "FRAUD003",
    "ACC101",
    "CUST101",
    "WITHDRAWAL",
    15000,
    "Mumbai"
)

time.sleep(2)


# 4. Withdrawal - Delhi
send_transaction(
    "FRAUD004",
    "ACC101",
    "CUST101",
    "WITHDRAWAL",
    20000,
    "Delhi"
)

time.sleep(2)


# 5. High-value withdrawal
send_transaction(
    "FRAUD005",
    "ACC102",
    "CUST102",
    "WITHDRAWAL",
    75000,
    "Hyderabad"
)

time.sleep(2)


# 6. Failed PIN - Hyderabad
send_transaction(
    "FRAUD006",
    "ACC103",
    "CUST103",
    "WITHDRAWAL",
    0,
    "Hyderabad",
    "FAILED"
)

time.sleep(2)


# 7. Failed PIN - Hyderabad
send_transaction(
    "FRAUD007",
    "ACC103",
    "CUST103",
    "WITHDRAWAL",
    0,
    "Hyderabad",
    "FAILED"
)

time.sleep(2)


# 8. Failed PIN - Hyderabad
send_transaction(
    "FRAUD008",
    "ACC103",
    "CUST103",
    "WITHDRAWAL",
    0,
    "Hyderabad",
    "FAILED"
)

time.sleep(2)


# 9. Failed PIN - Hyderabad
send_transaction(
    "FRAUD009",
    "ACC103",
    "CUST103",
    "WITHDRAWAL",
    0,
    "Hyderabad",
    "FAILED"
)

time.sleep(2)


# 10. FOURTH DISTINCT CITY - Chennai
# ACC101 now has:
# Hyderabad, Mumbai, Delhi, Chennai
send_transaction(
    "FRAUD010",
    "ACC101",
    "CUST101",
    "WITHDRAWAL",
    12000,
    "Chennai"
)

time.sleep(2)


producer.close()

print("\nAll test transactions sent.")
