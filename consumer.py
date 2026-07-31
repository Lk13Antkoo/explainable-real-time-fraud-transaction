import json
import requests
from kafka import KafkaConsumer

# Connect to Kafka and subscribe to the topic
consumer = KafkaConsumer(
    'transactions_raw',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

print("Fraud Detection Consumer started...")
print("Listening for transactions...")
print("-" * 50)

# Keep reading messages forever
for message in consumer:
    transaction = message.value
    
    # Remove the Class column before sending to API
    # (API doesn't expect this field)
    transaction.pop('Class', None)
    
    # Call your fraud detection API
    response = requests.post(
        'http://127.0.0.1:8000/score',
        json=transaction
    )
    
    result = response.json()
    risk = result['risk_score']
    flagged = result['is_flagged']
    amount = transaction['Amount']
    
    # Print result with clear formatting
    if flagged:
        status = "🚨 SUSPICIOUS"
    else:
        status = "✅ NORMAL"
    
    print(f"{status} | Risk: {risk:.4f} | Amount: €{amount:.2f}")