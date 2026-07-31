import pandas as pd
import json
import time
from kafka import KafkaProducer
import gc
# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load the dataset
df = pd.read_csv('./data/processed/X_train.csv')
output = pd.read_csv('./data/processed/y_train.csv')
df = pd.concat([df, output], axis=1)


# df already has isFraud as int64 and aligned with X
print(df['isFraud'].dtype)        # should be int64
print(df['isFraud'].value_counts())

# target per class
fraud_min = df['isFraud'].value_counts().min()

# shuffle once, then take first fraud_min rows per class
df_balanced = (
    df.sample(frac=1, random_state=42)   # shuffle
      .groupby('isFraud', group_keys=False)
      .head(fraud_min)
      .reset_index(drop=True)
)

print("Balanced isFraud distribution:")
print(df_balanced['isFraud'].value_counts())
print("Columns:", df_balanced.columns)


df_balanced = df_balanced[:500]

del df
gc.collect()



print("Starting to stream transactions...")
print("Press Ctrl+C to stop")
print("-" * 40)

# Send each transaction one by one
for index, row in df_balanced.iterrows():
    # Convert row to dictionary
    transaction = row.to_dict()
    
    # Send to Kafka topic called 'transactions_raw'
    producer.send('transactions_raw', value=transaction)
    
    # Show what we sent
    actual_class = "FRAUD" if row["isFraud"] == 1 else "normal"
    print(f"Sent transaction #{index} | Amount: ${row['TransactionAmt']:.2f} | Actual: {actual_class}")
    
    # Wait 0.5 seconds between transactions
    time.sleep(0.5)

producer.flush()
print("Done streaming all transactions.")