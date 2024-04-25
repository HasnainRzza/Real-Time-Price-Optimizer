from kafka import KafkaConsumer
import json
import itertools
import numpy as np
from pymongo import MongoClient
from BitVector import BitVector

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017')
db = client['PCYDB']
pcy_collection = db['pcy_frequent_pairs']

# Configuration parameters for the PCY algorithm
min_support = 3
bucket_size = 50000  # Set size of hash buckets

def create_consumer(topic):
    """Create a Kafka consumer for a given topic."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='PCY-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def hash_pair(pair):
    """Hash function for the pairs."""
    return (hash(pair[0]) + hash(pair[1])) % bucket_size

def update_buckets(basket, buckets):
    """Update hash buckets based on pairs in the basket."""
    for pair in itertools.combinations(basket, 2):
        bucket_index = hash_pair(pair)
        buckets[bucket_index] += 1
    return buckets

def get_bit_vector(buckets):
    """Generate a bit vector from the hash buckets based on the min support."""
    bit_vector = BitVector(size=bucket_size)
    for i in range(bucket_size):
        if buckets[i] >= min_support:
            bit_vector[i] = 1
    return bit_vector

def process_messages(consumer):
    """Process messages from Kafka and apply the PCY algorithm."""
    transactions = []
    buckets = np.zeros(bucket_size, dtype=int)  # Initialize hash buckets

    for message in consumer:
        data = message.value
        if 'asin' in data and 'also_buy' in data and data['also_buy']:
            # Append the current product asin with also_buy items as a single transaction
            transaction = [data['asin']] + data['also_buy']
            transactions.append(transaction)
            # Update buckets based on the current transaction
            update_buckets(transaction, buckets)

        if len(transactions) >= 100:  # Process every 100 transactions
            bit_vector = get_bit_vector(buckets)
            # Pass 2 - Generate frequent itemsets based on the bit vector
            item_count = {}
            for transaction in transactions:
                for item in transaction:
                    if item not in item_count:
                        item_count[item] = 0
                    item_count[item] += 1
            
            frequent_items = [item for item, count in item_count.items() if count >= min_support]
            candidate_pairs = {}
            for transaction in transactions:
                for pair in itertools.combinations(transaction, 2):
                    if bit_vector[hash_pair(pair)]:
                        if pair not in candidate_pairs:
                            candidate_pairs[pair] = 0
                        candidate_pairs[pair] += 1
            
            # Store frequent pairs and their support count in MongoDB
            for pair, count in candidate_pairs.items():
                if count >= min_support:
                    print(f"Frequent pair: {pair} with support count: {count}")
                    pcy_collection.insert_one({
                        "pair": pair,
                        "support_count": count
                    })

            # Reset for next batch
            transactions = []
            buckets = np.zeros(bucket_size, dtype=int)

def main():
    topic = 'amazon_metadata'
    consumer = create_consumer(topic)
    process_messages(consumer)

if __name__ == '__main__':
    main()

