from kafka import KafkaConsumer
import json
import itertools
import numpy as np
from BitVector import BitVector
from pymongo import MongoClient

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017')
db = client['PCYDB']
pcy_collection = db['pcy_frequent_itemsets']

# Configuration parameters for the PCY algorithm
min_support = 3  # Adjust based on your needs, possibly as a percentage
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

def hash_combination(combination):
    """Hash function for combinations using a simple summation hash."""
    return sum(hash(item) for item in combination) % bucket_size

def update_buckets(basket, buckets):
    """Update hash buckets based on all combinations in the basket."""
    for size in range(2, len(basket) + 1):  # Start from pairs to full combination
        for combination in itertools.combinations(basket, size):
            bucket_index = hash_combination(combination)
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
                for size in range(1, len(transaction) + 1):
                    for combination in itertools.combinations(transaction, size):
                        if size == 1 or bit_vector[hash_combination(combination)]:
                            if combination not in item_count:
                                item_count[combination] = 0
                            item_count[combination] += 1
            
            total_transactions = len(transactions)
            # Store frequent combinations and their support in MongoDB
            for combination, count in item_count.items():
                if count >= min_support:
                    support = count / total_transactions
                    print(f"Frequent combination: {combination}, support count: {count}, support: {support:.2f}")
                    pcy_collection.insert_one({
                        "combination": combination,
                        "support_count": count,
                        "support": support
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

