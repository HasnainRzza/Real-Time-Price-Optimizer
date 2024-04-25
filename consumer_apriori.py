from kafka import KafkaConsumer
import json

# Configuration parameters for the Apriori algorithm
min_support = 3
min_confidence = 0.1  # You can adjust this threshold as needed

def create_consumer(topic):
    """Create a Kafka consumer for a given topic."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='Apriori-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def apriori_gen(itemset, length):
    """Generate new candidate item sets of size k+1."""
    candidates = []
    for i in range(length):
        for j in range(i + 1, length):
            # Join step: create candidate k+1 item sets
            l1, l2 = sorted(itemset[i]), sorted(itemset[j])
            if l1[:-1] == l2[:-1]:  # only last item should be different
                candidate = l1[:-1] + [l1[-1], l2[-1]]
                candidates.append(candidate)
    return candidates

def apriori_prune(candidates, min_support, transaction_list):
    """Prune candidates that do not meet the minimum support."""
    item_counts = {tuple(item): 0 for item in candidates}
    for transaction in transaction_list:
        for candidate in candidates:
            if all(item in transaction for item in candidate):
                item_counts[tuple(candidate)] += 1
    total_transactions = len(transaction_list)
    return {item: (count, count / total_transactions) for item, count in item_counts.items() if count >= min_support}

def process_messages(consumer):
    """Process messages from Kafka and apply the Apriori algorithm."""
    transactions = []
    for message in consumer:
        data = message.value
        if 'asin' in data and 'also_buy' in data and data['also_buy']:
            # Append the current product asin with also_buy items as a single transaction
            transaction = [data['asin']] + data['also_buy']
            transactions.append(transaction)

        if len(transactions) >= 100:  # Process every 100 transactions
            # Initial itemset generation (C1)
            itemset = {tuple([item]): sum(item in t for t in transactions) for transaction in transactions for item in transaction}
            frequent_itemsets = apriori_prune(itemset.keys(), min_support, transactions)

            k = 2
            while frequent_itemsets:
                # Printing frequent itemsets and their support count and confidence
                for items, (count, support) in frequent_itemsets.items():
                    print(f"Frequent {k}-itemset: {items}")
                    print(f"Support Count: {count}")
                    print(f"Support: {support:.3f}")
                    
                    # Calculate and print confidence for each possible rule derived from the itemset
                    for item in items:
                        base_set = tuple([x for x in items if x != item])
                        base_count = itemset.get(tuple(base_set), 0)  # Ensure we are fetching from itemset correctly
                        if base_count > 0:
                            confidence = count / base_count
                            if confidence >= min_confidence:
                                print(f"Rule: {base_set} -> {item}")
                                print(f"Confidence: {confidence:.3f}")
                                print("-" * 10)

                # Generate candidates and prune again for next level of itemsets
                candidate_itemsets = apriori_gen(list(frequent_itemsets.keys()), len(frequent_itemsets))
                frequent_itemsets = apriori_prune(candidate_itemsets, min_support, transactions)
                k += 1

            transactions = []  # Reset transactions for the next batch

def main():
    topic = 'amazon_metadata'
    consumer = create_consumer(topic)
    process_messages(consumer)

if __name__ == '__main__':
    main()
