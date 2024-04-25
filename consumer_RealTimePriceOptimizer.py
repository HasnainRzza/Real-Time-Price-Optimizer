from kafka import KafkaConsumer
import json
import numpy as np

class DynamicPricingModel:
    def __init__(self):
        self.product_demand = {}
        self.brand_pricing = {}
        self.all_prices = []  # To dynamically set a reasonable default price

    def update_demand(self, transaction):
        for product in transaction['also_buy']:
            self.product_demand[product] = self.product_demand.get(product, 0) + 1

    def update_brand_pricing(self, product, price, brand):
        # Remove currency symbols and split ranges
        try:
            # Strip '$' and any other non-numeric except '.' and ','
            clean_price = price.replace('$', '').replace(',', '').strip()
            # Check if it's a range
            if '-' in clean_price:
                low, high = clean_price.split('-')
                low = float(low.strip())
                high = float(high.strip())
                # Use the average of the range
                price = (low + high) / 2
            else:
                price = float(clean_price)
        except ValueError as e:
            print(f"Warning: Invalid price data '{price}' for product {product}: {e}")
            return  # Skip this entry if the price isn't valid

        if price > 0:  # Ensure only positive prices are considered
            if brand in self.brand_pricing:
                self.brand_pricing[brand].append(price)
            else:
                self.brand_pricing[brand] = [price]

        self.all_prices.append(price)  # Add to overall prices for fallback default pricing

    def recommend_price(self, asin, brand):
        prices = self.brand_pricing.get(brand, [])
        avg_brand_price = np.mean(prices) if prices else np.mean(self.all_prices)

        demand_factor = self.product_demand.get(asin, 0)
        recommended_price = avg_brand_price * (1 + demand_factor / 100)  # Adjust price based on demand

        return recommended_price

def create_consumer():
    consumer = KafkaConsumer(
        'amazon_metadata',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='DynamicPricing-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def process_messages(consumer, model):
    for message in consumer:
        data = message.value
        if 'asin' in data and 'also_buy' in data and 'price' in data and 'brand' in data:
            model.update_demand(data)
            model.update_brand_pricing(data['asin'], data['price'], data['brand'])
            new_price = model.recommend_price(data['asin'], data['brand'])
            print(f"ASIN: {data['asin']}, Current Price: {data['price']}, Recommended Price: {new_price:.2f}")

def main():
    consumer = create_consumer()
    model = DynamicPricingModel()
    process_messages(consumer, model)

if __name__ == '__main__':
    main()
