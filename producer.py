import json
from kafka import KafkaProducer
from time import sleep

def load_dataset(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

def create_producer():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    return producer

def send_data(producer, topic, data):
    for item in data:
        producer.send(topic, value=item)
        producer.flush()  
        print(f"Sent data: {item}")
        sleep(1)

def main():
    file_path = 'processed_sample.json'
    data = load_dataset(file_path)
    producer = create_producer()
    topic = 'amazon_metadata'
    
    send_data(producer, topic, data)

if __name__ == '__main__':
    main()
