
# Real Time Price Optimizer 

This project aims to provide a scalable and real-time solution for analyzing Amazon metadata using streaming data processing techniques. It is designed for students in university-level courses focusing on big data processing, streaming analytics, and database integration.

## Concepts Used

- **Data Preprocessing:** Cleaning and formatting the Amazon metadata dataset for analysis, ensuring it is suitable for streaming and frequent itemset mining processes.
- **Streaming Pipeline Setup:** Developing a producer application to stream preprocessed data in real-time and creating consumer applications to subscribe to this data stream.
- **Frequent Itemset Mining:** Implementing the Apriori and PCY (Park-Chen-Yu) algorithms for discovering frequent itemsets in a streaming context, along with innovative analysis in the third consumer application.
- **Database Integration:** Utilizing a non-relational database such as MongoDB to store the results of the frequent itemset mining for further analysis and retrieval.
- **Bash Script:** Enhancing project execution with a Bash script to automate the setup of Kafka components, including Kafka Connect and Zookeeper, as well as running the producer and consumer applications.

## Required Technologies


### Hadoop
Hadoop was utilized in this project for distributed storage and processing of large volumes of data, facilitating efficient analysis and manipulation of the Amazon metadata dataset.

#### Start Hadoop
```bash
start-all.sh
```

### Spark

Spark was employed in this project for real-time data processing and analysis, enabling streaming analytics and frequent itemset mining on the Amazon metadata dataset.

#### Start Kafka Zookeeper
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

#### Start Kafka Server
```bash
bin/kafka-server-start.sh config/server.properties
```

### MongoDB
MongoDB was utilized in this project as a NoSQL database for storing the results of frequent itemset mining and facilitating efficient retrieval and analysis of streaming data from the Amazon metadata dataset.

### Install MongoDB by following tutorial from this link
```bash
https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-ubuntu/
```

## Overview of Code


After preprocessing the data, the `producer.py` script is utilized to stream the preprocessed data into Kafka, facilitating real-time data ingestion. Following this, the `consumer_apriori.py` script subscribes to the Kafka topic and implements the Apriori algorithm to identify frequent itemsets in the transaction data. 

![Apriori](https://github.com/tahabintariq/i222014_i221996_i221913_BDA-Assignment-3/assets/110919745/3dc77187-988e-4376-8b7c-246b470c8806)

Similarly, the `consumer_pcy.py` script processes Kafka messages, employing the PCY algorithm to efficiently identify frequent pairs within the streaming data.

The `consumerfinal_pcy.py` script refines the PCY algorithm implementation to identify frequent combinations within the streaming data. It utilizes advanced hashing and counting techniques to efficiently process large transaction datasets, providing valuable insights into complex purchase patterns and market trends.

![PCY](https://github.com/tahabintariq/i222014_i221996_i221913_BDA-Assignment-3/assets/110919745/d6f6eb14-8477-43e0-9bb1-6778e06fae30)

Moreover, the `consumer_realtimepriceoptimizer.py` script integrates a dynamic pricing model, leveraging Kafka messages to update product demand and brand pricing information. It recommends optimal prices in real-time based on demand fluctuations and brand preferences, enabling responsive pricing strategies for maximizing revenue and customer satisfaction.

![RealTimePriceOptimizer](https://github.com/tahabintariq/i222014_i221996_i221913_BDA-Assignment-3/assets/110919745/eaa34b10-dd9e-4de4-9109-13ab249e7fdb)


## Kafka Clustering

In addition to local Kafka deployment, the project leverages Kafka clustering through Confluent Cloud, ensuring scalability and fault tolerance for streaming data processing. By distributing data across multiple Kafka brokers hosted on Confluent Cloud, the system enhances reliability and performance, enabling seamless handling of high-volume data streams and supporting real-time analytics at scale.

![1](https://github.com/tahabintariq/i222014_i221996_i221913_BDA-Assignment-3/assets/110919745/da682420-5b8c-4d19-830f-78028f171074)

![2](https://github.com/tahabintariq/i222014_i221996_i221913_BDA-Assignment-3/assets/110919745/e92d9c16-3220-4ab0-9661-c73db73cdd85)

## Conclusion

This project provides a comprehensive solution for analyzing Amazon metadata in a streaming environment, showcasing proficiency in data preprocessing, streaming pipeline setup, frequent itemset mining, database integration, and automation with a bonus Bash script. It serves as an educational resource for students studying big data processing and streaming analytics, offering hands-on experience with real-world datasets and techniques.

## Team Members
- Hasnain Raza (22I-1996)
- Haider Akbar (22I-1913)
- Taha Bin Tariq (22I-2014)





