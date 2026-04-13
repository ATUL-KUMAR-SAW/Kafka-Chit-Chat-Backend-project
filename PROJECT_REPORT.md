# 📚 Final Year Project Report: Build Your Own Kafka 

**Project Title:** Building a Simplified Kafka-like Distributed Messaging System  
**Frameworks Used:** Java, Apache ZooKeeper, Apache Maven  

## 1. Abstract
The goal of this project is to implement a highly customized, simplified version of the widely-used distributed messaging system natively inspired by Apache Kafka. Instead of simply building an application *on top* of Kafka, we build the core internals—understanding how data partitioning, node-to-node network protocols, cluster formation, leader election, and resilient disk-backed message storage act fundamentally at scale.

## 2. Project Architecture & Internals

### 2.1 Topic Partitioning and Distribution
In enterprise messaging systems, data queues ("topics") are heavily loaded. We distribute topic data into smaller units called **Partitions**. 
* **Implementation:** Our Broker logically assigns incoming messages to individual partitions. These partitions are distributed evenly across our broker nodes. This avoids any single point of network or storage failure.

### 2.2 Leader Election & Cluster Coordination 
We utilized **Apache ZooKeeper** to implement distributed consensus.
* **Controller Role:** When the brokers start up, they race to create an Ephemeral Node in ZooKeeper. The first broker to succeed becomes the cluster's Controller. 
* **Failover Mechanism:** If the Controller crashes, its ephemeral node vanishes. ZooKeeper alerts the other brokers, who then initiate a new election sequence to pick the next controller.

### 2.3 The Storage Layer (Log Segments & Indices)
Unlike traditional databases (MySQL, MongoDB) which use B-trees and random disk I/O, messaging queuing relies on continuous sequential I/O (which is significantly faster).
* **Append-Only Logs:** Received messages are sequentially appended to a log file on the file system representing a specific partition.
* **Offsets:** Consumers maintain "offsets" (an integer pointer), reading messages systematically and preventing the broker from needing to constantly erase data after delivery.

### 2.4 Custom Network Protocol
A binary request/response protocol handles lightweight TCP socket transmissions. The clients dynamically serialize metadata (Topic Name, Partition ID, Payload) over sockets to the Brokers, and vice-versa, avoiding overhead seen in bloated REST APIs.

## 3. Results & Execution Path

The environment is successfully configured to run three concurrent isolated brokers mimicking a real-world datacenter cluster.
We confirmed:
1. **Network Binding:** Brokers successfully bind and listen to differing network ports (9091, 9092, 9093).
2. **Cluster Health:** ZooKeeper recognizes and synchronizes the active brokers.
3. **Data Integrity:** The native `SimpleKafkaProducer` effectively encodes strings and forwards them to the primary partition leader broker. The `SimpleKafkaConsumer` reliably pulls messages using index offsets.

## 4. Conclusion & Future Enhancements

Through this deep dive, the architectural constraints of distributed messaging were overcome. In modern big-data pipelines (ex: Uber driver locations, Netflix stream processing), message brokers form the heart of the tech stack.

**Future Considerations:**
- **Replication**: Implement replica followers that duplicate data from the Leader partition.
- **Consumer Groups**: Adding offset-tracking so consumers can pause and resume pulling messages seamlessly.
- **Load Balancing:** Implementing Round-robin packet distribution natively within the Producer. 
