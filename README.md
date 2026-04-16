<!--
## Kafka Distributed Messaging Engine for Real-Time Chat

A ground-up implementation of a highly scalable, fault-tolerant, distributed event streaming system built entirely in Java, inspired by Apache Kafka. This project replaces traditional database chat architectures with a high-throughput, append-only log storage engine.

## Project Overview

Modern real-time communication platforms require backend systems capable of handling large volumes of concurrent data streams. Traditional synchronous request-response models and centralized databases face severe limitations in scalability and latency. 

This project solves this by introducing a **Custom Distributed Messaging Engine** that leverages partitioned data, asynchronous streaming, and decentralized cluster coordination.

## Key Architecture Modules

1. **Messaging Engine (Custom Broker)**: The core Java broker handling publisher/subscriber routing, topic partitioning, and offset management.
2. **API Gateway (Javalin)**: A highly performant REST interface routing HTTP requests from the frontend client into the TCP-based broker network.
3. **Log Storage Module (Java NIO)**: Bypasses traditional SQL/NoSQL databases in favor of lightning-fast **append-only `.log` files** and `.index` offset tracking via `File-Channel`.
4. **Distributed Coordination (ZooKeeper)**: Facilitates leader election, partition assignments, and split-brain recovery through dynamic heartbeat tracking.
5. **Tiered Storage Module**: A cloud archival simulation (`.cloud_bucket`) that automatically rolls off older log segments to maintain local disk optimization.
6. **Frontend Chat Dashboard**: A responsive, real-time user interface utilizing Vanilla JS, HTML, and Tailwind CSS (served via Vite) designed to continuously poll the backend with minimal latency.

## Prerequisites

* **Java 11+**
* **Apache Maven** (`mvn`)
* **Node.js & npm**
* **Apache ZooKeeper (v3.8+)** (extracted locally with `zoo.cfg` configured)

## Getting Started

### 1. Build the Backend
From the root directory (`EP`), package the Java project:
```bash
mvn clean package
```

### 2. Start the Cluster Pipeline
Run the infrastructure from the bottom-up:

1. **Start ZooKeeper:** Open your Zookeeper bin folder and run `zkServer.cmd`.
2. **Start the Brokers:** Double click the provided `start-cluster.bat` file to instantly launch your 3-node cluster.
3. **Start the API Gateway:**
   ```bash
   java -cp target/build-your-own-kafka-1.0-SNAPSHOT.jar com.simplekafka.api.ApiServer
   ```

### 3. Start the Frontend Application
In a separate terminal, run the Vite development server:
```bash
cd frontend
npm install
npm run dev
```

### 4. Test the End-to-End Flow
Open `http://localhost:5173` in two separate browser tabs side-by-side. Name one user "Alice" and the other "Bob". Send a message and watch your custom distributed broker handle the request, append it to disk, and synchronize the state instantly!

## Documentation

For an in-depth breakdown of the technical specifications, system design, and testing methodology, please review the `PROJECT_REPORT.md` included in this repository.


//set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot"
-->