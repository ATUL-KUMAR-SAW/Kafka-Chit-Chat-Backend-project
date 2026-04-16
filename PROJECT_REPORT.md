<!--
# Project Report: Kafka Distributed Messaging Engine

**Author**: Atul Kumar  
**Domain**: Distributed Systems / Software Engineering

---

## 1. Abstract
The rapid growth of real-time communication platforms demands highly scalable, fault-tolerant backend systems. Traditional chat systems relying on synchronous request-response models and centralized databases face critical limitations in throughput and latency.

This project implements a custom **distributed event streaming system** inspired by the architecture of Apache Kafka. Developed in Java (JDK 11), it features an append-only log storage mechanism, decentralized coordination via Apache ZooKeeper, and a lightweight REST API Gateway (Javalin). The system demonstrates high throughput, low latency, and reliable message delivery under continuous operation.

---

## 2. System Architecture & Methodology

### 2.1 The Producer-Broker-Consumer Model
The system enforces strict decoupling between system components to enable independent scaling.

* **API Gateway Module:** Functions as the entry point. Parses incoming POST/GET requests and forwards them asynchronously into the messaging engine.
* **Messaging Engine:** Assigns incoming payloads to specific **Topics** and **Partitions**. Determines partition routing based on hashing to ensure even distribution of workload across the cluster.
* **Consumer Layer:** Reads data strictly based on sequence **Offsets**. This enables replayability and highly consistent fault recovery.

### 2.2 Log Storage Module (Java NIO File-Channel)
A massive departure from traditional database storage:
* Data is written to `.log` segments on disk sequentially.
* `.index` files map message offsets to precise byte positions.
* Utilizing Java NIO `File-Channel` bypasses overhead, operating directly at the file system level for highly optimized sequential I/O.
* **Tiered Storage:** When local log segments hit specific age/size thresholds, they are autonomously archived into a `.cloud_bucket` directory, simulating cloud-tier lifecycle management.

### 2.3 Distributed Coordination (ZooKeeper)
A multi-node architecture requires consensus to prevent split-brain scenarios. We integrated Apache ZooKeeper to manage:
1. **Broker Registration & Discovery** (via Ephemeral znodes)
2. **Partition Leadership** (Leader/Replica assignments)
3. **Consumer Group Checkpointing** (Preventing duplicate message consumption)
4. **Heartbeat Monitoring & Failover**

---

## 3. Implementation Workflow

### Message Delivery Pipeline
1. **Frontend Production:** User input is collected by the React/Tailwind frontend.
2. **Gateway Reception:** HTTP request is validated at the Javalin Gateway.
3. **Partition Assignment:** The Broker assigns the payload to an active Partition Leader.
4. **Storage Persistence:** The byte stream is appended sequentially to the active `.log` file on disk. Index offsets are updated.
5. **Consumption:** The UI polls the gateway, triggering the consumer to fetch data from the current offset checkpoint.

---

## 4. Testing & Validation

The system successfully passed comprehensive SDLC testing methodologies.
* **Unit Testing:** Modules (e.g., File-Channel storage engine, Hash Partitioner) functioned perfectly in isolation.
* **Data Consistency Test:** Sequential appending maintained 100% data integrity with zero corruption during high-throughput injection.
* **Fault Tolerance Validation:** Terminating Active Brokers triggered immediate ZooKeeper leader re-elections. The system self-healed and consumers resumed pulling data without manual intervention or data loss.

---

## 5. Conclusion & Future Scope

This project successfully proves the viability of constructing a highly efficient, distributed event streaming platform from fundamental principles. It drastically outperforms traditional database-driven chat systems in sheer I/O throughput. 

**Future Enhancements:**
* WebSockets for full-duplex bi-directional frontend streaming (replacing polling).
* Full containerization (Docker/Kubernetes) for genuine cloud-native microservice deployment.
* Advanced security mechanisms (JWT/TLS).
-->
