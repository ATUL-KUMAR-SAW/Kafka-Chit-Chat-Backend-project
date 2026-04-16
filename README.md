# 🚀 Custom Distributed Messaging System (Kafka Clone)

Welcome to the **Custom Distributed Messaging System (Kafka Clone)**! This project is a fully functional, highly educational distributed stream-processing engine built from scratch in Java 11. 

Designed to mimic the core mechanics of Apache Kafka without relying on its engine, this project implements a 3-node broker cluster, custom TCP binary protocols, leader election, log segmentation, and an intuitive modern frontend to demonstrate real-time distributed messaging.

---

## ✨ Key Features

- **Custom Broker Cluster**: A robust 3-node distributed backend cluster programmed in raw Java.
- **ZooKeeper Coordination**: Employs Apache ZooKeeper (v3.8.4) for Controller node election, partition leadership, and cluster synchronization.
- **Append-Only Immutable Logs**: Implements traditional Kafka-style `.log` binary data storage with `.index` offsets.
- **Partition Replication**: A custom TCP-level binary protocol handles replicating message streams from partition Leaders to Followers flawlessly.
- **Tiered Cloud Storage Simulation**: Automatically transfers stale/old log chunks into simulated `.cloud_bucket` archival storage.
- **Modern Web Interface**: A blazing-fast Vanilla JS frontend powered by Vite and Tailwind CSS to visually demonstrate the messaging flow.
- **Javalin API Gateway**: A RESTful translation layer built with Javalin to safely bridge the HTTP frontend with the underlying custom TCP socket protocols.

---

## 🛠️ Technology Stack

- **Backend core**: Java 11, Java NIO (Non-blocking I/O Sockets)
- **Coordination**: Apache ZooKeeper 3.8.4
- **API Gateway**: Javalin (Port 8082)
- **Build System**: Maven (`pom.xml`)
- **Frontend**: Vite, Vanilla JavaScript, Tailwind CSS (Port 5173)

---

## 🚀 Getting Started

Follow these steps to launch the entire distributed architecture locally.

### 1. Prerequisites
- **Java 11** or higher.
- **Maven** installed and configured in your PATII.
- **Node.js** and **npm** for the frontend.
- **Apache ZooKeeper** installed locally on your machine.

### 2. Compile the Backend
Ensure the backend is cleanly packaged:
```bash
mvn clean package
```

### 3. Launch the Architecture Pipeline
You will need 4 separate terminal windows to run all independent subcomponents asynchronously:

**Terminal 1: ZooKeeper**
Navigate to your ZooKeeper installation and start the server.
```bash
cd apache-zookeeper-3.8.4-bin\bin
.\zkServer.cmd
```

**Terminal 2: The Broker Cluster**
Launch the 3-node broker cluster. The system uses a dedicated batch file to spool up all brokers on different ports.
```bash
# From the project root
.\start-cluster.bat
```

**Terminal 3: API Gateway**
Start the Javalin REST server which acts as the Kafka Producer/Consumer for the Web UI.
```bash
# From the project root
java -cp target\build-your-own-kafka-1.0-SNAPSHOT.jar com.simplekafka.api.ApiServer
```

**Terminal 4: Frontend Development Server**
Launch the Vite UI.
```bash
cd frontend
npm run dev
```

### 4. Open the Interface
Visit `http://localhost:5173` in your web browser. You can now simulate sending messages across the distributed backend!

---

## 🏗️ Architecture Design

1. **The Clients** send a JSON message into the Javalin REST server over HTTP.
2. **The API Server** takes the message, packages it using a custom `.putInt() / .putLong()` binary protocol, and pipes it into the **Elected Leader Broker's** open TCP Socket.
3. **The Leader Broker** writes the message exactly once to its immutable `data/brokerX/topic/` log file.
4. **Follower Brokers** are sent replica network packets over TCP. They identically parse the bytes and append the log locally.
5. The API Consumer continuously polls the partition, parsing the raw byte stream back into JSON, delivering it flawlessly back to the UI.

Enjoy exploring distributed systems!