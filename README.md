# Build Your Own Kafka 🚀

This is a complete implementation of a simplified Kafka-like messaging system built from scratch in Java. This project was developed as part of a final-year Computer Science Engineering project to dive deep into distributed system architectures, consensus algorithms, and message-broker paradigms!

## 🌟 Key Features

* **Custom Topic Partitioning:** Understand how messaging topics are partitioned and mapped to brokers.
* **Leader Election via ZooKeeper:** The broker nodes autonomously elect a Controller using Apache ZooKeeper to manage the cluster state.
* **Producer & Consumer API:** End-to-end communication interface using pure Java socket programming.
* **Fault-tolerant Storage layer:** Sequential log appending simulating real Kafka append-only logs.
* **Full-Stack Chat UI:** A modern Web Frontend built on Vite that transforms the backend messaging broker into a persistent graphical chat dashboard!
* **Advanced Resiliency Engine:** Built-in self-healing retry logic to survive broker disconnects seamlessly.
* **Log Retention Daemon:** Automated background thread that structurally trims old data to enforce enterprise disk storage limits.
* **Persistent Checkpointing:** Consumers interact directly with ZooKeeper (`/consumers`) paths to track their byte offsets and seamlessly recover message history upon crash/restart.
* **Semantic Routing Algorithm:** Predictable state distribution mapping sender keys computationally to individual hard-coded cluster partitions preventing chronologic tearing.

## 🛠️ Prerequisites

1. **Java 11+**: Ensure you have JDK 11 or higher installed on your machine (`java -version`).
2. **Apache Maven**: Required to compile the project (`mvn -version`).
3. **Node.js / npm**: Required for your Frontend UI (`node -v`).
4. **Apache ZooKeeper**: Essential to coordinate our distributed brokers.
   - Extract it into a folder (e.g., `C:\zookeeper`)
   - Inside `zookeeper/conf/`, rename `zoo_sample.cfg` to `zoo.cfg`.

## 📦 Setting Up the Project

1. Open your terminal in this repository folder (`EP`).
2. Build the Java cluster backend using Maven:
   ```bash
   mvn clean package
   ```
3. A `.jar` file will be generated in `target/build-your-own-kafka-1.0-SNAPSHOT.jar`.
4. Install the frontend Node modules:
   ```bash
   cd frontend
   npm install
   ```

## ⚙️ Running the Full Project Demo

To elegantly demonstrate this project, you will launch all structural components from bottom to top. 

### Step 1: Start ZooKeeper (The Database Coordinator)
Open a terminal and start your local ZooKeeper instance:
```cmd
cd C:\zookeeper\bin
zkServer.cmd
```

### Step 2: Start the 3 Kafka Brokers (The Storage Cluster)
Instead of opening three separate windows manually, you can simply run the automated batch script!
1. Double-click the file named **`start-cluster.bat`** located in the `EP` folder.
2. It will automatically pop open 3 windows representing Broker 1, 2, and 3. You will immediately see logs of Broker 1 becoming the Leader/Controller for the cluster!

### Step 3: Start the Backend API Server (The Middleman)
Now we start the Java Server (Javalin) that routes browser requests into our Kafka Cluster. Open a terminal in the `EP` folder:
```bash
java -cp target/build-your-own-kafka-1.0-SNAPSHOT.jar com.simplekafka.api.ApiServer
```
*(You should see `API Server running on http://localhost:8082`)*

### Step 4: Start the Frontend App (The GUI)
Open a final terminal inside the `EP/frontend` folder and start the website:
```bash
cd frontend
npm run dev
```

### Step 5: Test the End-to-End Flow!
1. Your terminal will give you a local URL (e.g., `http://localhost:5173`).
2. Open **two browser tabs** side-by-side using that URL.
3. In the top right corner of the GUI, set Tab A's name to "Alice" and Tab B's name to "Bob".
4. Type a message in Alice's tab and hit Send.
5. Watch the magic happen: The API instantly transforms Alice's text into Byte Packets, routes them through your custom Kafka Producer load-balancing over Partition 0, 1, or 2, stores the bytes permanently on disk, and streams the updates out via your Consumer API so it pops up simultaneously in Bob's tab.

## 📖 Learn More
Check out the `PROJECT_REPORT.md` provided in this directory for your Final Year presentation structure. It contains an in-depth breakdown of the distributed system architecture!

//set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot"
//set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.29.7-hotspot"
