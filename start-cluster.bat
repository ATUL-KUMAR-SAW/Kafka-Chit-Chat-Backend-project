@echo off
echo ==========================================
echo Starting Kafka Mock Cluster...
echo ==========================================

REM Ensure the project is built first
if not exist "target\build-your-own-kafka-1.0-SNAPSHOT.jar" (
    echo [ERROR] JAR file not found. Please run 'mvn clean package' first.
    pause
    exit /b
)

echo Starting ZooKeeper... (Assuming it's running via external zkServer.cmd)
echo Be sure you have ZooKeeper running at port 2181!
timeout /t 3

echo Starting Broker 1 (Port 9091)...
start "Broker 1" cmd /k "java -cp target\build-your-own-kafka-1.0-SNAPSHOT.jar com.simplekafka.broker.SimpleKafkaBroker 1 localhost 9091 2181"

timeout /t 2
echo Starting Broker 2 (Port 9092)...
start "Broker 2" cmd /k "java -cp target\build-your-own-kafka-1.0-SNAPSHOT.jar com.simplekafka.broker.SimpleKafkaBroker 2 localhost 9092 2181"

timeout /t 2
echo Starting Broker 3 (Port 9093)...
start "Broker 3" cmd /k "java -cp target\build-your-own-kafka-1.0-SNAPSHOT.jar com.simplekafka.broker.SimpleKafkaBroker 3 localhost 9093 2181"

echo ==========================================
echo Cluster Started Successfully!
echo You can now run the Producer and Consumer.
echo ==========================================
pause
