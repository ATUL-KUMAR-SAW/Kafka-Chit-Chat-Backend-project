#!/bin/bash
set -e

echo "=========================================="
echo " Kafka Chit-Chat — Cloud Deployment"
echo "=========================================="

# 1. Start ZooKeeper
echo "[1/3] Starting ZooKeeper..."
/opt/zookeeper/bin/zkServer.sh start
sleep 3

# Wait for ZooKeeper readiness
echo "Waiting for ZooKeeper..."
for i in $(seq 1 30); do
  if echo ruok | nc -w 2 localhost 2181 2>/dev/null | grep -q imok; then
    echo "ZooKeeper is ready!"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ZooKeeper failed to start!"
    exit 1
  fi
  echo "  Waiting... ($i/30)"
  sleep 1
done

# 2. Start Broker 1
echo "[2/3] Starting Broker 1 on port 9091..."
java -Xmx96m -cp /app/app.jar com.simplekafka.broker.SimpleKafkaBroker 1 localhost 9091 2181 &
BROKER_PID=$!
sleep 5

echo "Broker 1 started (PID: $BROKER_PID)"

# 3. Start API Server (foreground — receives Render health checks)
API_PORT=${PORT:-10000}
echo "[3/3] Starting API Server on port $API_PORT..."
export PORT=$API_PORT
export BROKER_HOST=localhost
export BROKER_PORT=9091
export FRONTEND_DIR=/app/frontend-dist

exec java -Xmx128m -cp /app/app.jar com.simplekafka.api.ApiServer
