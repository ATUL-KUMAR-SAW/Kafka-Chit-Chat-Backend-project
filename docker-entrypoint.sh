#!/bin/bash
set -e

echo "=========================================="
echo " Kafka Chit-Chat — Cloud Deployment"
echo "=========================================="

# 1. Start ZooKeeper
echo "[1/3] Starting ZooKeeper..."
/opt/zookeeper/bin/zkServer.sh start
sleep 5

# Wait for ZooKeeper readiness (try ruok first, fallback to port check)
echo "Waiting for ZooKeeper..."
for i in $(seq 1 40); do
  # Try ruok command first
  if echo ruok | nc -w 2 localhost 2181 2>/dev/null | grep -q imok; then
    echo "ZooKeeper is ready! (ruok confirmed)"
    break
  fi
  # Fallback: just check if the port is accepting connections
  if nc -z -w 2 localhost 2181 2>/dev/null; then
    echo "ZooKeeper port 2181 is open, proceeding..."
    sleep 2
    break
  fi
  if [ "$i" -eq 40 ]; then
    echo "ZooKeeper failed to start! Showing logs:"
    cat /opt/zookeeper/logs/*.log 2>/dev/null || echo "No logs found"
    exit 1
  fi
  echo "  Waiting... ($i/40)"
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
