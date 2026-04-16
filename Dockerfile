# ============================================================
# Stage 1: Build Frontend
# ============================================================
FROM node:18-alpine AS frontend-build
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# ============================================================
# Stage 2: Build Java Backend
# ============================================================
FROM maven:3.8-openjdk-11 AS backend-build
WORKDIR /app
COPY pom.xml ./
RUN mvn dependency:go-offline -B
COPY src/ ./src/
RUN mvn clean package -DskipTests -q

# ============================================================
# Stage 3: Runtime
# ============================================================
FROM eclipse-temurin:11-jre-focal

WORKDIR /app

# Install required utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Download and install ZooKeeper
RUN wget -q https://archive.apache.org/dist/zookeeper/zookeeper-3.8.1/apache-zookeeper-3.8.1-bin.tar.gz && \
    tar -xzf apache-zookeeper-3.8.1-bin.tar.gz && \
    mv apache-zookeeper-3.8.1-bin /opt/zookeeper && \
    rm apache-zookeeper-3.8.1-bin.tar.gz

# Copy ZooKeeper configuration
COPY zoo.cfg /opt/zookeeper/conf/zoo.cfg

# Create data directories
RUN mkdir -p /tmp/zookeeper /app/data

# Copy built Java artifact
COPY --from=backend-build /app/target/build-your-own-kafka-1.0-SNAPSHOT.jar /app/app.jar

# Copy built frontend
COPY --from=frontend-build /app/frontend/dist /app/frontend-dist

# Copy startup script
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Render assigns PORT dynamically (usually 10000)
EXPOSE 10000

ENTRYPOINT ["/app/docker-entrypoint.sh"]
