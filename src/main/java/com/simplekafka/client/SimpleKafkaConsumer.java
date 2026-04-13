package com.simplekafka.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.simplekafka.broker.ZookeeperClient;

/**
 * Dynamic Example consumer for Build Your Own Kafka
 */
public class SimpleKafkaConsumer {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaConsumer.class.getName());
    private static final int MAX_BYTES = 1024 * 1024; // 1MB max fetch size
    private static final int POLL_INTERVAL_MS = 100;
    
    private final SimpleKafkaClient client;
    private final String topic;
    private final String groupId;
    private final String consumerId;
    private final AtomicBoolean running;
    private Thread consumerThread;
    private ZookeeperClient zkClient;
    
    // Concurrency maps for assigned partitions and offsets
    private final List<Integer> assignedPartitions = new ArrayList<>();
    private final Map<Integer, Long> currentOffsets = new ConcurrentHashMap<>();
    
    /**
     * Create a SimpleKafka consumer (Dynamic Rebalancing enabled)
     */
    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, String groupId) {
        this.client = new SimpleKafkaClient(bootstrapBroker, bootstrapPort);
        this.topic = topic;
        this.groupId = groupId;
        this.consumerId = UUID.randomUUID().toString();
        this.running = new AtomicBoolean(false);
        this.zkClient = new ZookeeperClient("localhost", 2181);
    }
    
    /**
     * Legacy constructor fallback locking to a single partition
     */
    public SimpleKafkaConsumer(String bootstrapBroker, int bootstrapPort, String topic, int partition, long startOffset, String groupId) {
        this(bootstrapBroker, bootstrapPort, topic, groupId);
        this.assignedPartitions.add(partition);
        this.currentOffsets.put(partition, startOffset);
    }
    
    /**
     * Initialize the consumer
     */
    public void initialize() throws IOException {
        client.initialize();
        
        // Check if topic exists
        if (client.getTopicMetadata(topic) == null) {
            throw new IOException("Topic does not exist: " + topic);
        }
        
        if (zkClient != null) {
            try {
                zkClient.connect();
                
                // Ensure base directories exist
                zkClient.createPath("/consumers/" + groupId + "/ids");
                zkClient.createPath("/consumers/" + groupId + "/offsets/" + topic);
                
                // Register consumer dynamically
                String myPath = "/consumers/" + groupId + "/ids/" + consumerId;
                zkClient.createEphemeralNode(myPath, topic);
                
                LOGGER.info("Registered consumer node: " + consumerId);
                
                if (assignedPartitions.isEmpty()) {
                    setupRebalanceWatcher();
                } else {
                    // Legacy mode
                    loadOffsetsForAssignments();
                }
            } catch (Exception e) {
                LOGGER.warning("Failed ZK initialization: " + e.getMessage());
            }
        }
    }
    
    private void setupRebalanceWatcher() {
        zkClient.watchChildren("/consumers/" + groupId + "/ids", children -> {
            try {
                rebalance(children);
            } catch (Exception e) {
                LOGGER.severe("Rebalance failed: " + e.getMessage());
            }
        });
    }
    
    private synchronized void rebalance(List<String> activeConsumers) throws Exception {
        LOGGER.info("Triggering group rebalance. Active consumers: " + activeConsumers);
        
        // Filter out consumers not subscribed to this topic
        List<String> topicConsumers = new ArrayList<>();
        for (String cId : activeConsumers) {
            String cTopic = zkClient.getData("/consumers/" + groupId + "/ids/" + cId);
            if (topic.equals(cTopic)) {
                topicConsumers.add(cId);
            }
        }
        
        Collections.sort(topicConsumers);
        
        SimpleKafkaClient.TopicMetadata metadata = client.getTopicMetadata(topic);
        if (metadata == null) return;
        
        List<Integer> allPartitions = new ArrayList<>();
        for (SimpleKafkaClient.PartitionInfo pi : metadata.getPartitions()) {
            allPartitions.add(pi.getId());
        }
        Collections.sort(allPartitions);
        
        int numConsumers = topicConsumers.size();
        if (numConsumers == 0) return;
        
        int myIndex = topicConsumers.indexOf(consumerId);
        if (myIndex == -1) return;
        
        int partsPerConsumer = allPartitions.size() / numConsumers;
        int remainder = allPartitions.size() % numConsumers;
        
        int start = partsPerConsumer * myIndex + Math.min(myIndex, remainder);
        int length = partsPerConsumer + (myIndex < remainder ? 1 : 0);
        
        assignedPartitions.clear();
        for (int i = start; i < start + length; i++) {
            assignedPartitions.add(allPartitions.get(i));
        }
        
        LOGGER.info("Rebalance complete. I am assigned partitions: " + assignedPartitions);
        loadOffsetsForAssignments();
    }
    
    private void loadOffsetsForAssignments() throws Exception {
        for (int p : assignedPartitions) {
            String path = "/consumers/" + groupId + "/offsets/" + topic + "/" + p;
            if (zkClient.exists(path)) {
                String offsetStr = zkClient.getData(path);
                if (offsetStr != null && !offsetStr.isEmpty()) {
                    currentOffsets.put(p, Long.parseLong(offsetStr));
                    LOGGER.info("Loaded partition " + p + " from Zookeeper offset: " + offsetStr);
                }
            } else {
                currentOffsets.put(p, 0L);
                zkClient.createPersistentNode(path, "0");
            }
        }
    }
    
    private void commitOffset(int partition, long offset) {
        if (zkClient != null) {
            try {
                String path = "/consumers/" + groupId + "/offsets/" + topic + "/" + partition;
                if (zkClient.exists(path)) {
                    zkClient.setData(path, String.valueOf(offset));
                } else {
                    zkClient.createPersistentNode(path, String.valueOf(offset));
                }
            } catch (Exception e) {
                LOGGER.warning("Failed to commit offset to ZK: " + e.getMessage());
            }
        }
    }
    
    /**
     * Start consuming messages in a loop
     */
    public void startConsuming(MessageHandler handler) {
        if (running.compareAndSet(false, true)) {
            consumerThread = new Thread(() -> {
                try {
                    while (running.get()) {
                        boolean polledAny = false;
                        
                        // Copy assignment list to avoid concurrency modification loops
                        List<Integer> currentAssignments;
                        synchronized (this) {
                            currentAssignments = new ArrayList<>(assignedPartitions);
                        }
                        
                        for (int p : currentAssignments) {
                            long offset = currentOffsets.getOrDefault(p, 0L);
                            List<byte[]> messages = client.fetch(topic, p, offset, MAX_BYTES);
                            
                            if (!messages.isEmpty()) {
                                polledAny = true;
                                for (byte[] message : messages) {
                                    handler.handle(message, p, offset);
                                    offset++;
                                }
                                currentOffsets.put(p, offset);
                                commitOffset(p, offset);
                            }
                        }
                        
                        // If no messages, wait a bit before polling again
                        if (!polledAny) {
                            Thread.sleep(POLL_INTERVAL_MS);
                        }
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        LOGGER.log(Level.SEVERE, "Error in consumer loop", e);
                    }
                    running.set(false);
                }
            });
            
            consumerThread.setDaemon(true);
            consumerThread.start();
            LOGGER.info("Started consuming loop for consumer: " + consumerId);
        }
    }
    
    /**
     * Stop consuming messages
     */
    public void stopConsuming() {
        if (running.compareAndSet(true, false)) {
            if (consumerThread != null) {
                try {
                    consumerThread.interrupt();
                    consumerThread.join(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (zkClient != null) {
                try {
                    zkClient.close();
                } catch (InterruptedException e) {}
            }
            LOGGER.info("Stopped consumer: " + consumerId);
        }
    }
    
    /**
     * Close the consumer
     */
    public void close() {
        stopConsuming();
    }
    
    /**
     * Interface for handling consumed messages
     */
    public interface MessageHandler {
        void handle(byte[] message, int partition, long offset);
    }
    
    /**
     * Main method for demonstration
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: SimpleKafkaConsumer <broker> <port> <topic> [groupId]");
            System.exit(1);
        }
        
        String broker = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];
        String groupId = args.length > 3 ? args[3] : "demo-console-group";
        
        try {
            SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(broker, port, topic, groupId);
            consumer.initialize();
            
            System.out.println("Consumer " + consumer.consumerId + " initialized. Starting consumption...");
            
            // Consume messages and print them
            consumer.startConsuming((message, partition, offset) -> {
                String messageStr = new String(message, StandardCharsets.UTF_8);
                System.out.println("[Partition " + partition + " | Offset " + offset + "]: " + messageStr);
            });
            
            System.out.println("Consumer started. Press enter to stop.");
            System.in.read();
            
            consumer.close();
            System.out.println("Consumer stopped");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Consumer error", e);
        }
    }
}