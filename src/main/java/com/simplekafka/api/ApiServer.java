package com.simplekafka.api;

import com.google.gson.Gson;
import com.simplekafka.client.SimpleKafkaConsumer;
import com.simplekafka.client.SimpleKafkaProducer;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.staticfiles.Location;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REST API Server built with Javalin.
 * Acts as the middleman between the web frontend and the Kafka cluster.
 * In production, also serves the frontend static files.
 */
public class ApiServer {
    private static final Logger LOGGER = Logger.getLogger(ApiServer.class.getName());
    private static final String TOPIC = "chat-topic";
    private static final Gson GSON = new Gson();

    private final SimpleKafkaProducer producer;
    private final List<SimpleKafkaConsumer> consumers;
    
    // In-memory cache of all sorted chat messages fetched from Kafka
    private final List<MessageObj> chatHistory = Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {
        try {
            new ApiServer().start();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start API Server", e);
        }
    }

    public ApiServer() throws Exception {
        // Read broker connection from environment (defaults for local dev)
        String brokerHost = System.getenv("BROKER_HOST") != null ? System.getenv("BROKER_HOST") : "localhost";
        int brokerPort = System.getenv("BROKER_PORT") != null ? Integer.parseInt(System.getenv("BROKER_PORT")) : 9091;

        LOGGER.info("Connecting to broker at " + brokerHost + ":" + brokerPort);

        // Initialize producer to create the chat-topic if it doesn't exist
        this.producer = new SimpleKafkaProducer(brokerHost, brokerPort, TOPIC);
        this.producer.initialize(); 

        // Sleep briefly to ensure cluster replication finishes if it just created the topic
        Thread.sleep(1500);

        this.consumers = new ArrayList<>();
        
        // Read partition count to properly subscribe to all partitions
        int partitionCount = System.getenv("PARTITION_COUNT") != null ? Integer.parseInt(System.getenv("PARTITION_COUNT")) : 3;
        
        // Use a unique group ID for every reboot so the API Server always reads from offset 0 
        // and fully rebuilds its in-memory chat history cache, ignoring old Zookeeper commitments.
        String uniqueGroupId = "api-server-" + java.util.UUID.randomUUID().toString();
        
        for (int i = 0; i < partitionCount; i++) {
            SimpleKafkaConsumer c = new SimpleKafkaConsumer(brokerHost, brokerPort, TOPIC, i, 0L, uniqueGroupId);
            c.initialize();
            consumers.add(c);
        }
    }

    public void start() {
        // Read port from environment (Render assigns PORT dynamically)
        int port = System.getenv("PORT") != null ? Integer.parseInt(System.getenv("PORT")) : 8082;

        // Start lightweight HTTP server with CORS allowed
        Javalin app = Javalin.create(config -> {
            config.plugins.enableCors(cors -> cors.add(it -> it.anyHost()));

            // Serve frontend static files if the directory exists (production mode)
            String frontendDir = System.getenv("FRONTEND_DIR") != null ? System.getenv("FRONTEND_DIR") : "frontend-dist";
            File dir = new File(frontendDir);
            if (dir.exists() && dir.isDirectory()) {
                config.staticFiles.add(frontendDir, Location.EXTERNAL);
                LOGGER.info("Serving frontend from: " + dir.getAbsolutePath());
            } else {
                LOGGER.info("No frontend directory found at '" + frontendDir + "' — API-only mode (use Vite dev server for frontend)");
            }
        }).start(port);

        app.get("/api/messages", this::getMessages);
        app.post("/api/messages", this::sendMessage);

        LOGGER.info("API Server running on http://localhost:" + port);

        // Start reading messages seamlessly across dynamically assigned partitions
        for (SimpleKafkaConsumer c : consumers) {
            c.startConsuming((message, partition, offset) -> {
                try {
                    String json = new String(message, StandardCharsets.UTF_8);
                    MessageObj obj = GSON.fromJson(json, MessageObj.class);
                    synchronized (chatHistory) {
                        chatHistory.add(obj);
                        // Sort by timestamp so multi-partition messages render linearly
                        chatHistory.sort((a, b) -> Long.compare(a.timestamp, b.timestamp));
                    }
                } catch (Exception e) {
                    LOGGER.warning("Failed to parse message from Kafka: " + e.getMessage());
                }
            });
        }
    }

    private void getMessages(Context ctx) {
        // Automatically returns the cached messages as JSON using our explicit Gson serializer
        String responseJson;
        synchronized (chatHistory) {
            responseJson = GSON.toJson(chatHistory);
        }
        ctx.contentType("application/json").result(responseJson);
    }

    private void sendMessage(Context ctx) {
        try {
            MessageObj payload = GSON.fromJson(ctx.body(), MessageObj.class);
            if (payload.timestamp == 0) {
                payload.timestamp = System.currentTimeMillis();
            }
            
            String json = GSON.toJson(payload);
            
            // Consistently distributes the message to a deterministic partition based on the sender
            producer.send(json, payload.sender); 
            ctx.status(200).result("OK");
        } catch (Exception e) {
            LOGGER.severe("Failed to send message: " + e.getMessage());
            ctx.status(500).result(e.getMessage());
        }
    }

    // Standard POJO representing our chat JSON object
    public static class MessageObj {
        public String sender;
        public String text;
        public long timestamp;
    }
}
