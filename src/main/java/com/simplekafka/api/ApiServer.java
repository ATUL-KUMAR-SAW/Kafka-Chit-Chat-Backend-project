package com.simplekafka.api;

import com.google.gson.Gson;
import com.simplekafka.client.SimpleKafkaConsumer;
import com.simplekafka.client.SimpleKafkaProducer;
import io.javalin.Javalin;
import io.javalin.http.Context;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REST API Server built with Javalin.
 * Acts as the middleman between the web frontend and the Kafka cluster.
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
        String brokerHost = "localhost";
        int brokerPort = 9091;

        // Initialize producer to create the chat-topic if it doesn't exist
        this.producer = new SimpleKafkaProducer(brokerHost, brokerPort, TOPIC);
        this.producer.initialize(); 

        // Sleep briefly to ensure cluster replication finishes if it just created the topic
        Thread.sleep(1500);

        this.consumers = new ArrayList<>();
        // Instead of hardcoding 3 consumers, we run 1 Dynamic Consumer
        // The Rebalancing engine will automatically detect and assign all available partitions!
        SimpleKafkaConsumer c = new SimpleKafkaConsumer(brokerHost, brokerPort, TOPIC, "api-server-group-v2");
        c.initialize();
        consumers.add(c);
    }

    public void start() {
        // Start lightweight HTTP server on port 8082 with CORS allowed
        Javalin app = Javalin.create(config -> {
            config.plugins.enableCors(cors -> cors.add(it -> it.anyHost()));
        }).start(8082);

        app.get("/api/messages", this::getMessages);
        app.post("/api/messages", this::sendMessage);

        LOGGER.info("API Server running on http://localhost:8082");

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
