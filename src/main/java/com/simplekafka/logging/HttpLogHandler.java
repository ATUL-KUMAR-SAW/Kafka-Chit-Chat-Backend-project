package com.simplekafka.logging;

import com.google.gson.Gson;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

public class HttpLogHandler extends Handler {
    private static final Gson GSON = new Gson();
    private final String componentName;
    private final String endpointUrl;
    
    // Single thread executor to send logs over HTTP without blocking the main broker processes
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public HttpLogHandler(String componentName, String endpointUrl) {
        this.componentName = componentName;
        this.endpointUrl = endpointUrl;
    }

    @Override
    public void publish(LogRecord record) {
        if (!isLoggable(record)) {
            return;
        }

        // Formats the message cleanly
        String msg = getFormatter() != null ? getFormatter().formatMessage(record) : record.getMessage();
        if (msg == null) msg = "";

        // Wrap the log into JSON
        LogPayload payload = new LogPayload(
            componentName,
            record.getLevel().getName(),
            msg,
            record.getMillis()
        );

        String json = GSON.toJson(payload);

        // Dispatches to the WebSocket server asynchronously
        executor.submit(() -> sendLog(json));
    }

    private void sendLog(String json) {
        try {
            URL url = new URL(endpointUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout(500); // Fail fast (500ms) if the API server is down
            conn.setReadTimeout(500);

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = json.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);           
            }

            // Trigger the request
            conn.getResponseCode();
            conn.disconnect();
        } catch (Exception e) {
            // Silently ignores if the dashboard backend isn't up. 
            // Logging an error here would cause an infinite recursive loop!
        }
    }

    @Override
    public void flush() {}

    @Override
    public void close() throws SecurityException {
        executor.shutdown();
    }

    // JSON POJO
    public static class LogPayload {
        public String component;
        public String level;
        public String message;
        public long timestamp;

        public LogPayload(String component, String level, String message, long timestamp) {
            this.component = component;
            this.level = level;
            this.message = message;
            this.timestamp = timestamp;
        }
    }
}
