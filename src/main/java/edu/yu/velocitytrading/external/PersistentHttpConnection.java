package edu.yu.velocitytrading.external;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;

public class PersistentHttpConnection {

    private final HttpClient client;
    private final URI targetUri;
    private final Counter counter;

    public PersistentHttpConnection(URI targetUri) {
        // HttpClient manages the persistent TCP connection pool automatically
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.targetUri = targetUri;
        this.counter = new Counter();
    }

    /**
     * Sends a separate POST request for each order.
     * The HttpClient reuses the underlying TCP connection (Keep-Alive).
     */
    public void sendData(String jsonBody) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(targetUri)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();

        // Send asynchronously to avoid blocking the generator loop
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(response -> {
                    if (response.statusCode() >= 400) {
                        System.err.println("Order rejected: " + response.statusCode() + " " + response.body());
                    }
                    counter.increment(response.statusCode() < 400);
                })
                .exceptionally(ex -> {
                    System.err.println("Network error: " + ex.getMessage());
                    return null;
                });
    }

    /**
     * Shutdown the connection, and complete remaining requests
     */
    public void shutdown() {
        client.close();
    }

    /**
     * Get the ratio of successful requests
     * @return the ratio of successful requests
     */
    public double getSuccessRate() {
        return counter.getPercentage();
    }
}