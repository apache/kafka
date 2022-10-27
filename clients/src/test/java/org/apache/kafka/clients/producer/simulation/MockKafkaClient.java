package org.apache.kafka.clients.producer.simulation;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.MockTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * This mocked version of `KafkaClient` is used to ensure delivery of requests
 * from the producer to the cluster model, which is responsible for handling
 * them.
 */
class MockKafkaClient implements KafkaClient {
    private final ClusterModel cluster;
    private final List<Node> brokers;
    private final ProducerMetadata metadata;
    private final MockTime time;
    private final Random random;

    private final int maxInflights;
    private final int requestTimeoutMs;

    private final Map<String, ConnectionModel> activeConnections = new HashMap<>();
    private final List<ClientResponse> disconnectResponses = new ArrayList<>();
    private boolean isClosed = false;
    private int correlationId = 0;
    private long nextConnectionId = 0;

    public MockKafkaClient(
        ClusterModel cluster,
        List<Node> brokers,
        ProducerMetadata metadata,
        MockTime time,
        Random random,
        int maxInflights,
        int requestTimeoutMs
    ) {
        this.cluster = cluster;
        this.brokers = brokers;
        this.metadata = metadata;
        this.time = time;
        this.random = random;
        this.maxInflights = maxInflights;
        this.requestTimeoutMs = requestTimeoutMs;
    }

    @Override
    public boolean isReady(Node node, long now) {
        ConnectionModel connection = activeConnections.get(node.idString());
        return connection != null && connection.isReady();
    }

    @Override
    public boolean ready(Node node, long now) {
        if (!disconnectResponses.isEmpty()) {
            // Don't allow reconnect until we have delivered notification about
            // disconnects to the client.
            return false;
        }

        ConnectionModel connection = activeConnections.computeIfAbsent(node.idString(), k -> {
            long connectionId = nextConnectionId++;
            ConnectionModel newConnection = new ConnectionModel(
                connectionId,
                time,
                random,
                cluster,
                maxInflights
            );
            cluster.broker(node.id()).connect(newConnection);
            return newConnection;
        });
        return connection.isReady();
    }

    @Override
    public long connectionDelay(Node node, long now) {
        return 0;
    }

    @Override
    public long pollDelayMs(Node node, long now) {
        return 0;
    }

    @Override
    public boolean connectionFailed(Node node) {
        return false;
    }

    @Override
    public AuthenticationException authenticationException(Node node) {
        return null;
    }

    @Override
    public void send(ClientRequest request, long now) {
        String nodeId = request.destination();
        ConnectionModel connection = activeConnections.get(nodeId);
        if (connection == null || !connection.isReady()) {
            throw new IllegalStateException("Connection for nodeId " + nodeId + " is not ready");
        }

        connection.clientSend(request);

    }

    void randomDisconnect() {
        if (!activeConnections.isEmpty()) {
            String nodeId = SimulationUtils.randomEntry(random, activeConnections).getKey();
            disconnect(nodeId, true);
            cluster.events.add(new SimulationEvent() {
                @Override
                public String toString() {
                    return "NetworkDisconnect()";
                }
            });
        }
    }

    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        if (metadata.updateRequested()) {
            int updateVersion = metadata.updateVersion();
            metadata.update(updateVersion, cluster.handleMetadataRequest(
                metadata.newMetadataRequestBuilder().build()
            ), false, time.milliseconds());
        }

        List<ClientResponse> responses = new ArrayList<>();
        for (ConnectionModel connection : activeConnections.values()) {
            ClientResponse clientResponse = connection.clientReceive();
            if (clientResponse != null) {
                responses.add(clientResponse);
            }
        }

        responses.addAll(this.disconnectResponses);
        this.disconnectResponses.clear();
        responses.forEach(ClientResponse::onComplete);
        return responses;
    }

    @Override
    public void disconnect(String nodeId) {
        disconnect(nodeId, true);
    }

    @Override
    public void close(String nodeId) {
        disconnect(nodeId, false);
    }

    private void disconnect(String nodeId, boolean returnResponses) {
        ConnectionModel connection = activeConnections.remove(nodeId);
        if (connection != null) {
            List<ClientResponse> disconnectResponses = connection.disconnect();
            if (returnResponses) {
                this.disconnectResponses.addAll(disconnectResponses);
            }
            int brokerId = Integer.parseInt(nodeId);
            cluster.broker(brokerId).disconnect(connection);
        }
    }

    @Override
    public Node leastLoadedNode(long now) {
        int randomIdx = random.nextInt(brokers.size());
        return brokers.get(randomIdx);
    }

    @Override
    public int inFlightRequestCount() {
        int totalInflights = 0;
        for (ConnectionModel connection : activeConnections.values()) {
            totalInflights += connection.numInflights();
        }
        return totalInflights;
    }

    @Override
    public boolean hasInFlightRequests() {
        return inFlightRequestCount() > 0;
    }

    @Override
    public int inFlightRequestCount(String nodeId) {
        ConnectionModel connection = activeConnections.get(nodeId);
        if (connection == null) {
            return 0;
        } else {
            return connection.numInflights();
        }
    }

    @Override
    public boolean hasInFlightRequests(String nodeId) {
        return inFlightRequestCount(nodeId) > 0;
    }

    @Override
    public boolean hasReadyNodes(long now) {
        for (ConnectionModel connection : activeConnections.values()) {
            if (connection.isReady()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void wakeup() {

    }

    @Override
    public ClientRequest newClientRequest(
        String nodeId,
        AbstractRequest.Builder<?> requestBuilder,
        long createdTimeMs,
        boolean expectResponse
    ) {
        return newClientRequest(
            nodeId,
            requestBuilder,
            createdTimeMs,
            expectResponse,
            requestTimeoutMs,
            null
        );
    }

    @Override
    public ClientRequest newClientRequest(
        String nodeId,
        AbstractRequest.Builder<?> requestBuilder,
        long createdTimeMs,
        boolean expectResponse,
        int requestTimeoutMs,
        RequestCompletionHandler callback
    ) {
        return new ClientRequest(
            nodeId,
            requestBuilder,
            correlationId++,
            "",
            createdTimeMs,
            expectResponse,
            requestTimeoutMs,
            callback
        );
    }

    @Override
    public void initiateClose() {
        isClosed = true;
    }

    @Override
    public boolean active() {
        return !isClosed;
    }

    @Override
    public void close() throws IOException {
        initiateClose();
    }
}
