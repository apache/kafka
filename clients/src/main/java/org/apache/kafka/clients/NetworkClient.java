/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelState;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CorrelationIdMismatchException;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.PushTelemetryResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.telemetry.internals.ClientTelemetrySender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to implement the
 * user-facing producer and consumer clients.
 * <p>
 * This class is not thread-safe!
 */
public class NetworkClient implements KafkaClient {

    private enum State {
        ACTIVE,
        CLOSING,
        CLOSED
    }

    private final Logger log;

    /* the selector used to perform network i/o */
    private final Selectable selector;

    private final MetadataUpdater metadataUpdater;

    private final Random randOffset;

    /* the state of each node's connection */
    private final ClusterConnectionStates connectionStates;

    /* the set of requests currently being sent or awaiting a response */
    private final InFlightRequests inFlightRequests;

    /* the socket send buffer size in bytes */
    private final int socketSendBuffer;

    /* the socket receive size buffer in bytes */
    private final int socketReceiveBuffer;

    /* the client id used to identify this client in requests to the server */
    private final String clientId;

    /* the current correlation id to use when sending requests to servers */
    private int correlation;

    /* default timeout for individual requests to await acknowledgement from servers */
    private final int defaultRequestTimeoutMs;

    /* time in ms to wait before retrying to create connection to a server */
    private final long reconnectBackoffMs;

    private final MetadataRecoveryStrategy metadataRecoveryStrategy;

    private final Time time;

    /**
     * True if we should send an ApiVersionRequest when first connecting to a broker.
     */
    private final boolean discoverBrokerVersions;

    private final ApiVersions apiVersions;

    private final Map<String, ApiVersionsRequest.Builder> nodesNeedingApiVersionsFetch = new HashMap<>();

    private final List<ClientResponse> abortedSends = new LinkedList<>();

    private final Sensor throttleTimeSensor;

    private final AtomicReference<State> state;

    private final TelemetrySender telemetrySender;

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext,
                         MetadataRecoveryStrategy metadataRecoveryStrategy) {
        this(selector,
             metadata,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             null,
             logContext,
             metadataRecoveryStrategy);
    }

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         Sensor throttleTimeSensor,
                         LogContext logContext,
                         MetadataRecoveryStrategy metadataRecoveryStrategy) {
        this(null,
             metadata,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             throttleTimeSensor,
             logContext,
             new DefaultHostResolver(),
             null,
             metadataRecoveryStrategy);
    }

    public NetworkClient(Selectable selector,
                         MetadataUpdater metadataUpdater,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         LogContext logContext,
                         MetadataRecoveryStrategy metadataRecoveryStrategy) {
        this(metadataUpdater,
             null,
             selector,
             clientId,
             maxInFlightRequestsPerConnection,
             reconnectBackoffMs,
             reconnectBackoffMax,
             socketSendBuffer,
             socketReceiveBuffer,
             defaultRequestTimeoutMs,
             connectionSetupTimeoutMs,
             connectionSetupTimeoutMaxMs,
             time,
             discoverBrokerVersions,
             apiVersions,
             null,
             logContext,
             new DefaultHostResolver(),
             null,
             metadataRecoveryStrategy);
    }

    public NetworkClient(MetadataUpdater metadataUpdater,
                         Metadata metadata,
                         Selectable selector,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         long reconnectBackoffMax,
                         int socketSendBuffer,
                         int socketReceiveBuffer,
                         int defaultRequestTimeoutMs,
                         long connectionSetupTimeoutMs,
                         long connectionSetupTimeoutMaxMs,
                         Time time,
                         boolean discoverBrokerVersions,
                         ApiVersions apiVersions,
                         Sensor throttleTimeSensor,
                         LogContext logContext,
                         HostResolver hostResolver,
                         ClientTelemetrySender clientTelemetrySender,
                         MetadataRecoveryStrategy metadataRecoveryStrategy) {
        /* It would be better if we could pass `DefaultMetadataUpdater` from the public constructor, but it's not
         * possible because `DefaultMetadataUpdater` is an inner class and it can only be instantiated after the
         * super constructor is invoked.
         */
        if (metadataUpdater == null) {
            if (metadata == null)
                throw new IllegalArgumentException("`metadata` must not be null");
            this.metadataUpdater = new DefaultMetadataUpdater(metadata);
        } else {
            this.metadataUpdater = metadataUpdater;
        }
        this.selector = selector;
        this.clientId = clientId;
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
        this.connectionStates = new ClusterConnectionStates(
                reconnectBackoffMs, reconnectBackoffMax,
                connectionSetupTimeoutMs, connectionSetupTimeoutMaxMs, logContext, hostResolver);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.randOffset = new Random();
        this.defaultRequestTimeoutMs = defaultRequestTimeoutMs;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.time = time;
        this.discoverBrokerVersions = discoverBrokerVersions;
        this.apiVersions = apiVersions;
        this.throttleTimeSensor = throttleTimeSensor;
        this.log = logContext.logger(NetworkClient.class);
        this.state = new AtomicReference<>(State.ACTIVE);
        this.telemetrySender = (clientTelemetrySender != null) ? new TelemetrySender(clientTelemetrySender) : null;
        this.metadataRecoveryStrategy = metadataRecoveryStrategy;
    }

    /**
     * Begin connecting to the given node, return true if we are already connected and ready to send to that node.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return True if we are ready to send to the given node
     */
    @Override
    public boolean ready(Node node, long now) {
        if (node.isEmpty())
            throw new IllegalArgumentException("Cannot connect to empty node " + node);

        if (isReady(node, now))
            return true;

        if (connectionStates.canConnect(node.idString(), now))
            // if we are interested in sending to a node and we don't have a connection to it, initiate one
            initiateConnect(node, now);

        return false;
    }

    // Visible for testing
    boolean canConnect(Node node, long now) {
        return connectionStates.canConnect(node.idString(), now);
    }

    /**
     * Disconnects the connection to a particular node, if there is one.
     * Any pending ClientRequests for this connection will receive disconnections.
     *
     * @param nodeId The id of the node
     */
    @Override
    public void disconnect(String nodeId) {
        if (connectionStates.isDisconnected(nodeId)) {
            log.debug("Client requested disconnect from node {}, which is already disconnected", nodeId);
            return;
        }

        log.info("Client requested disconnect from node {}", nodeId);
        selector.close(nodeId);
        long now = time.milliseconds();
        cancelInFlightRequests(nodeId, now, abortedSends, false);
        connectionStates.disconnected(nodeId, now);
    }

    private void cancelInFlightRequests(String nodeId,
                                        long now,
                                        Collection<ClientResponse> responses,
                                        boolean timedOut) {
        Iterable<InFlightRequest> inFlightRequests = this.inFlightRequests.clearAll(nodeId);
        for (InFlightRequest request : inFlightRequests) {
            if (log.isDebugEnabled()) {
                log.debug("Cancelled in-flight {} request with correlation id {} due to node {} being disconnected " +
                        "(elapsed time since creation: {}ms, elapsed time since send: {}ms, throttle time: {}ms, request timeout: {}ms): {}",
                    request.header.apiKey(), request.header.correlationId(), nodeId,
                    request.timeElapsedSinceCreateMs(now), request.timeElapsedSinceSendMs(now),
                    request.throttleTimeMs(), request.requestTimeoutMs, request.request);
            } else {
                log.info("Cancelled in-flight {} request with correlation id {} due to node {} being disconnected " +
                        "(elapsed time since creation: {}ms, elapsed time since send: {}ms, throttle time: {}ms, request timeout: {}ms)",
                    request.header.apiKey(), request.header.correlationId(), nodeId,
                    request.timeElapsedSinceCreateMs(now), request.timeElapsedSinceSendMs(now),
                    request.throttleTimeMs(), request.requestTimeoutMs);
            }

            if (!request.isInternalRequest) {
                if (responses != null) {
                    ClientResponse clientResponse;

                    if (timedOut)
                        clientResponse = request.timedOut(now);
                    else
                        clientResponse = request.disconnected(now);

                    responses.add(clientResponse);
                }
            } else if (request.header.apiKey() == ApiKeys.METADATA) {
                metadataUpdater.handleFailedRequest(now, Optional.empty());
            } else if (isTelemetryApi(request.header.apiKey()) && telemetrySender != null) {
                telemetrySender.handleFailedRequest(request.header.apiKey(), null);
            }
        }
    }

    /**
     * Closes the connection to a particular node (if there is one).
     * All requests on the connection will be cleared.  ClientRequest callbacks will not be invoked
     * for the cleared requests, nor will they be returned from poll().
     *
     * @param nodeId The id of the node
     */
    @Override
    public void close(String nodeId) {
        log.info("Client requested connection close from node {}", nodeId);
        selector.close(nodeId);
        long now = time.milliseconds();
        cancelInFlightRequests(nodeId, now, null, false);
        connectionStates.remove(nodeId);
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     *
     * @param node The node to check
     * @param now The current timestamp
     * @return The number of milliseconds to wait.
     */
    @Override
    public long connectionDelay(Node node, long now) {
        return connectionStates.connectionDelay(node.idString(), now);
    }

    // Return the remaining throttling delay in milliseconds if throttling is in progress. Return 0, otherwise.
    // This is for testing.
    public long throttleDelayMs(Node node, long now) {
        return connectionStates.throttleDelayMs(node.idString(), now);
    }

    /**
     * Return the poll delay in milliseconds based on both connection and throttle delay.
     * @param node the connection to check
     * @param now the current time in ms
     */
    @Override
    public long pollDelayMs(Node node, long now) {
        return connectionStates.pollDelayMs(node.idString(), now);
    }

    /**
     * Check if the connection of the node has failed, based on the connection state. Such connection failure are
     * usually transient and can be resumed in the next {@link #ready(org.apache.kafka.common.Node, long)} }
     * call, but there are cases where transient failures needs to be caught and re-acted upon.
     *
     * @param node the node to check
     * @return true iff the connection has failed and the node is disconnected
     */
    @Override
    public boolean connectionFailed(Node node) {
        return connectionStates.isDisconnected(node.idString());
    }

    /**
     * Check if authentication to this node has failed, based on the connection state. Authentication failures are
     * propagated without any retries.
     *
     * @param node the node to check
     * @return an AuthenticationException iff authentication has failed, null otherwise
     */
    @Override
    public AuthenticationException authenticationException(Node node) {
        return connectionStates.authenticationException(node.idString());
    }

    /**
     * Check if the node with the given id is ready to send more requests.
     *
     * @param node The node
     * @param now The current time in ms
     * @return true if the node is ready
     */
    @Override
    public boolean isReady(Node node, long now) {
        // if we need to update our metadata now declare all requests unready to make metadata requests first
        // priority
        return !metadataUpdater.isUpdateDue(now) && canSendRequest(node.idString(), now);
    }

    /**
     * Are we connected and ready and able to send more requests to the given connection?
     *
     * @param node The node
     * @param now the current timestamp
     */
    private boolean canSendRequest(String node, long now) {
        return connectionStates.isReady(node, now) && selector.isChannelReady(node) &&
            inFlightRequests.canSendMore(node);
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     * @param request The request
     * @param now The current timestamp
     */
    @Override
    public void send(ClientRequest request, long now) {
        doSend(request, false, now);
    }

    // package-private for testing
    void sendInternalMetadataRequest(MetadataRequest.Builder builder, String nodeConnectionId, long now) {
        ClientRequest clientRequest = newClientRequest(nodeConnectionId, builder, now, true);
        doSend(clientRequest, true, now);
    }

    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now) {
        ensureActive();
        String nodeId = clientRequest.destination();
        if (!isInternalRequest) {
            // If this request came from outside the NetworkClient, validate
            // that we can send data.  If the request is internal, we trust
            // that internal code has done this validation.  Validation
            // will be slightly different for some internal requests (for
            // example, ApiVersionsRequests can be sent prior to being in
            // READY state.)
            if (!canSendRequest(nodeId, now))
                throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");
        }
        AbstractRequest.Builder<?> builder = clientRequest.requestBuilder();
        try {
            NodeApiVersions versionInfo = apiVersions.get(nodeId);
            short version;
            // Note: if versionInfo is null, we have no server version information. This would be
            // the case when sending the initial ApiVersionRequest which fetches the version
            // information itself.  It is also the case when discoverBrokerVersions is set to false.
            if (versionInfo == null) {
                version = builder.latestAllowedVersion();
                if (discoverBrokerVersions && log.isTraceEnabled())
                    log.trace("No version information found when sending {} with correlation id {} to node {}. " +
                            "Assuming version {}.", clientRequest.apiKey(), clientRequest.correlationId(), nodeId, version);
            } else {
                version = versionInfo.latestUsableVersion(clientRequest.apiKey(), builder.oldestAllowedVersion(),
                        builder.latestAllowedVersion());
            }
            // The call to build may also throw UnsupportedVersionException, if there are essential
            // fields that cannot be represented in the chosen version.
            doSend(clientRequest, isInternalRequest, now, builder.build(version));
        } catch (UnsupportedVersionException unsupportedVersionException) {
            // If the version is not supported, skip sending the request over the wire.
            // Instead, simply add it to the local queue of aborted requests.
            log.debug("Version mismatch when attempting to send {} with correlation id {} to {}", builder,
                    clientRequest.correlationId(), clientRequest.destination(), unsupportedVersionException);
            ClientResponse clientResponse = new ClientResponse(clientRequest.makeHeader(builder.latestAllowedVersion()),
                    clientRequest.callback(), clientRequest.destination(), now, now,
                    false, unsupportedVersionException, null, null);

            if (!isInternalRequest)
                abortedSends.add(clientResponse);
            else if (clientRequest.apiKey() == ApiKeys.METADATA)
                metadataUpdater.handleFailedRequest(now, Optional.of(unsupportedVersionException));
            else if (isTelemetryApi(clientRequest.apiKey()) && telemetrySender != null)
                telemetrySender.handleFailedRequest(clientRequest.apiKey(), unsupportedVersionException);
        }
    }

    private void doSend(ClientRequest clientRequest, boolean isInternalRequest, long now, AbstractRequest request) {
        String destination = clientRequest.destination();
        RequestHeader header = clientRequest.makeHeader(request.version());
        if (log.isDebugEnabled()) {
            log.debug("Sending {} request with header {} and timeout {} to node {}: {}",
                clientRequest.apiKey(), header, clientRequest.requestTimeoutMs(), destination, request);
        }
        Send send = request.toSend(header);
        InFlightRequest inFlightRequest = new InFlightRequest(
                clientRequest,
                header,
                isInternalRequest,
                request,
                send,
                now);
        this.inFlightRequests.add(inFlightRequest);
        selector.send(new NetworkSend(clientRequest.destination(), send));
    }

    /**
     * Do actual reads and writes to sockets.
     *
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately,
     *                must be non-negative. The actual timeout will be the minimum of timeout, request timeout and
     *                metadata timeout
     * @param now The current time in milliseconds
     * @return The list of responses received
     */
    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        ensureActive();

        if (!abortedSends.isEmpty()) {
            // If there are aborted sends because of unsupported version exceptions or disconnects,
            // handle them immediately without waiting for Selector#poll.
            List<ClientResponse> responses = new ArrayList<>();
            handleAbortedSends(responses);
            completeResponses(responses);
            return responses;
        }

        long metadataTimeout = metadataUpdater.maybeUpdate(now);
        long telemetryTimeout = telemetrySender != null ? telemetrySender.maybeUpdate(now) : Integer.MAX_VALUE;
        try {
            this.selector.poll(Utils.min(timeout, metadataTimeout, telemetryTimeout, defaultRequestTimeoutMs));
        } catch (IOException e) {
            log.error("Unexpected error during I/O", e);
        }

        // process completed actions
        long updatedNow = this.time.milliseconds();
        List<ClientResponse> responses = new ArrayList<>();
        handleCompletedSends(responses, updatedNow);
        handleCompletedReceives(responses, updatedNow);
        handleDisconnections(responses, updatedNow);
        handleConnections();
        handleInitiateApiVersionRequests(updatedNow);
        handleTimedOutConnections(responses, updatedNow);
        handleTimedOutRequests(responses, updatedNow);
        completeResponses(responses);

        return responses;
    }

    private void completeResponses(List<ClientResponse> responses) {
        for (ClientResponse response : responses) {
            try {
                response.onComplete();
            } catch (Exception e) {
                log.error("Uncaught error in request completion:", e);
            }
        }
    }

    /**
     * Get the number of in-flight requests
     */
    @Override
    public int inFlightRequestCount() {
        return this.inFlightRequests.count();
    }

    @Override
    public boolean hasInFlightRequests() {
        return !this.inFlightRequests.isEmpty();
    }

    /**
     * Get the number of in-flight requests for a given node
     */
    @Override
    public int inFlightRequestCount(String node) {
        return this.inFlightRequests.count(node);
    }

    @Override
    public boolean hasInFlightRequests(String node) {
        return !this.inFlightRequests.isEmpty(node);
    }

    @Override
    public boolean hasReadyNodes(long now) {
        return connectionStates.hasReadyNodes(now);
    }

    /**
     * Interrupt the client if it is blocked waiting on I/O.
     */
    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    @Override
    public void initiateClose() {
        if (state.compareAndSet(State.ACTIVE, State.CLOSING)) {
            wakeup();
        }
    }

    @Override
    public boolean active() {
        return state.get() == State.ACTIVE;
    }

    private void ensureActive() {
        if (!active())
            throw new DisconnectException("NetworkClient is no longer active, state is " + state);
    }

    /**
     * Close the network client
     */
    @Override
    public void close() {
        state.compareAndSet(State.ACTIVE, State.CLOSING);
        if (state.compareAndSet(State.CLOSING, State.CLOSED)) {
            this.selector.close();
            this.metadataUpdater.close();
            if (telemetrySender != null)
                telemetrySender.close();
        } else {
            log.warn("Attempting to close NetworkClient that has already been closed.");
        }
    }

    /**
     * Choose the node with the fewest outstanding requests which is at least eligible for connection. This method will
     * prefer a node with an existing connection, but will potentially choose a node for which we don't yet have a
     * connection if all existing connections are in use. If no connection exists, this method will prefer a node
     * with least recent connection attempts. This method will never choose a node for which there is no
     * existing connection and from which we have disconnected within the reconnect backoff period, or an active
     * connection which is being throttled.
     *
     * @return The node with the fewest in-flight requests.
     */
    @Override
    public LeastLoadedNode leastLoadedNode(long now) {
        List<Node> nodes = this.metadataUpdater.fetchNodes();
        if (nodes.isEmpty())
            throw new IllegalStateException("There are no nodes in the Kafka cluster");
        int inflight = Integer.MAX_VALUE;

        Node foundConnecting = null;
        Node foundCanConnect = null;
        Node foundReady = null;

        boolean atLeastOneConnectionReady = false;

        int offset = this.randOffset.nextInt(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            int idx = (offset + i) % nodes.size();
            Node node = nodes.get(idx);

            if (!atLeastOneConnectionReady
                    && connectionStates.isReady(node.idString(), now)
                    && selector.isChannelReady(node.idString())) {
                atLeastOneConnectionReady = true;
            }

            if (canSendRequest(node.idString(), now)) {
                int currInflight = this.inFlightRequests.count(node.idString());
                if (currInflight == 0) {
                    // if we find an established connection with no in-flight requests we can stop right away
                    log.trace("Found least loaded node {} connected with no in-flight requests", node);
                    return new LeastLoadedNode(node, true);
                } else if (currInflight < inflight) {
                    // otherwise if this is the best we have found so far, record that
                    inflight = currInflight;
                    foundReady = node;
                }
            } else if (connectionStates.isPreparingConnection(node.idString())) {
                foundConnecting = node;
            } else if (canConnect(node, now)) {
                if (foundCanConnect == null ||
                        this.connectionStates.lastConnectAttemptMs(foundCanConnect.idString()) >
                                this.connectionStates.lastConnectAttemptMs(node.idString())) {
                    foundCanConnect = node;
                }
            } else {
                log.trace("Removing node {} from least loaded node selection since it is neither ready " +
                        "for sending or connecting", node);
            }
        }

        // We prefer established connections if possible. Otherwise, we will wait for connections
        // which are being established before connecting to new nodes.
        if (foundReady != null) {
            log.trace("Found least loaded node {} with {} inflight requests", foundReady, inflight);
            return new LeastLoadedNode(foundReady, atLeastOneConnectionReady);
        } else if (foundConnecting != null) {
            log.trace("Found least loaded connecting node {}", foundConnecting);
            return new LeastLoadedNode(foundConnecting, atLeastOneConnectionReady);
        } else if (foundCanConnect != null) {
            log.trace("Found least loaded node {} with no active connection", foundCanConnect);
            return new LeastLoadedNode(foundCanConnect, atLeastOneConnectionReady);
        } else {
            log.trace("Least loaded node selection failed to find an available node");
            return new LeastLoadedNode(null, atLeastOneConnectionReady);
        }
    }

    public static AbstractResponse parseResponse(ByteBuffer responseBuffer, RequestHeader requestHeader) {
        try {
            return AbstractResponse.parseResponse(responseBuffer, requestHeader);
        } catch (BufferUnderflowException e) {
            throw new SchemaException("Buffer underflow while parsing response for request with header " + requestHeader, e);
        } catch (CorrelationIdMismatchException e) {
            if (SaslClientAuthenticator.isReserved(requestHeader.correlationId())
                && !SaslClientAuthenticator.isReserved(e.responseCorrelationId()))
                throw new SchemaException("The response is unrelated to Sasl request since its correlation id is "
                    + e.responseCorrelationId() + " and the reserved range for Sasl request is [ "
                    + SaslClientAuthenticator.MIN_RESERVED_CORRELATION_ID + ","
                    + SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + "]");
            else {
                throw e;
            }
        }
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     * @param disconnectState The state of the disconnected channel
     */
    private void processDisconnection(List<ClientResponse> responses,
                                      String nodeId,
                                      long now,
                                      ChannelState disconnectState) {
        processDisconnection(responses, nodeId, now, disconnectState, false);
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     */
    private void processTimeoutDisconnection(List<ClientResponse> responses, String nodeId, long now) {
        processDisconnection(responses, nodeId, now, ChannelState.LOCAL_CLOSE, true);
    }

    /**
     * Post process disconnection of a node
     *
     * @param responses The list of responses to update
     * @param nodeId Id of the node to be disconnected
     * @param now The current time
     * @param disconnectState The state of the disconnected channel
     * @param timedOut {@code true} if the connection is disconnected because of a timeout (request or connection)
     */
    private void processDisconnection(List<ClientResponse> responses,
                                      String nodeId,
                                      long now,
                                      ChannelState disconnectState,
                                      boolean timedOut) {
        connectionStates.disconnected(nodeId, now);
        apiVersions.remove(nodeId);
        nodesNeedingApiVersionsFetch.remove(nodeId);
        switch (disconnectState.state()) {
            case AUTHENTICATION_FAILED:
                AuthenticationException exception = disconnectState.exception();
                connectionStates.authenticationFailed(nodeId, now, exception);
                log.error("Connection to node {} ({}) failed authentication due to: {}", nodeId,
                    disconnectState.remoteAddress(), exception.getMessage());
                break;
            case AUTHENTICATE:
                log.warn("Connection to node {} ({}) terminated during authentication. This may happen " +
                    "due to any of the following reasons: (1) Firewall blocking Kafka TLS " +
                    "traffic (eg it may only allow HTTPS traffic), (2) Transient network issue.",
                    nodeId, disconnectState.remoteAddress());
                break;
            case NOT_CONNECTED:
                log.warn("Connection to node {} ({}) could not be established. Node may not be available.", nodeId, disconnectState.remoteAddress());
                break;
            default:
                break; // Disconnections in other states are logged at debug level in Selector
        }

        cancelInFlightRequests(nodeId, now, responses, timedOut);
        metadataUpdater.handleServerDisconnect(now, nodeId, Optional.ofNullable(disconnectState.exception()));
    }

    /**
     * Iterate over all the inflight requests and expire any requests that have exceeded the configured requestTimeout.
     * The connection to the node associated with the request will be terminated and will be treated as a disconnection.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleTimedOutRequests(List<ClientResponse> responses, long now) {
        List<String> nodeIds = this.inFlightRequests.nodesWithTimedOutRequests(now);
        for (String nodeId : nodeIds) {
            // close connection to the node
            this.selector.close(nodeId);
            log.info("Disconnecting from node {} due to request timeout.", nodeId);
            processTimeoutDisconnection(responses, nodeId, now);
        }
    }

    private void handleAbortedSends(List<ClientResponse> responses) {
        responses.addAll(abortedSends);
        abortedSends.clear();
    }

    /**
     * Handle socket channel connection timeout. The timeout will hit iff a connection
     * stays at the ConnectionState.CONNECTING state longer than the timeout value,
     * as indicated by ClusterConnectionStates.NodeConnectionState.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleTimedOutConnections(List<ClientResponse> responses, long now) {
        List<String> nodes = connectionStates.nodesWithConnectionSetupTimeout(now);
        for (String nodeId : nodes) {
            this.selector.close(nodeId);
            log.info(
                "Disconnecting from node {} due to socket connection setup timeout. " +
                "The timeout value is {} ms.",
                nodeId,
                connectionStates.connectionSetupTimeoutMs(nodeId));
            processTimeoutDisconnection(responses, nodeId, now);
        }
    }

    /**
     * Handle any completed request send. In particular if no response is expected consider the request complete.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // if no response is expected then when the send is completed, return it
        for (NetworkSend send : this.selector.completedSends()) {
            InFlightRequest request = this.inFlightRequests.lastSent(send.destinationId());
            if (!request.expectResponse) {
                this.inFlightRequests.completeLastSent(send.destinationId());
                responses.add(request.completed(null, now));
            }
        }
    }

    /**
     * If a response from a node includes a non-zero throttle delay and client-side throttling has been enabled for
     * the connection to the node, throttle the connection for the specified delay.
     *
     * @param response the response
     * @param apiVersion the API version of the response
     * @param nodeId the id of the node
     * @param now The current time
     */
    private void maybeThrottle(AbstractResponse response, short apiVersion, String nodeId, long now) {
        int throttleTimeMs = response.throttleTimeMs();
        if (throttleTimeMs > 0 && response.shouldClientThrottle(apiVersion)) {
            inFlightRequests.incrementThrottleTime(nodeId, throttleTimeMs);
            connectionStates.throttle(nodeId, now + throttleTimeMs);
            log.trace("Connection to node {} is throttled for {} ms until timestamp {}", nodeId, throttleTimeMs,
                      now + throttleTimeMs);
        }
    }

    /**
     * Handle any completed receives and update the response list with the responses received.
     *
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedReceives(List<ClientResponse> responses, long now) {
        for (NetworkReceive receive : this.selector.completedReceives()) {
            String source = receive.source();
            InFlightRequest req = inFlightRequests.completeNext(source);

            AbstractResponse response = parseResponse(receive.payload(), req.header);
            if (throttleTimeSensor != null)
                throttleTimeSensor.record(response.throttleTimeMs(), now);

            if (log.isDebugEnabled()) {
                log.debug("Received {} response from node {} for request with header {}: {}",
                    req.header.apiKey(), req.destination, req.header, response);
            }

            // If the received response includes a throttle delay, throttle the connection.
            maybeThrottle(response, req.header.apiVersion(), req.destination, now);
            if (req.isInternalRequest && response instanceof MetadataResponse)
                metadataUpdater.handleSuccessfulResponse(req.header, now, (MetadataResponse) response);
            else if (req.isInternalRequest && response instanceof ApiVersionsResponse)
                handleApiVersionsResponse(responses, req, now, (ApiVersionsResponse) response);
            else if (req.isInternalRequest && response instanceof GetTelemetrySubscriptionsResponse)
                telemetrySender.handleResponse((GetTelemetrySubscriptionsResponse) response);
            else if (req.isInternalRequest && response instanceof PushTelemetryResponse)
                telemetrySender.handleResponse((PushTelemetryResponse) response);
            else
                responses.add(req.completed(response, now));
        }
    }

    private void handleApiVersionsResponse(List<ClientResponse> responses,
                                           InFlightRequest req, long now, ApiVersionsResponse apiVersionsResponse) {
        final String node = req.destination;
        if (apiVersionsResponse.data().errorCode() != Errors.NONE.code()) {
            if (req.request.version() == 0 || apiVersionsResponse.data().errorCode() != Errors.UNSUPPORTED_VERSION.code()) {
                log.warn("Received error {} from node {} when making an ApiVersionsRequest with correlation id {}. Disconnecting.",
                        Errors.forCode(apiVersionsResponse.data().errorCode()), node, req.header.correlationId());
                this.selector.close(node);
                processDisconnection(responses, node, now, ChannelState.LOCAL_CLOSE);
            } else {
                // Starting from Apache Kafka 2.4, ApiKeys field is populated with the supported versions of
                // the ApiVersionsRequest when an UNSUPPORTED_VERSION error is returned.
                // If not provided, the client falls back to version 0.
                short maxApiVersion = 0;
                if (apiVersionsResponse.data().apiKeys().size() > 0) {
                    ApiVersion apiVersion = apiVersionsResponse.data().apiKeys().find(ApiKeys.API_VERSIONS.id);
                    if (apiVersion != null) {
                        maxApiVersion = apiVersion.maxVersion();
                    }
                }
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder(maxApiVersion));
            }
            return;
        }
        NodeApiVersions nodeVersionInfo = new NodeApiVersions(
            apiVersionsResponse.data().apiKeys(),
            apiVersionsResponse.data().supportedFeatures(),
            apiVersionsResponse.data().zkMigrationReady(),
            apiVersionsResponse.data().finalizedFeatures(),
            apiVersionsResponse.data().finalizedFeaturesEpoch());
        apiVersions.update(node, nodeVersionInfo);
        this.connectionStates.ready(node);
        log.debug("Node {} has finalized features epoch: {}, finalized features: {}, supported features: {}, ZK migration ready: {}, API versions: {}.",
                node, apiVersionsResponse.data().finalizedFeaturesEpoch(), apiVersionsResponse.data().finalizedFeatures(),
                apiVersionsResponse.data().supportedFeatures(), apiVersionsResponse.data().zkMigrationReady(), nodeVersionInfo);
    }

    /**
     * Handle any disconnected connections
     *
     * @param responses The list of responses that completed with the disconnection
     * @param now The current time
     */
    private void handleDisconnections(List<ClientResponse> responses, long now) {
        for (Map.Entry<String, ChannelState> entry : this.selector.disconnected().entrySet()) {
            String node = entry.getKey();
            ChannelState channelState = entry.getValue();
            if (channelState == ChannelState.EXPIRED) {
                log.debug("Idle connection to node {} disconnected.", node);
            } else {
                log.info("Node {} disconnected.", node);
            }
            processDisconnection(responses, node, now, channelState);
        }
    }

    /**
     * Record any newly completed connections
     */
    private void handleConnections() {
        for (String node : this.selector.connected()) {
            // We are now connected.  Note that we might not still be able to send requests. For instance,
            // if SSL is enabled, the SSL handshake happens after the connection is established.
            // Therefore, it is still necessary to check isChannelReady before attempting to send on this
            // connection.
            if (discoverBrokerVersions) {
                nodesNeedingApiVersionsFetch.put(node, new ApiVersionsRequest.Builder());
                log.debug("Completed connection to node {}. Fetching API versions.", node);
            } else {
                this.connectionStates.ready(node);
                log.debug("Completed connection to node {}. Ready.", node);
            }
        }
    }

    private void handleInitiateApiVersionRequests(long now) {
        Iterator<Map.Entry<String, ApiVersionsRequest.Builder>> iter = nodesNeedingApiVersionsFetch.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, ApiVersionsRequest.Builder> entry = iter.next();
            String node = entry.getKey();
            if (selector.isChannelReady(node) && inFlightRequests.canSendMore(node)) {
                log.debug("Initiating API versions fetch from node {}.", node);
                // We transition the connection to the CHECKING_API_VERSIONS state only when
                // the ApiVersionsRequest is queued up to be sent out. Without this, the client
                // could remain in the CHECKING_API_VERSIONS state forever if the channel does
                // not before ready.
                this.connectionStates.checkingApiVersions(node);
                ApiVersionsRequest.Builder apiVersionRequestBuilder = entry.getValue();
                ClientRequest clientRequest = newClientRequest(node, apiVersionRequestBuilder, now, true);
                doSend(clientRequest, true, now);
                iter.remove();
            }
        }
    }

    /**
     * Initiate a connection to the given node
     * @param node the node to connect to
     * @param now current time in epoch milliseconds
     */
    private void initiateConnect(Node node, long now) {
        String nodeConnectionId = node.idString();
        try {
            connectionStates.connecting(nodeConnectionId, now, node.host());
            InetAddress address = connectionStates.currentAddress(nodeConnectionId);
            log.debug("Initiating connection to node {} using address {}", node, address);
            selector.connect(nodeConnectionId,
                    new InetSocketAddress(address, node.port()),
                    this.socketSendBuffer,
                    this.socketReceiveBuffer);
        } catch (IOException e) {
            log.warn("Error connecting to node {}", node, e);
            // Attempt failed, we'll try again after the backoff
            connectionStates.disconnected(nodeConnectionId, now);
            // Notify metadata updater of the connection failure
            metadataUpdater.handleServerDisconnect(now, nodeConnectionId, Optional.empty());
        }
    }

    /**
     * Return true if there's at least one connection establishment is currently underway
     */
    private boolean isAnyNodeConnecting() {
        for (Node node : metadataUpdater.fetchNodes()) {
            if (connectionStates.isConnecting(node.idString())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return true if the ApiKey belongs to the Telemetry API.
     */
    private boolean isTelemetryApi(ApiKeys apiKey) {
        return apiKey == ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS || apiKey == ApiKeys.PUSH_TELEMETRY;
    }

    class DefaultMetadataUpdater implements MetadataUpdater {

        /* the current cluster metadata */
        private final Metadata metadata;

        // Defined if there is a request in progress, null otherwise
        private InProgressData inProgress;

        DefaultMetadataUpdater(Metadata metadata) {
            this.metadata = metadata;
            this.inProgress = null;
        }

        @Override
        public List<Node> fetchNodes() {
            return metadata.fetch().nodes();
        }

        @Override
        public boolean isUpdateDue(long now) {
            return !hasFetchInProgress() && this.metadata.timeToNextUpdate(now) == 0;
        }

        private boolean hasFetchInProgress() {
            return inProgress != null;
        }

        @Override
        public long maybeUpdate(long now) {
            // should we update our metadata?
            long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
            long waitForMetadataFetch = hasFetchInProgress() ? defaultRequestTimeoutMs : 0;

            long metadataTimeout = Math.max(timeToNextMetadataUpdate, waitForMetadataFetch);
            if (metadataTimeout > 0) {
                return metadataTimeout;
            }

            // Beware that the behavior of this method and the computation of timeouts for poll() are
            // highly dependent on the behavior of leastLoadedNode.
            LeastLoadedNode leastLoadedNode = leastLoadedNode(now);

            // Rebootstrap if needed and configured.
            if (metadataRecoveryStrategy == MetadataRecoveryStrategy.REBOOTSTRAP
                    && !leastLoadedNode.hasNodeAvailableOrConnectionReady()) {
                metadata.rebootstrap();

                leastLoadedNode = leastLoadedNode(now);
            }

            if (leastLoadedNode.node() == null) {
                log.debug("Give up sending metadata request since no node is available");
                return reconnectBackoffMs;
            }

            return maybeUpdate(now, leastLoadedNode.node());
        }

        @Override
        public void handleServerDisconnect(long now, String destinationId, Optional<AuthenticationException> maybeFatalException) {
            Cluster cluster = metadata.fetch();
            // 'processDisconnection' generates warnings for misconfigured bootstrap server configuration
            // resulting in 'Connection Refused' and misconfigured security resulting in authentication failures.
            // The warning below handles the case where a connection to a broker was established, but was disconnected
            // before metadata could be obtained.
            if (cluster.isBootstrapConfigured()) {
                int nodeId = Integer.parseInt(destinationId);
                Node node = cluster.nodeById(nodeId);
                if (node != null)
                    log.warn("Bootstrap broker {} disconnected", node);
            }

            // If we have a disconnect while an update is due, we treat it as a failed update
            // so that we can backoff properly
            if (isUpdateDue(now))
                handleFailedRequest(now, Optional.empty());

            maybeFatalException.ifPresent(metadata::fatalError);

            // The disconnect may be the result of stale metadata, so request an update
            metadata.requestUpdate(false);
        }

        @Override
        public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
            maybeFatalException.ifPresent(metadata::fatalError);
            metadata.failedUpdate(now);
            inProgress = null;
        }

        @Override
        public void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse response) {
            // If any partition has leader with missing listeners, log up to ten of these partitions
            // for diagnosing broker configuration issues.
            // This could be a transient issue if listeners were added dynamically to brokers.
            List<TopicPartition> missingListenerPartitions = response.topicMetadata().stream().flatMap(topicMetadata ->
                topicMetadata.partitionMetadata().stream()
                    .filter(partitionMetadata -> partitionMetadata.error == Errors.LISTENER_NOT_FOUND)
                    .map(partitionMetadata -> new TopicPartition(topicMetadata.topic(), partitionMetadata.partition())))
                .collect(Collectors.toList());
            if (!missingListenerPartitions.isEmpty()) {
                int count = missingListenerPartitions.size();
                log.warn("{} partitions have leader brokers without a matching listener, including {}",
                        count, missingListenerPartitions.subList(0, Math.min(10, count)));
            }

            // Check if any topic's metadata failed to get updated
            Map<String, Errors> errors = response.errors();
            if (!errors.isEmpty())
                log.warn("The metadata response from the cluster reported a recoverable issue with correlation id {} : {}", requestHeader.correlationId(), errors);

            // When talking to the startup phase of a broker, it is possible to receive an empty metadata set, which
            // we should retry later.
            if (response.brokers().isEmpty()) {
                log.trace("Ignoring empty metadata response with correlation id {}.", requestHeader.correlationId());
                this.metadata.failedUpdate(now);
            } else {
                this.metadata.update(inProgress.requestVersion, response, inProgress.isPartialUpdate, now);
            }

            inProgress = null;
        }

        @Override
        public void close() {
            this.metadata.close();
        }

        /**
         * Add a metadata request to the list of sends if we can make one
         */
        private long maybeUpdate(long now, Node node) {
            String nodeConnectionId = node.idString();

            if (canSendRequest(nodeConnectionId, now)) {
                Metadata.MetadataRequestAndVersion requestAndVersion = metadata.newMetadataRequestAndVersion(now);
                MetadataRequest.Builder metadataRequest = requestAndVersion.requestBuilder;
                log.debug("Sending metadata request {} to node {}", metadataRequest, node);
                sendInternalMetadataRequest(metadataRequest, nodeConnectionId, now);
                inProgress = new InProgressData(requestAndVersion.requestVersion, requestAndVersion.isPartialUpdate);
                return defaultRequestTimeoutMs;
            }

            // If there's any connection establishment underway, wait until it completes. This prevents
            // the client from unnecessarily connecting to additional nodes while a previous connection
            // attempt has not been completed.
            if (isAnyNodeConnecting()) {
                // Strictly the timeout we should return here is "connect timeout", but as we don't
                // have such application level configuration, using reconnect backoff instead.
                return reconnectBackoffMs;
            }

            if (connectionStates.canConnect(nodeConnectionId, now)) {
                // We don't have a connection to this node right now, make one
                log.debug("Initialize connection to node {} for sending metadata request", node);
                initiateConnect(node, now);
                return reconnectBackoffMs;
            }

            // connected, but can't send more OR connecting
            // In either case, we just need to wait for a network event to let us know the selected
            // connection might be usable again.
            return Long.MAX_VALUE;
        }

        public class InProgressData {
            public final int requestVersion;
            public final boolean isPartialUpdate;

            private InProgressData(int requestVersion, boolean isPartialUpdate) {
                this.requestVersion = requestVersion;
                this.isPartialUpdate = isPartialUpdate;
            }
        }

    }

    class TelemetrySender {

        private final ClientTelemetrySender clientTelemetrySender;
        private Node stickyNode;

        public TelemetrySender(ClientTelemetrySender clientTelemetrySender) {
            this.clientTelemetrySender = clientTelemetrySender;
        }

        public long maybeUpdate(long now) {
            long timeToNextUpdate = clientTelemetrySender.timeToNextUpdate(defaultRequestTimeoutMs);
            if (timeToNextUpdate > 0)
                return timeToNextUpdate;

            // Per KIP-714, let's continue to re-use the same broker for as long as possible.
            if (stickyNode == null) {
                stickyNode = leastLoadedNode(now).node();
                if (stickyNode == null) {
                    log.debug("Give up sending telemetry request since no node is available");
                    return reconnectBackoffMs;
                }
            }

            return maybeUpdate(now, stickyNode);
        }

        private long maybeUpdate(long now, Node node) {
            String nodeConnectionId = node.idString();

            if (canSendRequest(nodeConnectionId, now)) {
                Optional<AbstractRequest.Builder<?>> requestOpt = clientTelemetrySender.createRequest();

                if (!requestOpt.isPresent())
                    return Long.MAX_VALUE;

                AbstractRequest.Builder<?> request = requestOpt.get();
                ClientRequest clientRequest = newClientRequest(nodeConnectionId, request, now, true);
                doSend(clientRequest, true, now);
                return defaultRequestTimeoutMs;
            } else {
                // Per KIP-714, if we can't issue a request to this broker node, let's clear it out
                // and try another broker on the next loop.
                stickyNode = null;
            }

            // If there's any connection establishment underway, wait until it completes. This prevents
            // the client from unnecessarily connecting to additional nodes while a previous connection
            // attempt has not been completed.
            if (isAnyNodeConnecting())
                return reconnectBackoffMs;

            if (connectionStates.canConnect(nodeConnectionId, now)) {
                // We don't have a connection to this node right now, make one
                log.debug("Initialize connection to node {} for sending telemetry request", node);
                initiateConnect(node, now);
                return reconnectBackoffMs;
            }

            // In either case, we just need to wait for a network event to let us know the selected
            // connection might be usable again.
            return Long.MAX_VALUE;
        }

        public void handleResponse(GetTelemetrySubscriptionsResponse response) {
            clientTelemetrySender.handleResponse(response);
        }

        public void handleResponse(PushTelemetryResponse response) {
            clientTelemetrySender.handleResponse(response);
        }

        public void handleFailedRequest(ApiKeys apiKey, KafkaException maybeFatalException) {
            if (apiKey == ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS)
                clientTelemetrySender.handleFailedGetTelemetrySubscriptionsRequest(maybeFatalException);
            else if (apiKey == ApiKeys.PUSH_TELEMETRY)
                clientTelemetrySender.handleFailedPushTelemetryRequest(maybeFatalException);
            else
                throw new IllegalStateException("Invalid api key for failed telemetry request");
        }

        public void close() {
            try {
                clientTelemetrySender.close();
            } catch (Exception exception) {
                log.error("Failed to close client telemetry sender", exception);
            }
        }
    }

    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse) {
        return newClientRequest(nodeId, requestBuilder, createdTimeMs, expectResponse, defaultRequestTimeoutMs, null);
    }

    // visible for testing
    int nextCorrelationId() {
        if (SaslClientAuthenticator.isReserved(correlation)) {
            // the numeric overflow is fine as negative values is acceptable
            correlation = SaslClientAuthenticator.MAX_RESERVED_CORRELATION_ID + 1;
        }
        return correlation++;
    }

    // visible for testing
    Node telemetryConnectedNode() {
        return telemetrySender.stickyNode;
    }

    @Override
    public ClientRequest newClientRequest(String nodeId,
                                          AbstractRequest.Builder<?> requestBuilder,
                                          long createdTimeMs,
                                          boolean expectResponse,
                                          int requestTimeoutMs,
                                          RequestCompletionHandler callback) {
        return new ClientRequest(nodeId, requestBuilder, nextCorrelationId(), clientId, createdTimeMs, expectResponse,
                requestTimeoutMs, callback);
    }

    public boolean discoverBrokerVersions() {
        return discoverBrokerVersions;
    }

    static class InFlightRequest {
        final RequestHeader header;
        final String destination;
        final RequestCompletionHandler callback;
        final boolean expectResponse;
        final AbstractRequest request;
        final boolean isInternalRequest; // used to flag requests which are initiated internally by NetworkClient
        final Send send;
        final long sendTimeMs;
        final long createdTimeMs;
        final long requestTimeoutMs;
        long throttleTimeMs;

        public InFlightRequest(ClientRequest clientRequest,
                               RequestHeader header,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this(header,
                 clientRequest.requestTimeoutMs(),
                 clientRequest.createdTimeMs(),
                 clientRequest.destination(),
                 clientRequest.callback(),
                 clientRequest.expectResponse(),
                 isInternalRequest,
                 request,
                 send,
                 sendTimeMs);
        }

        public InFlightRequest(RequestHeader header,
                               int requestTimeoutMs,
                               long createdTimeMs,
                               String destination,
                               RequestCompletionHandler callback,
                               boolean expectResponse,
                               boolean isInternalRequest,
                               AbstractRequest request,
                               Send send,
                               long sendTimeMs) {
            this.header = header;
            this.requestTimeoutMs = requestTimeoutMs;
            this.createdTimeMs = createdTimeMs;
            this.destination = destination;
            this.callback = callback;
            this.expectResponse = expectResponse;
            this.isInternalRequest = isInternalRequest;
            this.request = request;
            this.send = send;
            this.sendTimeMs = sendTimeMs;
        }

        public long timeElapsedSinceSendMs(long currentTimeMs) {
            return Math.max(0, currentTimeMs - sendTimeMs);
        }

        public long throttleTimeMs() {
            return throttleTimeMs;
        }

        public long timeElapsedSinceCreateMs(long currentTimeMs) {
            return Math.max(0, currentTimeMs - createdTimeMs);
        }

        public ClientResponse completed(AbstractResponse response, long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    false, null, null, response);
        }

        public ClientResponse timedOut(long timeMs) {
            // A timed out request is considered disconnected as well
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    true, true, null, null, null);
        }

        public ClientResponse disconnected(long timeMs) {
            return new ClientResponse(header, callback, destination, createdTimeMs, timeMs,
                    true, null, null, null);
        }

        @Override
        public String toString() {
            return "InFlightRequest(header=" + header +
                    ", destination=" + destination +
                    ", expectResponse=" + expectResponse +
                    ", createdTimeMs=" + createdTimeMs +
                    ", sendTimeMs=" + sendTimeMs +
                    ", isInternalRequest=" + isInternalRequest +
                    ", request=" + request +
                    ", callback=" + callback +
                    ", send=" + send + ")";
        }

        public void incrementThrottleTime(long throttleTimeMs) {
            this.throttleTimeMs = throttleTimeMs + this.throttleTimeMs;
        }
    }

}
