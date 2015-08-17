/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Selectable;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A network client for asynchronous request/response network i/o. This is an internal class used to implement the
 * user-facing producer and consumer clients.
 * <p>
 * This class is not thread-safe!
 */
public class NetworkClient implements KafkaClient {

    private static final Logger log = LoggerFactory.getLogger(NetworkClient.class);

    /* the selector used to perform network i/o */
    private final Selectable selector;

    /* the current cluster metadata */
    private final Metadata metadata;

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

    /* a random offset to use when choosing nodes to avoid having all nodes choose the same node */
    private final int nodeIndexOffset;

    /* the current correlation id to use when sending requests to servers */
    private int correlation;

    /* true iff there is a metadata request that has been sent and for which we have not yet received a response */
    private boolean metadataFetchInProgress;

    /* the last timestamp when no broker node is available to connect */
    private long lastNoNodeAvailableMs;

    public NetworkClient(Selectable selector,
                         Metadata metadata,
                         String clientId,
                         int maxInFlightRequestsPerConnection,
                         long reconnectBackoffMs,
                         int socketSendBuffer,
                         int socketReceiveBuffer) {
        this.selector = selector;
        this.metadata = metadata;
        this.clientId = clientId;
        this.inFlightRequests = new InFlightRequests(maxInFlightRequestsPerConnection);
        this.connectionStates = new ClusterConnectionStates(reconnectBackoffMs);
        this.socketSendBuffer = socketSendBuffer;
        this.socketReceiveBuffer = socketReceiveBuffer;
        this.correlation = 0;
        this.nodeIndexOffset = new Random().nextInt(Integer.MAX_VALUE);
        this.metadataFetchInProgress = false;
        this.lastNoNodeAvailableMs = 0;
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
        if (isReady(node, now))
            return true;

        if (connectionStates.canConnect(node.idString(), now))
            // if we are interested in sending to a node and we don't have a connection to it, initiate one
            initiateConnect(node, now);

        return false;
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
        return connectionStates.connectionState(node.idString()).equals(ConnectionState.DISCONNECTED);
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
        String nodeId = node.idString();
        if (!this.metadataFetchInProgress && this.metadata.timeToNextUpdate(now) == 0)
            // if we need to update our metadata now declare all requests unready to make metadata requests first
            // priority
            return false;
        else
            // otherwise we are ready if we are connected and can send more requests
            return isSendable(nodeId);
    }

    /**
     * Are we connected and ready and able to send more requests to the given connection?
     * 
     * @param node The node
     */
    private boolean isSendable(String node) {
        return connectionStates.isConnected(node) && inFlightRequests.canSendMore(node);
    }

    /**
     * Return the state of the connection to the given node
     * 
     * @param node The node to check
     * @return The connection state
     */
    public ConnectionState connectionState(String node) {
        return connectionStates.connectionState(node);
    }

    /**
     * Queue up the given request for sending. Requests can only be sent out to ready nodes.
     * 
     * @param request The request
     */
    @Override
    public void send(ClientRequest request) {
        String nodeId = request.request().destination();
        if (!isSendable(nodeId))
            throw new IllegalStateException("Attempt to send a request to node " + nodeId + " which is not ready.");

        this.inFlightRequests.add(request);
        selector.send(request.request());
    }

    /**
     * Do actual reads and writes to sockets.
     * 
     * @param timeout The maximum amount of time to wait (in ms) for responses if there are none immediately
     * @param now The current time in milliseconds
     * @return The list of responses received
     */
    @Override
    public List<ClientResponse> poll(long timeout, long now) {
        // should we update our metadata?
        long timeToNextMetadataUpdate = metadata.timeToNextUpdate(now);
        long timeToNextReconnectAttempt = Math.max(this.lastNoNodeAvailableMs + metadata.refreshBackoff() - now, 0);
        long waitForMetadataFetch = this.metadataFetchInProgress ? Integer.MAX_VALUE : 0;
        // if there is no node available to connect, back off refreshing metadata
        long metadataTimeout = Math.max(Math.max(timeToNextMetadataUpdate, timeToNextReconnectAttempt),
                                        waitForMetadataFetch);
        if (metadataTimeout == 0)
            maybeUpdateMetadata(now);
        // do the I/O
        try {
            this.selector.poll(Math.min(timeout, metadataTimeout));
        } catch (IOException e) {
            log.error("Unexpected error during I/O in producer network thread", e);
        }

        // process completed actions
        List<ClientResponse> responses = new ArrayList<ClientResponse>();
        handleCompletedSends(responses, now);
        handleCompletedReceives(responses, now);
        handleDisconnections(responses, now);
        handleConnections();

        // invoke callbacks
        for (ClientResponse response : responses) {
            if (response.request().hasCallback()) {
                try {
                    response.request().callback().onComplete(response);
                } catch (Exception e) {
                    log.error("Uncaught error in request completion:", e);
                }
            }
        }

        return responses;
    }

    /**
     * Await all the outstanding responses for requests on the given connection
     * 
     * @param node The node to block on
     * @param now The current time in ms
     * @return All the collected responses
     */
    @Override
    public List<ClientResponse> completeAll(String node, long now) {
        try {
            this.selector.muteAll();
            this.selector.unmute(node);
            List<ClientResponse> responses = new ArrayList<ClientResponse>();
            while (inFlightRequestCount(node) > 0)
                responses.addAll(poll(Integer.MAX_VALUE, now));
            return responses;
        } finally {
            this.selector.unmuteAll();
        }
    }

    /**
     * Wait for all outstanding requests to complete.
     */
    @Override
    public List<ClientResponse> completeAll(long now) {
        List<ClientResponse> responses = new ArrayList<ClientResponse>();
        while (inFlightRequestCount() > 0)
            responses.addAll(poll(Integer.MAX_VALUE, now));
        return responses;
    }

    /**
     * Get the number of in-flight requests
     */
    @Override
    public int inFlightRequestCount() {
        return this.inFlightRequests.inFlightRequestCount();
    }

    /**
     * Get the number of in-flight requests for a given node
     */
    @Override
    public int inFlightRequestCount(String node) {
        return this.inFlightRequests.inFlightRequestCount(node);
    }

    /**
     * Generate a request header for the given API key
     * 
     * @param key The api key
     * @return A request header with the appropriate client id and correlation id
     */
    @Override
    public RequestHeader nextRequestHeader(ApiKeys key) {
        return new RequestHeader(key.id, clientId, correlation++);
    }

    /**
     * Interrupt the client if it is blocked waiting on I/O.
     */
    @Override
    public void wakeup() {
        this.selector.wakeup();
    }

    /**
     * Close the network client
     */
    @Override
    public void close() {
        this.selector.close();
    }

    /**
     * Choose the node with the fewest outstanding requests which is at least eligible for connection. This method will
     * prefer a node with an existing connection, but will potentially choose a node for which we don't yet have a
     * connection if all existing connections are in use. This method will never choose a node for which there is no
     * existing connection and from which we have disconnected within the reconnect backoff period.
     * 
     * @return The node with the fewest in-flight requests.
     */
    public Node leastLoadedNode(long now) {
        List<Node> nodes = this.metadata.fetch().nodes();
        int inflight = Integer.MAX_VALUE;
        Node found = null;
        for (int i = 0; i < nodes.size(); i++) {
            int idx = Utils.abs((this.nodeIndexOffset + i) % nodes.size());
            Node node = nodes.get(idx);
            int currInflight = this.inFlightRequests.inFlightRequestCount(node.idString());
            if (currInflight == 0 && this.connectionStates.isConnected(node.idString())) {
                // if we find an established connection with no in-flight requests we can stop right away
                return node;
            } else if (!this.connectionStates.isBlackedOut(node.idString(), now) && currInflight < inflight) {
                // otherwise if this is the best we have found so far, record that
                inflight = currInflight;
                found = node;
            }
        }
        return found;
    }

    /**
     * Handle any completed request send. In particular if no response is expected consider the request complete.
     * 
     * @param responses The list of responses to update
     * @param now The current time
     */
    private void handleCompletedSends(List<ClientResponse> responses, long now) {
        // if no response is expected then when the send is completed, return it
        for (Send send : this.selector.completedSends()) {
            ClientRequest request = this.inFlightRequests.lastSent(send.destination());
            if (!request.expectResponse()) {
                this.inFlightRequests.completeLastSent(send.destination());
                responses.add(new ClientResponse(request, now, false, null));
            }
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
            ClientRequest req = inFlightRequests.completeNext(source);
            ResponseHeader header = ResponseHeader.parse(receive.payload());
            short apiKey = req.request().header().apiKey();
            Struct body = (Struct) ProtoUtils.currentResponseSchema(apiKey).read(receive.payload());
            correlate(req.request().header(), header);
            if (apiKey == ApiKeys.METADATA.id && req.isInitiatedByNetworkClient()) {
                handleMetadataResponse(req.request().header(), body, now);
            } else {
                // need to add body/header to response here
                responses.add(new ClientResponse(req, now, false, body));
            }
        }
    }

    private void handleMetadataResponse(RequestHeader header, Struct body, long now) {
        this.metadataFetchInProgress = false;
        MetadataResponse response = new MetadataResponse(body);
        Cluster cluster = response.cluster();
        // check if any topics metadata failed to get updated
        if (response.errors().size() > 0) {
            log.warn("Error while fetching metadata with correlation id {} : {}", header.correlationId(), response.errors());
        }
        // don't update the cluster if there are no valid nodes...the topic we want may still be in the process of being
        // created which means we will get errors and no nodes until it exists
        if (cluster.nodes().size() > 0) {
            this.metadata.update(cluster, now);
        } else {
            log.trace("Ignoring empty metadata response with correlation id {}.", header.correlationId());
            this.metadata.failedUpdate(now);
        }
    }

    /**
     * Handle any disconnected connections
     * 
     * @param responses The list of responses that completed with the disconnection
     * @param now The current time
     */
    private void handleDisconnections(List<ClientResponse> responses, long now) {
        for (String node : this.selector.disconnected()) {
            connectionStates.disconnected(node);
            log.debug("Node {} disconnected.", node);
            for (ClientRequest request : this.inFlightRequests.clearAll(node)) {
                log.trace("Cancelled request {} due to node {} being disconnected", request, node);
                ApiKeys requestKey = ApiKeys.forId(request.request().header().apiKey());
                if (requestKey == ApiKeys.METADATA)
                    metadataFetchInProgress = false;
                else
                    responses.add(new ClientResponse(request, now, true, null));
            }
        }
        // we got a disconnect so we should probably refresh our metadata and see if that broker is dead
        if (this.selector.disconnected().size() > 0)
            this.metadata.requestUpdate();
    }

    /**
     * Record any newly completed connections
     */
    private void handleConnections() {
        for (String node : this.selector.connected()) {
            log.debug("Completed connection to node {}", node);
            this.connectionStates.connected(node);
        }
    }

    /**
     * Validate that the response corresponds to the request we expect or else explode
     */
    private void correlate(RequestHeader requestHeader, ResponseHeader responseHeader) {
        if (requestHeader.correlationId() != responseHeader.correlationId())
            throw new IllegalStateException("Correlation id for response (" + responseHeader.correlationId()
                    + ") does not match request (" + requestHeader.correlationId() + ")");
    }

    /**
     * Create a metadata request for the given topics
     */
    private ClientRequest metadataRequest(long now, String node, Set<String> topics) {
        MetadataRequest metadata = new MetadataRequest(new ArrayList<String>(topics));
        RequestSend send = new RequestSend(node, nextRequestHeader(ApiKeys.METADATA), metadata.toStruct());
        return new ClientRequest(now, true, send, null, true);
    }

    /**
     * Add a metadata request to the list of sends if we can make one
     */
    private void maybeUpdateMetadata(long now) {
        // Beware that the behavior of this method and the computation of timeouts for poll() are
        // highly dependent on the behavior of leastLoadedNode.
        Node node = this.leastLoadedNode(now);
        if (node == null) {
            log.debug("Give up sending metadata request since no node is available");
            // mark the timestamp for no node available to connect
            this.lastNoNodeAvailableMs = now;
            return;
        }
        String nodeConnectionId = node.idString();


        if (connectionStates.isConnected(nodeConnectionId) && inFlightRequests.canSendMore(nodeConnectionId)) {
            Set<String> topics = metadata.topics();
            this.metadataFetchInProgress = true;
            ClientRequest metadataRequest = metadataRequest(now, nodeConnectionId, topics);
            log.debug("Sending metadata request {} to node {}", metadataRequest, node.id());
            this.selector.send(metadataRequest.request());
            this.inFlightRequests.add(metadataRequest);
        } else if (connectionStates.canConnect(nodeConnectionId, now)) {
            // we don't have a connection to this node right now, make one
            log.debug("Initialize connection to node {} for sending metadata request", node.id());
            initiateConnect(node, now);
            // If initiateConnect failed immediately, this node will be put into blackout and we
            // should allow immediately retrying in case there is another candidate node. If it
            // is still connecting, the worst case is that we end up setting a longer timeout
            // on the next round and then wait for the response.
        } else { // connected, but can't send more OR connecting
            // In either case, we just need to wait for a network event to let us know the selected
            // connection might be usable again.
            this.lastNoNodeAvailableMs = now;
        }
    }

    /**
     * Initiate a connection to the given node
     */
    private void initiateConnect(Node node, long now) {
        String nodeConnectionId = node.idString();
        try {
            log.debug("Initiating connection to node {} at {}:{}.", node.id(), node.host(), node.port());
            this.connectionStates.connecting(nodeConnectionId, now);
            selector.connect(nodeConnectionId,
                             new InetSocketAddress(node.host(), node.port()),
                             this.socketSendBuffer,
                             this.socketReceiveBuffer);
        } catch (IOException e) {
            /* attempt failed, we'll try again after the backoff */
            connectionStates.disconnected(nodeConnectionId);
            /* maybe the problem is our metadata, update it */
            metadata.requestUpdate();
            log.debug("Error connecting to node {} at {}:{}:", node.id(), node.host(), node.port(), e);
        }
    }

}
