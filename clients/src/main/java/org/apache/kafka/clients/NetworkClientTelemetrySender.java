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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.PushTelemetryResponse;
import org.apache.kafka.common.telemetry.internals.ClientTelemetrySender;

import org.slf4j.Logger;

import java.util.Optional;

/** Owns telemetry request scheduling and sticky-node policy for NetworkClient. */
final class NetworkClientTelemetrySender {

    private final ClientTelemetrySender clientTelemetrySender;
    private final NetworkClientTransport transport;
    private final MetadataUpdater metadataUpdater;
    private final Logger log;
    private final int defaultRequestTimeoutMs;
    private final long reconnectBackoffMs;
    private Node stickyNode;

    NetworkClientTelemetrySender(ClientTelemetrySender clientTelemetrySender,
                                 NetworkClientTransport transport, MetadataUpdater metadataUpdater,
                                 Logger log, int defaultRequestTimeoutMs, long reconnectBackoffMs) {
        this.transport = transport;
        this.metadataUpdater = metadataUpdater;
        this.log = log;
        this.defaultRequestTimeoutMs = defaultRequestTimeoutMs;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.clientTelemetrySender = clientTelemetrySender;
    }

    Node stickyNode() {
        return stickyNode;
    }

    public long maybeUpdate(long now) {
        long timeToNextUpdate = clientTelemetrySender.timeToNextUpdate(defaultRequestTimeoutMs);
        if (timeToNextUpdate > 0)
            return timeToNextUpdate;

        // The node connection params can change while having the same node id hence check if the cached
        // sticky node has not changed, if changed then reset the sticky node.
        if (stickyNode != null && isNodeChanged(stickyNode)) {
            log.debug("Telemetry stickyNode {} either is no longer in metadata or changed, clearing it.", stickyNode);
            stickyNode = null;
        }

        // Per KIP-714, let's continue to re-use the same broker for as long as possible.
        if (stickyNode == null) {
            stickyNode = transport.leastLoadedNode(now).node();
            if (stickyNode == null) {
                log.debug("Give up sending telemetry request since no node is available");
                return reconnectBackoffMs;
            }
        }

        return maybeUpdate(now, stickyNode);
    }

    private long maybeUpdate(long now, Node node) {
        String nodeConnectionId = node.idString();

        if (transport.canSendRequest(nodeConnectionId, now)) {
            Optional<AbstractRequest.Builder<?>> requestOpt = clientTelemetrySender.createRequest();

            if (requestOpt.isEmpty())
                return Long.MAX_VALUE;

            AbstractRequest.Builder<?> request = requestOpt.get();
            transport.sendInternalRequest(request, nodeConnectionId, now);
            return defaultRequestTimeoutMs;
        } else {
            // Per KIP-714, if we can't issue a request to this broker node, let's clear it out
            // and try another broker on the next loop.
            stickyNode = null;
        }

        // If there's any connection establishment underway, wait until it completes. This prevents
        // the client from unnecessarily connecting to additional nodes while a previous connection
        // attempt has not been completed.
        if (transport.isAnyNodeConnecting())
            return reconnectBackoffMs;

        if (transport.canConnect(nodeConnectionId, now)) {
            // We don't have a connection to this node right now, make one
            log.debug("Initialize connection to node {} for sending telemetry request", node);
            transport.initiateConnect(node, now);
            return reconnectBackoffMs;
        }

        // In either case, we just need to wait for a network event to let us know the selected
        // connection might be usable again.
        return Long.MAX_VALUE;
    }

    private boolean isNodeChanged(Node node) {
        Node newNode = metadataUpdater.fetchNodes().stream()
                .filter(n -> n.id() == node.id())
                .findFirst().orElse(null);
        return newNode == null || !newNode.equals(node);
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
