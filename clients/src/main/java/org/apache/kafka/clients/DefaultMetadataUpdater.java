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
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;

import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Owns metadata request and recovery policy for NetworkClient. */
final class DefaultMetadataUpdater implements MetadataUpdater {

    /* the current cluster metadata */
    private final Metadata metadata;
    private final NetworkClientTransport transport;
    private final Logger log;
    private final int defaultRequestTimeoutMs;
    private final long reconnectBackoffMs;
    private final MetadataRecoveryStrategy metadataRecoveryStrategy;

    // Defined if there is a request in progress, null otherwise
    private InProgressData inProgress;

    /*
     * The time in wall-clock milliseconds when we started attempts to fetch metadata. If empty,
     * metadata has not been requested. This is the start time based on which rebootstrap is
     * triggered if metadata is not obtained for the configured rebootstrap trigger interval.
     * Set to Optional.of(0L) to force rebootstrap immediately.
     */
    private Optional<Long> metadataAttemptStartMs = Optional.empty();

    DefaultMetadataUpdater(Metadata metadata, NetworkClientTransport transport, Logger log,
                           int defaultRequestTimeoutMs, long reconnectBackoffMs,
                           MetadataRecoveryStrategy metadataRecoveryStrategy) {
        this.transport = transport;
        this.log = log;
        this.defaultRequestTimeoutMs = defaultRequestTimeoutMs;
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.metadataRecoveryStrategy = metadataRecoveryStrategy;
        this.metadata = metadata;
        this.inProgress = null;
    }

    @Override
    public String clusterId() {
        ClusterResource clusterResource = metadata.fetch().clusterResource();
        if (clusterResource != null) {
            return clusterResource.clusterId();
        }
        return null;
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

        if (metadataAttemptStartMs.isEmpty())
            metadataAttemptStartMs = Optional.of(now);

        // Beware that the behavior of this method and the computation of timeouts for poll() are
        // highly dependent on the behavior of leastLoadedNode.
        LeastLoadedNode leastLoadedNode = transport.leastLoadedNode(now);

        // Rebootstrap if needed and configured.
        // Only rebootstrap if we've already completed initial bootstrap - otherwise we're still
        // in the initial DNS resolution phase and should let ensureBootstrapped() handle it.
        if (metadataRecoveryStrategy == MetadataRecoveryStrategy.REBOOTSTRAP
                && isBootstrapped()
                && !leastLoadedNode.hasNodeAvailableOrConnectionReady()) {
            rebootstrap(now);

            leastLoadedNode = transport.leastLoadedNode(now);
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

        if (metadataRecoveryStrategy == MetadataRecoveryStrategy.REBOOTSTRAP && response.topLevelError() == Errors.REBOOTSTRAP_REQUIRED) {
            log.info("Rebootstrap requested by server.");
            initiateRebootstrap();
        } else if (response.brokers().isEmpty()) {
            // When talking to the startup phase of a broker, it is possible to receive an empty metadata set, which
            // we should retry later.
            log.trace("Ignoring empty metadata response with correlation id {}.", requestHeader.correlationId());
            this.metadata.failedUpdate(now);
        } else {
            this.metadata.update(inProgress.requestVersion, response, inProgress.isPartialUpdate, now);
            metadataAttemptStartMs = Optional.empty();
        }

        inProgress = null;
    }

    @Override
    public boolean needsRebootstrap(long now, long rebootstrapTriggerMs) {
        return metadataAttemptStartMs.filter(startMs -> now - startMs > rebootstrapTriggerMs).isPresent();
    }

    @Override
    public void rebootstrap(long now) {
        metadata.rebootstrap();
        metadataAttemptStartMs = Optional.of(now);
    }

    @Override
    public void bootstrapFailed(KafkaException exception) {
        metadata.bootstrapFatalError(exception);
    }

    @Override
    public boolean isBootstrapped() {
        // We are bootstrapped if we have any nodes available (either from DNS resolution or metadata response)
        return !metadata.fetch().nodes().isEmpty();
    }

    @Override
    public void bootstrap(List<InetSocketAddress> addresses) {
        metadata.bootstrap(addresses);
    }

    @Override
    public void close() {
        this.metadata.close();
    }

    private void initiateRebootstrap() {
        metadataAttemptStartMs = Optional.of(0L); // to force rebootstrap
    }

    /**
     * Add a metadata request to the list of sends if we can make one
     */
    private long maybeUpdate(long now, Node node) {
        String nodeConnectionId = node.idString();

        if (transport.canSendRequest(nodeConnectionId, now)) {
            Metadata.MetadataRequestAndVersion requestAndVersion = metadata.newMetadataRequestAndVersion(now);
            MetadataRequest.Builder metadataRequest = requestAndVersion.requestBuilder;
            log.debug("Sending metadata request {} to node {}", metadataRequest, node);
            transport.sendInternalRequest(metadataRequest, nodeConnectionId, now);
            inProgress = new InProgressData(requestAndVersion.requestVersion, requestAndVersion.isPartialUpdate);
            return defaultRequestTimeoutMs;
        }

        // If there's any connection establishment underway, wait until it completes. This prevents
        // the client from unnecessarily connecting to additional nodes while a previous connection
        // attempt has not been completed.
        if (transport.isAnyNodeConnecting()) {
            // Strictly the timeout we should return here is "connect timeout", but as we don't
            // have such application level configuration, using reconnect backoff instead.
            return reconnectBackoffMs;
        }

        if (transport.canConnect(nodeConnectionId, now)) {
            // We don't have a connection to this node right now, make one
            log.debug("Initialize connection to node {} for sending metadata request", node);
            transport.initiateConnect(node, now);
            return reconnectBackoffMs;
        }

        // connected, but can't send more OR connecting
        // In either case, we just need to wait for a network event to let us know the selected
        // connection might be usable again.
        return Long.MAX_VALUE;
    }

    private static final class InProgressData {
        public final int requestVersion;
        public final boolean isPartialUpdate;

        private InProgressData(int requestVersion, boolean isPartialUpdate) {
            this.requestVersion = requestVersion;
            this.isPartialUpdate = isPartialUpdate;
        }
    }

}
