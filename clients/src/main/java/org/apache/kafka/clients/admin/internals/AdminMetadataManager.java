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

package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.MetadataUpdater;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.MismatchedEndpointTypeException;
import org.apache.kafka.common.errors.UnsupportedEndpointTypeException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Manages the metadata for KafkaAdminClient.
 *
 * This class is not thread-safe.  It is only accessed from the AdminClient
 * service thread (which also uses the NetworkClient).
 */
public class AdminMetadataManager {
    private final Logger log;

    /**
     * The minimum amount of time that we should wait between subsequent
     * retries, when fetching metadata.
     */
    private final long refreshBackoffMs;

    /**
     * The minimum amount of time that we should wait before triggering an
     * automatic metadata refresh.
     */
    private final long metadataExpireMs;

    /**
     * True if we are communicating directly with the controller quorum as specified by KIP-919.
     */
    private final boolean usingBootstrapControllers;

    /**
     * Used to update the NetworkClient metadata.
     */
    private final AdminMetadataUpdater updater;

    /**
     * The current metadata state.
     */
    private State state = State.QUIESCENT;

    /**
     * The time in wall-clock milliseconds when we last updated the metadata.
     */
    private long lastMetadataUpdateMs = 0;

    /**
     * The time in wall-clock milliseconds when we last attempted to fetch new
     * metadata.
     */
    private long lastMetadataFetchAttemptMs = 0;

    /**
     * The current cluster information.
     */
    private Cluster cluster = Cluster.empty();

    /**
     * If this is non-null, it is a fatal exception that will terminate all attempts at communication.
     */
    private ApiException fatalException = null;

    public class AdminMetadataUpdater implements MetadataUpdater {
        @Override
        public List<Node> fetchNodes() {
            return cluster.nodes();
        }

        @Override
        public boolean isUpdateDue(long now) {
            return false;
        }

        @Override
        public long maybeUpdate(long now) {
            return Long.MAX_VALUE;
        }

        @Override
        public void handleServerDisconnect(long now, String destinationId, Optional<AuthenticationException> maybeFatalException) {
            maybeFatalException.ifPresent(AdminMetadataManager.this::updateFailed);
            AdminMetadataManager.this.requestUpdate();
        }

        @Override
        public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
            // Do nothing
        }

        @Override
        public void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse metadataResponse) {
            // Do nothing
        }

        @Override
        public boolean isBootstrapped() {
            // TODO: noop
            return false;
        }

        @Override
        public void bootstrap(List<InetSocketAddress> addresses) {
            // TODO: noop
        }

        @Override
        public void close() {
        }
    }

    /**
     * The current AdminMetadataManager state.
     */
    enum State {
        QUIESCENT,
        UPDATE_REQUESTED,
        UPDATE_PENDING
    }

    public AdminMetadataManager(
        LogContext logContext,
        long refreshBackoffMs,
        long metadataExpireMs,
        boolean usingBootstrapControllers
    ) {
        this.log = logContext.logger(AdminMetadataManager.class);
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.usingBootstrapControllers = usingBootstrapControllers;
        this.updater = new AdminMetadataUpdater();
    }

    public boolean usingBootstrapControllers() {
        return usingBootstrapControllers;
    }

    public AdminMetadataUpdater updater() {
        return updater;
    }

    public boolean isReady() {
        if (fatalException != null) {
            log.debug("Metadata is not usable: failed to get metadata.", fatalException);
            throw fatalException;
        }
        if (cluster.nodes().isEmpty()) {
            log.trace("Metadata is not ready: bootstrap nodes have not been " +
                "initialized yet.");
            return false;
        }
        if (cluster.isBootstrapConfigured()) {
            log.trace("Metadata is not ready: we have not fetched metadata from " +
                "the bootstrap nodes yet.");
            return false;
        }
        log.trace("Metadata is ready to use.");
        return true;
    }

    public Node controller() {
        return cluster.controller();
    }

    public Node nodeById(int nodeId) {
        return cluster.nodeById(nodeId);
    }

    public void requestUpdate() {
        if (state == State.QUIESCENT) {
            state = State.UPDATE_REQUESTED;
            log.debug("Requesting metadata update.");
        }
    }

    public void clearController() {
        if (cluster.controller() != null) {
            log.trace("Clearing cached controller node {}.", cluster.controller());
            this.cluster = new Cluster(cluster.clusterResource().clusterId(),
                cluster.nodes(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                null);
        }
    }

    /**
     * Determine if the AdminClient should fetch new metadata.
     */
    public long metadataFetchDelayMs(long now) {
        switch (state) {
            case QUIESCENT:
                // Calculate the time remaining until the next periodic update.
                // We want to avoid making many metadata requests in a short amount of time,
                // so there is a metadata refresh backoff period.
                return Math.max(delayBeforeNextAttemptMs(now), delayBeforeNextExpireMs(now));
            case UPDATE_REQUESTED:
                // Respect the backoff, even if an update has been requested
                return delayBeforeNextAttemptMs(now);
            default:
                // An update is already pending, so we don't need to initiate another one.
                return Long.MAX_VALUE;
        }
    }

    private long delayBeforeNextExpireMs(long now) {
        long timeSinceUpdate = now - lastMetadataUpdateMs;
        return Math.max(0, metadataExpireMs - timeSinceUpdate);
    }

    private long delayBeforeNextAttemptMs(long now) {
        long timeSinceAttempt = now - lastMetadataFetchAttemptMs;
        return Math.max(0, refreshBackoffMs - timeSinceAttempt);
    }

    /**
     * Transition into the UPDATE_PENDING state.  Updates lastMetadataFetchAttemptMs.
     */
    public void transitionToUpdatePending(long now) {
        this.state = State.UPDATE_PENDING;
        this.lastMetadataFetchAttemptMs = now;
    }

    public void updateFailed(Throwable exception) {
        // We depend on pending calls to request another metadata update
        this.state = State.QUIESCENT;

        if (exception instanceof AuthenticationException) {
            log.warn("Metadata update failed due to authentication error", exception);
            this.fatalException = (ApiException) exception;
        } else if (exception instanceof MismatchedEndpointTypeException) {
            log.warn("Metadata update failed due to mismatched endpoint type error", exception);
            this.fatalException = (ApiException) exception;
        } else if (exception instanceof UnsupportedEndpointTypeException) {
            log.warn("Metadata update failed due to unsupported endpoint type error", exception);
            this.fatalException = (ApiException) exception;
        } else if (exception instanceof UnsupportedVersionException) {
            if (usingBootstrapControllers) {
                log.warn("The remote node is not a CONTROLLER that supports the KIP-919 " +
                    "DESCRIBE_CLUSTER api.", exception);
            } else {
                log.warn("The remote node is not a BROKER that supports the METADATA api.", exception);
            }
            this.fatalException = (ApiException) exception;
        } else {
            log.info("Metadata update failed", exception);
        }
    }

    /**
     * Receive new metadata, and transition into the QUIESCENT state.
     * Updates lastMetadataUpdateMs, cluster, and authException.
     */
    public void update(Cluster cluster, long now) {
        if (cluster.isBootstrapConfigured()) {
            log.debug("Setting bootstrap cluster metadata {}.", cluster);
        } else {
            log.debug("Updating cluster metadata to {}", cluster);
            this.lastMetadataUpdateMs = now;
        }

        this.state = State.QUIESCENT;
        this.fatalException = null;

        if (!cluster.nodes().isEmpty()) {
            this.cluster = cluster;
        }
    }
}
