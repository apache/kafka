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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;

/**
 * Manages the metadata for KafkaAdminClient.
 *
 * This class is not thread-safe.  It is only accessed from the AdminClient
 * service thread (which also uses the NetworkClient).
 */
public class AdminMetadataManager {
    private Logger log;

    /**
     * The timer.
     */
    private final Time time;

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
     * If we got an authorization exception when we last attempted to fetch
     * metadata, this is it; null, otherwise.
     */
    private AuthenticationException authException = null;

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
        public void handleDisconnection(String destination) {
            // Do nothing
        }

        @Override
        public void handleAuthenticationFailure(AuthenticationException e) {
            log.info("AdminMetadataManager got AuthenticationException", e);
            update(Cluster.empty(), time.milliseconds(), e);
        }

        @Override
        public void handleCompletedMetadataResponse(RequestHeader requestHeader, long now, MetadataResponse metadataResponse) {
            // Do nothing
        }

        @Override
        public void requestUpdate() {
            // Do nothing
        }
    }

    /**
     * The current AdminMetadataManager state.
     */
    enum State {
        QUIESCENT,
        UPDATE_REQUESTED,
        UPDATE_PENDING;
    }

    public AdminMetadataManager(LogContext logContext, Time time, long refreshBackoffMs,
                                long metadataExpireMs) {
        this.log = logContext.logger(AdminMetadataManager.class);
        this.time = time;
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.updater = new AdminMetadataUpdater();
    }

    public AdminMetadataUpdater updater() {
        return updater;
    }

    public boolean isReady() {
        if (authException != null) {
            log.debug("Metadata is not usable: failed to get metadata.", authException);
            throw authException;
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
                Collections.<PartitionInfo>emptySet(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(),
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
                long timeSinceUpdate = now - lastMetadataUpdateMs;
                long timeRemainingUntilUpdate = metadataExpireMs - timeSinceUpdate;
                long timeSinceAttempt = now - lastMetadataFetchAttemptMs;
                long timeRemainingUntilAttempt = refreshBackoffMs - timeSinceAttempt;
                return Math.max(Math.max(0L, timeRemainingUntilUpdate), timeRemainingUntilAttempt);
            case UPDATE_REQUESTED:
                // An update has been explicitly requested.  Do it as soon as possible.
                return 0;
            default:
                // An update is already pending, so we don't need to initiate another one.
                return Long.MAX_VALUE;
        }
    }

    /**
     * Transition into the UPDATE_PENDING state.  Updates lastMetadataFetchAttemptMs.
     */
    public void transitionToUpdatePending(long now) {
        this.state = State.UPDATE_PENDING;
        this.lastMetadataFetchAttemptMs = now;
    }

    /**
     * Receive new metadata, and transition into the QUIESCENT state.
     * Updates lastMetadataUpdateMs, cluster, and authException.
     */
    public void update(Cluster cluster, long now, AuthenticationException authException) {
        if (cluster.isBootstrapConfigured()) {
            log.debug("Setting bootstrap cluster metadata {}.", cluster);
        } else {
            log.debug("Received cluster metadata {}{}.",
                cluster, authException == null ? "" : " with authentication exception.");
        }
        this.state = State.QUIESCENT;
        this.lastMetadataUpdateMs = now;
        this.authException = authException;
        if (!cluster.nodes().isEmpty()) {
            this.cluster = cluster;
        }
    }
}
