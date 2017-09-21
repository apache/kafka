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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * AbstractCoordinator implements group management for a single group member by interacting with
 * a designated Kafka broker (the coordinator). Group semantics are provided by extending this class.
 * See {@link ConsumerCoordinator} for example usage.
 *
 * From a high level, Kafka's group management protocol consists of the following sequence of actions:
 *
 * <ol>
 *     <li>Group Registration: Group members register with the coordinator providing their own metadata
 *         (such as the set of topics they are interested in).</li>
 *     <li>Group/Leader Selection: The coordinator select the members of the group and chooses one member
 *         as the leader.</li>
 *     <li>State Assignment: The leader collects the metadata from all the members of the group and
 *         assigns state.</li>
 *     <li>Group Stabilization: Each member receives the state assigned by the leader and begins
 *         processing.</li>
 * </ol>
 *
 * To leverage this protocol, an implementation must define the format of metadata provided by each
 * member for group registration in {@link #metadata()} and the format of the state assignment provided
 * by the leader in {@link #performAssignment(String, String, Map)} and becomes available to members in
 * {@link #onJoinComplete(int, String, String, ByteBuffer)}.
 *
 * Note on locking: this class shares state between the caller and a background thread which is
 * used for sending heartbeats after the client has joined the group. All mutable state as well as
 * state transitions are protected with the class's monitor. Generally this means acquiring the lock
 * before reading or writing the state of the group (e.g. generation, memberId) and holding the lock
 * when sending a request that affects the state of the group (e.g. JoinGroup, LeaveGroup).
 */
public abstract class AbstractCoordinator implements Closeable {
    public static final String HEARTBEAT_THREAD_PREFIX = "kafka-coordinator-heartbeat-thread";

    private enum MemberState {
        UNJOINED,    // the client is not part of a group
        REBALANCING, // the client has begun rebalancing
        STABLE,      // the client has joined and is sending heartbeats
    }

    private final Logger log;
    private final int sessionTimeoutMs;
    private final boolean leaveGroupOnClose;
    private final GroupCoordinatorMetrics sensors;
    private final Heartbeat heartbeat;
    protected final int rebalanceTimeoutMs;
    protected final String groupId;
    protected final ConsumerNetworkClient client;
    protected final Time time;
    protected final long retryBackoffMs;

    private HeartbeatThread heartbeatThread = null;
    private boolean rejoinNeeded = true;
    private boolean needsJoinPrepare = true;
    private MemberState state = MemberState.UNJOINED;
    private RequestFuture<ByteBuffer> joinFuture = null;
    private Node coordinator = null;
    private Generation generation = Generation.NO_GENERATION;

    private RequestFuture<Void> findCoordinatorFuture = null;

    /**
     * Initialize the coordination manager.
     */
    public AbstractCoordinator(LogContext logContext,
                               ConsumerNetworkClient client,
                               String groupId,
                               int rebalanceTimeoutMs,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs,
                               boolean leaveGroupOnClose) {
        this.log = logContext.logger(AbstractCoordinator.class);
        this.client = client;
        this.time = time;
        this.groupId = groupId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.leaveGroupOnClose = leaveGroupOnClose;
        this.heartbeat = new Heartbeat(sessionTimeoutMs, heartbeatIntervalMs, rebalanceTimeoutMs, retryBackoffMs);
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix);
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Unique identifier for the class of supported protocols (e.g. "consumer" or "connect").
     * @return Non-null protocol type name
     */
    protected abstract String protocolType();

    /**
     * Get the current list of protocols and their associated metadata supported
     * by the local member. The order of the protocols in the list indicates the preference
     * of the protocol (the first entry is the most preferred). The coordinator takes this
     * preference into account when selecting the generation protocol (generally more preferred
     * protocols will be selected as long as all members support them and there is no disagreement
     * on the preference).
     * @return Non-empty map of supported protocols and metadata
     */
    protected abstract List<ProtocolMetadata> metadata();

    /**
     * Invoked prior to each group join or rejoin. This is typically used to perform any
     * cleanup from the previous generation (such as committing offsets for the consumer)
     * @param generation The previous generation or -1 if there was none
     * @param memberId The identifier of this member in the previous group or "" if there was none
     */
    protected abstract void onJoinPrepare(int generation, String memberId);

    /**
     * Perform assignment for the group. This is used by the leader to push state to all the members
     * of the group (e.g. to push partition assignments in the case of the new consumer)
     * @param leaderId The id of the leader (which is this member)
     * @param allMemberMetadata Metadata from all members of the group
     * @return A map from each member to their state assignment
     */
    protected abstract Map<String, ByteBuffer> performAssignment(String leaderId,
                                                                 String protocol,
                                                                 Map<String, ByteBuffer> allMemberMetadata);

    /**
     * Invoked when a group member has successfully joined a group. If this call is woken up (i.e.
     * if the invocation raises {@link org.apache.kafka.common.errors.WakeupException}), then it
     * will be retried on the next call to {@link #ensureActiveGroup()}.
     *
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param protocol The protocol selected by the coordinator
     * @param memberAssignment The assignment propagated from the group leader
     */
    protected abstract void onJoinComplete(int generation,
                                           String memberId,
                                           String protocol,
                                           ByteBuffer memberAssignment);

    /**
     * Block until the coordinator for this group is known and is ready to receive requests.
     */
    public synchronized void ensureCoordinatorReady() {
        // Using zero as current time since timeout is effectively infinite
        ensureCoordinatorReady(0, Long.MAX_VALUE);
    }

    /**
     * Ensure that the coordinator is ready to receive requests.
     * @param startTimeMs Current time in milliseconds
     * @param timeoutMs Maximum time to wait to discover the coordinator
     * @return true If coordinator discovery and initial connection succeeded, false otherwise
     */
    protected synchronized boolean ensureCoordinatorReady(long startTimeMs, long timeoutMs) {
        long remainingMs = timeoutMs;

        while (coordinatorUnknown()) {
            RequestFuture<Void> future = lookupCoordinator();
            client.poll(future, remainingMs);

            if (future.failed()) {
                if (future.isRetriable()) {
                    remainingMs = timeoutMs - (time.milliseconds() - startTimeMs);
                    if (remainingMs <= 0)
                        break;

                    log.debug("Coordinator discovery failed, refreshing metadata");
                    client.awaitMetadataUpdate(remainingMs);
                } else
                    throw future.exception();
            } else if (coordinator != null && client.connectionFailed(coordinator)) {
                // we found the coordinator, but the connection has failed, so mark
                // it dead and backoff before retrying discovery
                coordinatorDead();
                time.sleep(retryBackoffMs);
            }

            remainingMs = timeoutMs - (time.milliseconds() - startTimeMs);
            if (remainingMs <= 0)
                break;
        }

        return !coordinatorUnknown();
    }

    protected synchronized RequestFuture<Void> lookupCoordinator() {
        if (findCoordinatorFuture == null) {
            // find a node to ask about the coordinator
            Node node = this.client.leastLoadedNode();
            if (node == null) {
                log.debug("No broker available to send GroupCoordinator request");
                return RequestFuture.noBrokersAvailable();
            } else
                findCoordinatorFuture = sendGroupCoordinatorRequest(node);
        }
        return findCoordinatorFuture;
    }

    private synchronized void clearFindCoordinatorFuture() {
        findCoordinatorFuture = null;
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes)
     * @return true if it should, false otherwise
     */
    protected synchronized boolean needRejoin() {
        return rejoinNeeded;
    }

    private synchronized boolean rejoinIncomplete() {
        return joinFuture != null;
    }

    /**
     * Check the status of the heartbeat thread (if it is active) and indicate the liveness
     * of the client. This must be called periodically after joining with {@link #ensureActiveGroup()}
     * to ensure that the member stays in the group. If an interval of time longer than the
     * provided rebalance timeout expires without calling this method, then the client will proactively
     * leave the group.
     * @param now current time in milliseconds
     * @throws RuntimeException for unexpected errors raised from the heartbeat thread
     */
    protected synchronized void pollHeartbeat(long now) {
        if (heartbeatThread != null) {
            if (heartbeatThread.hasFailed()) {
                // set the heartbeat thread to null and raise an exception. If the user catches it,
                // the next call to ensureActiveGroup() will spawn a new heartbeat thread.
                RuntimeException cause = heartbeatThread.failureCause();
                heartbeatThread = null;
                throw cause;
            }
            // Awake the heartbeat thread if needed
            if (heartbeat.shouldHeartbeat(now)) {
                notify();
            }
            heartbeat.poll(now);
        }
    }

    protected synchronized long timeToNextHeartbeat(long now) {
        // if we have not joined the group, we don't need to send heartbeats
        if (state == MemberState.UNJOINED)
            return Long.MAX_VALUE;
        return heartbeat.timeToNextHeartbeat(now);
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     */
    public void ensureActiveGroup() {
        // always ensure that the coordinator is ready because we may have been disconnected
        // when sending heartbeats and does not necessarily require us to rejoin the group.
        ensureCoordinatorReady();
        startHeartbeatThreadIfNeeded();
        joinGroupIfNeeded();
    }

    private synchronized void startHeartbeatThreadIfNeeded() {
        if (heartbeatThread == null) {
            heartbeatThread = new HeartbeatThread();
            heartbeatThread.start();
        }
    }

    private synchronized void disableHeartbeatThread() {
        if (heartbeatThread != null)
            heartbeatThread.disable();
    }

    private void closeHeartbeatThread() {
        HeartbeatThread thread = null;
        synchronized (this) {
            if (heartbeatThread == null)
                return;
            heartbeatThread.close();
            thread = heartbeatThread;
            heartbeatThread = null;
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for consumer heartbeat thread to close");
            throw new InterruptException(e);
        }
    }

    // visible for testing. Joins the group without starting the heartbeat thread.
    void joinGroupIfNeeded() {
        while (needRejoin() || rejoinIncomplete()) {
            ensureCoordinatorReady();

            // call onJoinPrepare if needed. We set a flag to make sure that we do not call it a second
            // time if the client is woken up before a pending rebalance completes. This must be called
            // on each iteration of the loop because an event requiring a rebalance (such as a metadata
            // refresh which changes the matched subscription set) can occur while another rebalance is
            // still in progress.
            if (needsJoinPrepare) {
                onJoinPrepare(generation.generationId, generation.memberId);
                needsJoinPrepare = false;
            }

            RequestFuture<ByteBuffer> future = initiateJoinGroup();
            client.poll(future);

            if (future.succeeded()) {
                onJoinComplete(generation.generationId, generation.memberId, generation.protocol, future.value());

                // We reset the join group future only after the completion callback returns. This ensures
                // that if the callback is woken up, we will retry it on the next joinGroupIfNeeded.
                resetJoinGroupFuture();
                needsJoinPrepare = true;
            } else {
                resetJoinGroupFuture();
                RuntimeException exception = future.exception();
                if (exception instanceof UnknownMemberIdException ||
                        exception instanceof RebalanceInProgressException ||
                        exception instanceof IllegalGenerationException)
                    continue;
                else if (!future.isRetriable())
                    throw exception;
                time.sleep(retryBackoffMs);
            }
        }
    }

    private synchronized void resetJoinGroupFuture() {
        this.joinFuture = null;
    }

    private synchronized RequestFuture<ByteBuffer> initiateJoinGroup() {
        // we store the join future in case we are woken up by the user after beginning the
        // rebalance in the call to poll below. This ensures that we do not mistakenly attempt
        // to rejoin before the pending rebalance has completed.
        if (joinFuture == null) {
            // fence off the heartbeat thread explicitly so that it cannot interfere with the join group.
            // Note that this must come after the call to onJoinPrepare since we must be able to continue
            // sending heartbeats if that callback takes some time.
            disableHeartbeatThread();

            state = MemberState.REBALANCING;
            joinFuture = sendJoinGroupRequest();
            joinFuture.addListener(new RequestFutureListener<ByteBuffer>() {
                @Override
                public void onSuccess(ByteBuffer value) {
                    // handle join completion in the callback so that the callback will be invoked
                    // even if the consumer is woken up before finishing the rebalance
                    synchronized (AbstractCoordinator.this) {
                        log.info("Successfully joined group with generation {}", generation.generationId);
                        state = MemberState.STABLE;
                        rejoinNeeded = false;

                        if (heartbeatThread != null)
                            heartbeatThread.enable();
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    // we handle failures below after the request finishes. if the join completes
                    // after having been woken up, the exception is ignored and we will rejoin
                    synchronized (AbstractCoordinator.this) {
                        state = MemberState.UNJOINED;
                    }
                }
            });
        }
        return joinFuture;
    }

    /**
     * Join the group and return the assignment for the next generation. This function handles both
     * JoinGroup and SyncGroup, delegating to {@link #performAssignment(String, String, Map)} if
     * elected leader by the coordinator.
     * @return A request future which wraps the assignment returned from the group leader
     */
    private RequestFuture<ByteBuffer> sendJoinGroupRequest() {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // send a join group request to the coordinator
        log.info("(Re-)joining group");
        JoinGroupRequest.Builder requestBuilder = new JoinGroupRequest.Builder(
                groupId,
                this.sessionTimeoutMs,
                this.generation.memberId,
                protocolType(),
                metadata()).setRebalanceTimeout(this.rebalanceTimeoutMs);

        log.debug("Sending JoinGroup ({}) to coordinator {}", requestBuilder, this.coordinator);
        return client.send(coordinator, requestBuilder)
                .compose(new JoinGroupResponseHandler());
    }

    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {
        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
            Errors error = joinResponse.error();
            if (error == Errors.NONE) {
                log.debug("Received successful JoinGroup response: {}", joinResponse);
                sensors.joinLatency.record(response.requestLatencyMs());

                synchronized (AbstractCoordinator.this) {
                    if (state != MemberState.REBALANCING) {
                        // if the consumer was woken up before a rebalance completes, we may have already left
                        // the group. In this case, we do not want to continue with the sync group.
                        future.raise(new UnjoinedGroupException());
                    } else {
                        AbstractCoordinator.this.generation = new Generation(joinResponse.generationId(),
                                joinResponse.memberId(), joinResponse.groupProtocol());
                        if (joinResponse.isLeader()) {
                            onJoinLeader(joinResponse).chain(future);
                        } else {
                            onJoinFollower().chain(future);
                        }
                    }
                }
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                log.debug("Attempt to join group rejected since coordinator {} is loading the group.", coordinator());
                // backoff and retry
                future.raise(error);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                // reset the member id and retry immediately
                resetGeneration();
                log.debug("Attempt to join group failed due to unknown member id.");
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR) {
                // re-discover the coordinator and retry with backoff
                coordinatorDead();
                log.debug("Attempt to join group failed due to obsolete coordinator information: {}", error.message());
                future.raise(error);
            } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL
                    || error == Errors.INVALID_SESSION_TIMEOUT
                    || error == Errors.INVALID_GROUP_ID) {
                // log the error and re-throw the exception
                log.error("Attempt to join group failed due to fatal error: {}", error.message());
                future.raise(error);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                // unexpected error, throw the exception
                future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
            }
        }
    }

    private RequestFuture<ByteBuffer> onJoinFollower() {
        // send follower's sync group with an empty assignment
        SyncGroupRequest.Builder requestBuilder =
                new SyncGroupRequest.Builder(groupId, generation.generationId, generation.memberId,
                        Collections.<String, ByteBuffer>emptyMap());
        log.debug("Sending follower SyncGroup to coordinator {}: {}", this.coordinator, requestBuilder);
        return sendSyncGroupRequest(requestBuilder);
    }

    private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
        try {
            // perform the leader synchronization and send back the assignment for the group
            Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.leaderId(), joinResponse.groupProtocol(),
                    joinResponse.members());

            SyncGroupRequest.Builder requestBuilder =
                    new SyncGroupRequest.Builder(groupId, generation.generationId, generation.memberId, groupAssignment);
            log.debug("Sending leader SyncGroup to coordinator {}: {}", this.coordinator, requestBuilder);
            return sendSyncGroupRequest(requestBuilder);
        } catch (RuntimeException e) {
            return RequestFuture.failure(e);
        }
    }

    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest.Builder requestBuilder) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();
        return client.send(coordinator, requestBuilder)
                .compose(new SyncGroupResponseHandler());
    }

    private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {
        @Override
        public void handle(SyncGroupResponse syncResponse,
                           RequestFuture<ByteBuffer> future) {
            Errors error = syncResponse.error();
            if (error == Errors.NONE) {
                sensors.syncLatency.record(response.requestLatencyMs());
                future.complete(syncResponse.memberAssignment());
            } else {
                requestRejoin();

                if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(new GroupAuthorizationException(groupId));
                } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                    log.debug("SyncGroup failed due to group rebalance");
                    future.raise(error);
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION) {
                    log.debug("SyncGroup failed: {}", error.message());
                    resetGeneration();
                    future.raise(error);
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR) {
                    log.debug("SyncGroup failed:", error.message());
                    coordinatorDead();
                    future.raise(error);
                } else {
                    future.raise(new KafkaException("Unexpected error from SyncGroup: " + error.message()));
                }
            }
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendGroupCoordinatorRequest(Node node) {
        // initiate the group metadata request
        log.debug("Sending GroupCoordinator request to broker {}", node);
        FindCoordinatorRequest.Builder requestBuilder =
                new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, this.groupId);
        return client.send(node, requestBuilder)
                     .compose(new GroupCoordinatorResponseHandler());
    }

    private class GroupCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {

        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            log.debug("Received GroupCoordinator response {}", resp);

            FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) resp.responseBody();
            // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
            // for the coordinator in the underlying network client layer
            // TODO: this needs to be better handled in KAFKA-1935
            Errors error = findCoordinatorResponse.error();
            clearFindCoordinatorFuture();
            if (error == Errors.NONE) {
                synchronized (AbstractCoordinator.this) {
                    AbstractCoordinator.this.coordinator = new Node(
                            Integer.MAX_VALUE - findCoordinatorResponse.node().id(),
                            findCoordinatorResponse.node().host(),
                            findCoordinatorResponse.node().port());
                    log.info("Discovered coordinator {}", coordinator);
                    client.tryConnect(coordinator);
                    heartbeat.resetTimeouts(time.milliseconds());
                }
                future.complete(null);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                log.debug("Group coordinator lookup failed: {}", error.message());
                future.raise(error);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<Void> future) {
            clearFindCoordinatorFuture();
            super.onFailure(e, future);
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     * @return true if the coordinator is unknown
     */
    public boolean coordinatorUnknown() {
        return coordinator() == null;
    }

    /**
     * Get the current coordinator
     * @return the current coordinator or null if it is unknown
     */
    protected synchronized Node coordinator() {
        if (coordinator != null && client.connectionFailed(coordinator)) {
            coordinatorDead();
            return null;
        }
        return this.coordinator;
    }

    /**
     * Mark the current coordinator as dead.
     */
    protected synchronized void coordinatorDead() {
        if (this.coordinator != null) {
            log.info("Marking the coordinator {} dead", this.coordinator);

            // Disconnect from the coordinator to ensure that there are no in-flight requests remaining.
            // Pending callbacks will be invoked with a DisconnectException.
            client.disconnect(this.coordinator);
            this.coordinator = null;
        }
    }

    /**
     * Get the current generation state if the group is stable.
     * @return the current generation or null if the group is unjoined/rebalancing
     */
    protected synchronized Generation generation() {
        if (this.state != MemberState.STABLE)
            return null;
        return generation;
    }

    /**
     * Reset the generation and memberId because we have fallen out of the group.
     */
    protected synchronized void resetGeneration() {
        this.generation = Generation.NO_GENERATION;
        this.rejoinNeeded = true;
        this.state = MemberState.UNJOINED;
    }

    protected synchronized void requestRejoin() {
        this.rejoinNeeded = true;
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     */
    @Override
    public final void close() {
        close(0);
    }

    protected void close(long timeoutMs) {
        try {
            closeHeartbeatThread();
        } finally {

            // Synchronize after closing the heartbeat thread since heartbeat thread
            // needs this lock to complete and terminate after close flag is set.
            synchronized (this) {
                if (leaveGroupOnClose) {
                    maybeLeaveGroup();
                }

                // At this point, there may be pending commits (async commits or sync commits that were
                // interrupted using wakeup) and the leave group request which have been queued, but not
                // yet sent to the broker. Wait up to close timeout for these pending requests to be processed.
                // If coordinator is not known, requests are aborted.
                Node coordinator = coordinator();
                if (coordinator != null && !client.awaitPendingRequests(coordinator, timeoutMs))
                    log.warn("Close timed out with {} pending requests to coordinator, terminating client connections",
                            client.pendingRequestCount(coordinator));
            }
        }
    }

    /**
     * Leave the current group and reset local generation/memberId.
     */
    public synchronized void maybeLeaveGroup() {
        if (!coordinatorUnknown() && state != MemberState.UNJOINED && generation != Generation.NO_GENERATION) {
            // this is a minimal effort attempt to leave the group. we do not
            // attempt any resending if the request fails or times out.
            log.debug("Sending LeaveGroup request to coordinator {}", coordinator);
            LeaveGroupRequest.Builder request =
                    new LeaveGroupRequest.Builder(groupId, generation.memberId);
            client.send(coordinator, request)
                    .compose(new LeaveGroupResponseHandler());
            client.pollNoWakeup();
        }

        resetGeneration();
    }

    private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {
        @Override
        public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
            Errors error = leaveResponse.error();
            if (error == Errors.NONE) {
                log.debug("LeaveGroup request returned successfully");
                future.complete(null);
            } else {
                log.debug("LeaveGroup request failed with error: {}", error.message());
                future.raise(error);
            }
        }
    }

    // visible for testing
    synchronized RequestFuture<Void> sendHeartbeatRequest() {
        log.debug("Sending Heartbeat request to coordinator {}", coordinator);
        HeartbeatRequest.Builder requestBuilder =
                new HeartbeatRequest.Builder(this.groupId, this.generation.generationId, this.generation.memberId);
        return client.send(coordinator, requestBuilder)
                .compose(new HeartbeatResponseHandler());
    }

    private class HeartbeatResponseHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatLatency.record(response.requestLatencyMs());
            Errors error = heartbeatResponse.error();
            if (error == Errors.NONE) {
                log.debug("Received successful Heartbeat response");
                future.complete(null);
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR) {
                log.debug("Attempt to heartbeat since coordinator {} is either not started or not valid.",
                        coordinator());
                coordinatorDead();
                future.raise(error);
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                log.debug("Attempt to heartbeat failed since group is rebalancing");
                requestRejoin();
                future.raise(Errors.REBALANCE_IN_PROGRESS);
            } else if (error == Errors.ILLEGAL_GENERATION) {
                log.debug("Attempt to heartbeat failed since generation {} is not current", generation.generationId);
                resetGeneration();
                future.raise(Errors.ILLEGAL_GENERATION);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                log.debug("Attempt to heartbeat failed for since member id {} is not valid.", generation.memberId);
                resetGeneration();
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T> extends RequestFutureAdapter<ClientResponse, T> {
        protected ClientResponse response;

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException) {
                coordinatorDead();
            }
            future.raise(e);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                R responseObj = (R) clientResponse.responseBody();
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone())
                    future.raise(e);
            }
        }

    }

    protected Meter createMeter(Metrics metrics, String groupName, String baseName, String descriptiveName) {
        return new Meter(new Count(),
                metrics.metricName(baseName + "-rate", groupName,
                        String.format("The number of %s per second", descriptiveName)),
                metrics.metricName(baseName + "-total", groupName,
                        String.format("The total number of %s", descriptiveName)));
    }

    private class GroupCoordinatorMetrics {
        public final String metricGrpName;

        public final Sensor heartbeatLatency;
        public final Sensor joinLatency;
        public final Sensor syncLatency;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(metrics.metricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatLatency.add(createMeter(metrics, metricGrpName, "heartbeat", "heartbeats"));

            this.joinLatency = metrics.sensor("join-latency");
            this.joinLatency.add(metrics.metricName("join-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-time-max",
                    this.metricGrpName,
                    "The max time taken for a group rejoin"), new Max());
            this.joinLatency.add(createMeter(metrics, metricGrpName, "join", "group joins"));


            this.syncLatency = metrics.sensor("sync-latency");
            this.syncLatency.add(metrics.metricName("sync-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-time-max",
                    this.metricGrpName,
                    "The max time taken for a group sync"), new Max());
            this.syncLatency.add(createMeter(metrics, metricGrpName, "sync", "group syncs"));

            Measurable lastHeartbeat =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
                    }
                };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last controller heartbeat was sent"),
                lastHeartbeat);
        }
    }

    private class HeartbeatThread extends KafkaThread {
        private boolean enabled = false;
        private boolean closed = false;
        private AtomicReference<RuntimeException> failed = new AtomicReference<>(null);

        private HeartbeatThread() {
            super(HEARTBEAT_THREAD_PREFIX + (groupId.isEmpty() ? "" : " | " + groupId), true);
        }

        public void enable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Enabling heartbeat thread");
                this.enabled = true;
                heartbeat.resetTimeouts(time.milliseconds());
                AbstractCoordinator.this.notify();
            }
        }

        public void disable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Disabling heartbeat thread");
                this.enabled = false;
            }
        }

        public void close() {
            synchronized (AbstractCoordinator.this) {
                this.closed = true;
                AbstractCoordinator.this.notify();
            }
        }

        private boolean hasFailed() {
            return failed.get() != null;
        }

        private RuntimeException failureCause() {
            return failed.get();
        }

        @Override
        public void run() {
            try {
                log.debug("Heartbeat thread started");
                while (true) {
                    synchronized (AbstractCoordinator.this) {
                        if (closed)
                            return;

                        if (!enabled) {
                            AbstractCoordinator.this.wait();
                            continue;
                        }

                        if (state != MemberState.STABLE) {
                            // the group is not stable (perhaps because we left the group or because the coordinator
                            // kicked us out), so disable heartbeats and wait for the main thread to rejoin.
                            disable();
                            continue;
                        }

                        client.pollNoWakeup();
                        long now = time.milliseconds();

                        if (coordinatorUnknown()) {
                            if (findCoordinatorFuture != null || lookupCoordinator().failed())
                                // the immediate future check ensures that we backoff properly in the case that no
                                // brokers are available to connect to.
                                AbstractCoordinator.this.wait(retryBackoffMs);
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            // the session timeout has expired without seeing a successful heartbeat, so we should
                            // probably make sure the coordinator is still healthy.
                            coordinatorDead();
                        } else if (heartbeat.pollTimeoutExpired(now)) {
                            // the poll timeout has expired, which means that the foreground thread has stalled
                            // in between calls to poll(), so we explicitly leave the group.
                            maybeLeaveGroup();
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            // poll again after waiting for the retry backoff in case the heartbeat failed or the
                            // coordinator disconnected
                            AbstractCoordinator.this.wait(retryBackoffMs);
                        } else {
                            heartbeat.sentHeartbeat(now);

                            sendHeartbeatRequest().addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {
                                    synchronized (AbstractCoordinator.this) {
                                        heartbeat.receiveHeartbeat(time.milliseconds());
                                    }
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    synchronized (AbstractCoordinator.this) {
                                        if (e instanceof RebalanceInProgressException) {
                                            // it is valid to continue heartbeating while the group is rebalancing. This
                                            // ensures that the coordinator keeps the member in the group for as long
                                            // as the duration of the rebalance timeout. If we stop sending heartbeats,
                                            // however, then the session timeout may expire before we can rejoin.
                                            heartbeat.receiveHeartbeat(time.milliseconds());
                                        } else {
                                            heartbeat.failHeartbeat();

                                            // wake up the thread if it's sleeping to reschedule the heartbeat
                                            AbstractCoordinator.this.notify();
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            } catch (AuthenticationException e) {
                log.error("An authentication error occurred in the heartbeat thread", e);
                this.failed.set(e);
            } catch (GroupAuthorizationException e) {
                log.error("A group authorization error occurred in the heartbeat thread for group {}", groupId, e);
                this.failed.set(e);
            } catch (InterruptedException | InterruptException e) {
                Thread.interrupted();
                log.error("Unexpected interrupt received in heartbeat thread for group {}", groupId, e);
                this.failed.set(new RuntimeException(e));
            } catch (RuntimeException e) {
                log.error("Heartbeat thread for group {} failed due to unexpected error", groupId, e);
                this.failed.set(e);
            } finally {
                log.debug("Heartbeat thread has closed");
            }
        }

    }

    protected static class Generation {
        public static final Generation NO_GENERATION = new Generation(
                OffsetCommitRequest.DEFAULT_GENERATION_ID,
                JoinGroupRequest.UNKNOWN_MEMBER_ID,
                null);

        public final int generationId;
        public final String memberId;
        public final String protocol;

        public Generation(int generationId, String memberId, String protocol) {
            this.generationId = generationId;
            this.memberId = memberId;
            this.protocol = protocol;
        }
    }

    private static class UnjoinedGroupException extends RetriableException {

    }

}
