/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupCoordinatorNotAvailableException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.GroupCoordinatorRequest;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
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
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 */
public abstract class AbstractCoordinator implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractCoordinator.class);

    private enum MemberState {
        UNJOINED,    // the client is not part of a group
        REBALANCING, // the client has begun rebalancing
        STABLE,      // the client has joined and is sending heartbeats
        LEAVING,     // the client has sent a leave group request and is awaiting its return
    }

    private final int rebalanceTimeoutMs;
    private final int sessionTimeoutMs;
    private final GroupCoordinatorMetrics sensors;
    protected final String groupId;
    protected final ConsumerNetworkClient client;
    protected final Time time;
    protected final long retryBackoffMs;
    private final Heartbeat heartbeat;

    private HeartbeatThread heartbeatThread = null;
    private boolean rejoinNeeded = true;
    private boolean needsJoinPrepare = true;
    private MemberState state = MemberState.UNJOINED;
    private RequestFuture<ByteBuffer> joinFuture = null;
    protected Node coordinator = null;
    protected String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;;
    protected String protocol = null;
    protected int generation = OffsetCommitRequest.DEFAULT_GENERATION_ID;

    private RequestFuture<Void> findCoordinatorFuture = null;

    /**
     * Initialize the coordination manager.
     */
    public AbstractCoordinator(ConsumerNetworkClient client,
                               String groupId,
                               int rebalanceTimeoutMs,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs) {
        this.client = client;
        this.time = time;
        this.groupId = groupId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.sessionTimeoutMs = sessionTimeoutMs;
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
     * Invoked when a group member has successfully joined a group.
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
        while (coordinatorUnknown()) {
            RequestFuture<Void> future = lookupCoordinator();
            client.poll(future);

            if (future.failed()) {
                if (future.isRetriable())
                    client.awaitMetadataUpdate();
                else
                    throw future.exception();
            } else if (coordinator != null && client.connectionFailed(coordinator)) {
                // we found the coordinator, but the connection has failed, so mark
                // it dead and backoff before retrying discovery
                coordinatorDead();
                time.sleep(retryBackoffMs);
            }
        }
    }

    protected RequestFuture<Void> lookupCoordinator() {
        if (findCoordinatorFuture == null) {
            findCoordinatorFuture = sendGroupCoordinatorRequest();
            findCoordinatorFuture.addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    findCoordinatorFuture = null;
                }

                @Override
                public void onFailure(RuntimeException e) {
                    findCoordinatorFuture = null;
                }
            });
        }
        return findCoordinatorFuture;
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes)
     * @return true if it should, false otherwise
     */
    protected synchronized boolean needRejoin() {
        return rejoinNeeded;
    }

    /**
     * Ensure that the heartbeat thread is active and
     * @param now
     */
    protected synchronized void pollHeartbeat(long now) {
        if (heartbeatThread != null) {
            if (heartbeatThread.hasFailed())
                throw heartbeatThread.failureCause();

            heartbeat.poll(now);
        }
    }


    /**
     * Ensure that the group is active (i.e. joined and synced)
     */
    public synchronized void ensureActiveGroup() {
        // always ensure that the coordinator is ready because we may have been disconnected
        // when sending heartbeats and does not necessarily require us to rejoin the group.
        ensureCoordinatorReady();

        if (!needRejoin())
            return;

        if (needsJoinPrepare) {
            onJoinPrepare(generation, memberId);
            needsJoinPrepare = false;
        }

        if (heartbeatThread == null) {
            heartbeatThread = new HeartbeatThread();
            heartbeatThread.start();
        }

        while (needRejoin()) {
            ensureCoordinatorReady();

            // ensure that there are no pending requests to the coordinator. This is important
            // in particular to avoid resending a pending JoinGroup request.
            if (state == MemberState.LEAVING || client.pendingRequestCount(this.coordinator) > 0) {
                client.awaitPendingRequests(this.coordinator);
                continue;
            }

            if (joinFuture == null) {
                state = MemberState.REBALANCING;
                joinFuture = sendJoinGroupRequest();
                joinFuture.addListener(new RequestFutureListener<ByteBuffer>() {
                    @Override
                    public void onSuccess(ByteBuffer value) {
                        // handle join completion in the callback so that the callback will be invoked
                        // even if the consumer is woken up before finishing the rebalance
                        joinFuture = null;
                        state = MemberState.STABLE;
                        needsJoinPrepare = true;
                        heartbeatThread.enable();
                        onJoinComplete(generation, memberId, protocol, value);
                    }

                    @Override
                    public void onFailure(RuntimeException e) {
                        // we handle failures below after the request finishes. if the join completes
                        // after having been woken up, the exception is ignored and we will rejoin
                        joinFuture = null;
                        state = MemberState.UNJOINED;
                    }
                });
            }

            RequestFuture<ByteBuffer> future = joinFuture;
            client.poll(future);

            if (future.failed()) {
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
        log.info("(Re-)joining group {}", groupId);
        JoinGroupRequest request = new JoinGroupRequest(
                groupId,
                this.sessionTimeoutMs,
                this.rebalanceTimeoutMs,
                this.memberId,
                protocolType(),
                metadata());

        log.debug("Sending JoinGroup ({}) to coordinator {}", request, this.coordinator);
        return client.send(coordinator, ApiKeys.JOIN_GROUP, request)
                .compose(new JoinGroupResponseHandler());
    }


    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {

        @Override
        public JoinGroupResponse parse(ClientResponse response) {
            return new JoinGroupResponse(response.responseBody());
        }

        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
            synchronized (AbstractCoordinator.this) {
                Errors error = Errors.forCode(joinResponse.errorCode());
                if (error == Errors.NONE) {
                    log.debug("Received successful join group response for group {}: {}", groupId, joinResponse.toStruct());
                    AbstractCoordinator.this.memberId = joinResponse.memberId();
                    AbstractCoordinator.this.generation = joinResponse.generationId();
                    AbstractCoordinator.this.rejoinNeeded = false;
                    AbstractCoordinator.this.protocol = joinResponse.groupProtocol();
                    sensors.joinLatency.record(response.requestLatencyMs());
                    if (joinResponse.isLeader()) {
                        onJoinLeader(joinResponse).chain(future);
                    } else {
                        onJoinFollower().chain(future);
                    }
                } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                    log.debug("Attempt to join group {} rejected since coordinator {} is loading the group.", groupId,
                            coordinator);
                    // backoff and retry
                    future.raise(error);
                } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                    // reset the member id and retry immediately
                    AbstractCoordinator.this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                    log.debug("Attempt to join group {} failed due to unknown member id.", groupId);
                    future.raise(Errors.UNKNOWN_MEMBER_ID);
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                    // re-discover the coordinator and retry with backoff
                    coordinatorDead();
                    log.debug("Attempt to join group {} failed due to obsolete coordinator information: {}", groupId, error.message());
                    future.raise(error);
                } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL
                        || error == Errors.INVALID_SESSION_TIMEOUT
                        || error == Errors.INVALID_GROUP_ID) {
                    // log the error and re-throw the exception
                    log.error("Attempt to join group {} failed due to fatal error: {}", groupId, error.message());
                    future.raise(error);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(new GroupAuthorizationException(groupId));
                } else {
                    // unexpected error, throw the exception
                    future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
                }
            }
        }
    }

    private RequestFuture<ByteBuffer> onJoinFollower() {
        // send follower's sync group with an empty assignment
        SyncGroupRequest request = new SyncGroupRequest(groupId, generation,
                memberId, Collections.<String, ByteBuffer>emptyMap());
        log.debug("Sending follower SyncGroup for group {} to coordinator {}: {}", groupId, this.coordinator, request);
        return sendSyncGroupRequest(request);
    }

    private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
        try {
            // perform the leader synchronization and send back the assignment for the group
            Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.leaderId(), joinResponse.groupProtocol(),
                    joinResponse.members());

            SyncGroupRequest request = new SyncGroupRequest(groupId, generation, memberId, groupAssignment);
            log.debug("Sending leader SyncGroup for group {} to coordinator {}: {}", groupId, this.coordinator, request);
            return sendSyncGroupRequest(request);
        } catch (RuntimeException e) {
            return RequestFuture.failure(e);
        }
    }

    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest request) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();
        return client.send(coordinator, ApiKeys.SYNC_GROUP, request)
                .compose(new SyncGroupResponseHandler());
    }

    private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {

        @Override
        public SyncGroupResponse parse(ClientResponse response) {
            return new SyncGroupResponse(response.responseBody());
        }

        @Override
        public void handle(SyncGroupResponse syncResponse,
                           RequestFuture<ByteBuffer> future) {
            synchronized (AbstractCoordinator.this) {
                Errors error = Errors.forCode(syncResponse.errorCode());
                if (error == Errors.NONE) {
                    log.info("Successfully joined group {} with generation {}", groupId, generation);
                    sensors.syncLatency.record(response.requestLatencyMs());
                    future.complete(syncResponse.memberAssignment());
                } else {
                    AbstractCoordinator.this.rejoinNeeded = true;
                    if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                        future.raise(new GroupAuthorizationException(groupId));
                    } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                        log.debug("SyncGroup for group {} failed due to coordinator rebalance", groupId);
                        future.raise(error);
                    } else if (error == Errors.UNKNOWN_MEMBER_ID
                            || error == Errors.ILLEGAL_GENERATION) {
                        log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                        AbstractCoordinator.this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                        future.raise(error);
                    } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                            || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                        log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                        coordinatorDead();
                        future.raise(error);
                    } else {
                        future.raise(new KafkaException("Unexpected error from SyncGroup: " + error.message()));
                    }
                }
            }
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendGroupCoordinatorRequest() {
        // initiate the group metadata request
        // find a node to ask about the coordinator
        Node node = this.client.leastLoadedNode();
        if (node == null) {
            // TODO: If there are no brokers left, perhaps we should use the bootstrap set
            // from configuration?
            return RequestFuture.noBrokersAvailable();
        } else {
            // create a group  metadata request
            log.debug("Sending coordinator request for group {} to broker {}", groupId, node);
            GroupCoordinatorRequest metadataRequest = new GroupCoordinatorRequest(this.groupId);
            return client.send(node, ApiKeys.GROUP_COORDINATOR, metadataRequest)
                    .compose(new GroupCoordinatorResponseHandler());
        }
    }

    private class GroupCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {

        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            synchronized (AbstractCoordinator.this) {
                log.debug("Received group coordinator response {}", resp);

                GroupCoordinatorResponse groupCoordinatorResponse = new GroupCoordinatorResponse(resp.responseBody());
                // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
                // for the coordinator in the underlying network client layer
                // TODO: this needs to be better handled in KAFKA-1935
                Errors error = Errors.forCode(groupCoordinatorResponse.errorCode());
                if (error == Errors.NONE) {
                    AbstractCoordinator.this.coordinator = new Node(Integer.MAX_VALUE - groupCoordinatorResponse.node().id(),
                            groupCoordinatorResponse.node().host(),
                            groupCoordinatorResponse.node().port());

                    log.info("Discovered coordinator {} for group {}.", coordinator, groupId);
                    client.tryConnect(coordinator);
                    future.complete(null);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(new GroupAuthorizationException(groupId));
                } else {
                    future.raise(error);
                }
            }
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     * @return true if the coordinator is unknown
     */
    public synchronized boolean coordinatorUnknown() {
        if (coordinator == null)
            return true;

        if (client.connectionFailed(coordinator)) {
            coordinatorDead();
            return true;
        }

        return false;
    }

    /**
     * Mark the current coordinator as dead.
     */
    protected synchronized void coordinatorDead() {
        if (this.coordinator != null) {
            log.info("Marking the coordinator {} dead for group {}", this.coordinator, groupId);
            client.failUnsentRequests(this.coordinator, GroupCoordinatorNotAvailableException.INSTANCE);
            this.coordinator = null;
        }
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     */
    @Override
    public synchronized void close() {
        if (heartbeatThread != null)
            heartbeatThread.close();

        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();
        maybeLeaveGroup();
    }

    /**
     * Leave the current group and reset local generation/memberId.
     */
    public synchronized void maybeLeaveGroup() {
        if (!coordinatorUnknown() && state == MemberState.STABLE) {
            // this is a minimal effort attempt to leave the group. we do not
            // attempt any resending if the request fails or times out.
            sendLeaveGroupRequest();
        }

        this.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID;
        this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        rejoinNeeded = true;
    }

    private void sendLeaveGroupRequest() {
        state = MemberState.LEAVING;

        LeaveGroupRequest request = new LeaveGroupRequest(groupId, memberId);
        RequestFuture<Void> future = client.send(coordinator, ApiKeys.LEAVE_GROUP, request)
                .compose(new LeaveGroupResponseHandler());

        client.poll(future, 0);
    }

    private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {

        @Override
        public LeaveGroupResponse parse(ClientResponse response) {
            return new LeaveGroupResponse(response.responseBody());
        }

        @Override
        public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
            // process the response
            synchronized (AbstractCoordinator.this) {
                state = MemberState.UNJOINED;

                Errors error = Errors.forCode(leaveResponse.errorCode());
                if (error == Errors.NONE)
                    future.complete(null);
                else {
                    log.debug("LeaveGroup request for group {} failed with error: {}", groupId, error.message());
                    future.raise(error);
                }
            }
        }
    }

    /**
     * Send a heartbeat request now (visible only for testing).
     */
    protected synchronized RequestFuture<Void> sendHeartbeatRequest() {
        HeartbeatRequest req = new HeartbeatRequest(this.groupId, this.generation, this.memberId);
        return client.send(coordinator, ApiKeys.HEARTBEAT, req)
                .compose(new HeartbeatResponseHandler());
    }

    private class HeartbeatResponseHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {

        @Override
        public HeartbeatResponse parse(ClientResponse response) {
            return new HeartbeatResponse(response.responseBody());
        }

        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            synchronized (AbstractCoordinator.this) {
                sensors.heartbeatLatency.record(response.requestLatencyMs());
                Errors error = Errors.forCode(heartbeatResponse.errorCode());
                if (error == Errors.NONE) {
                    log.debug("Received successful heartbeat response for group {}", groupId);
                    future.complete(null);
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                    log.debug("Attempt to heart beat failed for group {} since coordinator {} is either not started or not valid.",
                            groupId, coordinator);
                    coordinatorDead();
                    future.raise(error);
                } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                    log.debug("Attempt to heart beat failed for group {} since it is rebalancing.", groupId);
                    AbstractCoordinator.this.rejoinNeeded = true;
                    future.raise(Errors.REBALANCE_IN_PROGRESS);
                } else if (error == Errors.ILLEGAL_GENERATION) {
                    log.debug("Attempt to heart beat failed for group {} since generation id is not legal.", groupId);
                    AbstractCoordinator.this.rejoinNeeded = true;
                    memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                    state = MemberState.UNJOINED;
                    future.raise(Errors.ILLEGAL_GENERATION);
                } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                    log.debug("Attempt to heart beat failed for group {} since member id is not valid.", groupId);
                    memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                    AbstractCoordinator.this.rejoinNeeded = true;
                    state = MemberState.UNJOINED;
                    future.raise(Errors.UNKNOWN_MEMBER_ID);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(new GroupAuthorizationException(groupId));
                } else {
                    future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
                }
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T> extends RequestFutureAdapter<ClientResponse, T> {
        protected ClientResponse response;

        public abstract R parse(ClientResponse response);

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException)
                coordinatorDead();
            future.raise(e);
        }

        @Override
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                R responseObj = parse(clientResponse);
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone())
                    future.raise(e);
            }
        }

    }

    private class GroupCoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor heartbeatLatency;
        public final Sensor joinLatency;
        public final Sensor syncLatency;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(metrics.metricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatLatency.add(metrics.metricName("heartbeat-rate",
                this.metricGrpName,
                "The average number of heartbeats per second"), new Rate(new Count()));

            this.joinLatency = metrics.sensor("join-latency");
            this.joinLatency.add(metrics.metricName("join-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-time-max",
                    this.metricGrpName,
                    "The max time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-rate",
                    this.metricGrpName,
                    "The number of group joins per second"), new Rate(new Count()));

            this.syncLatency = metrics.sensor("sync-latency");
            this.syncLatency.add(metrics.metricName("sync-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-time-max",
                    this.metricGrpName,
                    "The max time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-rate",
                    this.metricGrpName,
                    "The number of group syncs per second"), new Rate(new Count()));

            Measurable lastHeartbeat =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
                    }
                };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last controller heartbeat"),
                lastHeartbeat);
        }
    }

    private class HeartbeatThread extends Thread {
        private boolean enabled = false;
        private boolean closed = false;
        private RequestFuture<Void> findCoordinatorFuture;
        private AtomicReference<RuntimeException> failed = new AtomicReference<>(null);



        public void enable() {
            synchronized (AbstractCoordinator.this) {
                this.enabled = true;
                heartbeat.resetTimeouts(time.milliseconds());
                AbstractCoordinator.this.notify();
            }
        }

        public void disable() {
            synchronized (AbstractCoordinator.this) {
                this.enabled = false;
            }
        }

        public void close() {
            synchronized (AbstractCoordinator.this) {
                this.closed = true;
                AbstractCoordinator.this.notify();
            }
        }

        private void await() {
            synchronized (AbstractCoordinator.this) {
                try {
                    AbstractCoordinator.this.wait();
                } catch (InterruptedException e) {

                }
            }
        }

        private void awaitMs(long waitTimeMs) {
            synchronized (AbstractCoordinator.this) {
                try {
                    AbstractCoordinator.this.wait(waitTimeMs);
                } catch (InterruptedException e) {

                }
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
                while (true) {
                    synchronized (AbstractCoordinator.this) {
                        if (closed)
                            return;

                        if (!enabled) {
                            await();
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
                            if (findCoordinatorFuture != null && !findCoordinatorFuture.isDone()) {
                                awaitMs(retryBackoffMs);
                                continue;
                            }

                            findCoordinatorFuture = sendGroupCoordinatorRequest();
                            findCoordinatorFuture.addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {
                                    heartbeat.resetTimeouts(time.milliseconds());
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    log.debug("Group coordinator lookup for group {} failed", groupId, e);
                                }
                            });
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            // the session timeout has expired without seeing a successful heartbeat, so we should
                            // probably make sure the coordinator is still healthy.
                            coordinatorDead();
                        } else if (heartbeat.pollTimeoutExpired(now)) {
                            // the poll timeout has expired, which means that the foreground thread has stalled
                            // in between calls to poll(), so we explicitly leave the group.
                            maybeLeaveGroup();
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            awaitMs(heartbeat.timeToNextHeartbeat(now));
                        } else {
                            heartbeat.sentHeartbeat(now);
                            RequestFuture<Void> future = sendHeartbeatRequest();
                            future.addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {
                                    heartbeat.receiveHeartbeat(time.milliseconds());
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    if (e instanceof RebalanceInProgressException) {
                                        // it is valid to continue heartbeating while the group is rebalancing. This
                                        // ensures that the coordinator keeps the member in the group for as long
                                        // as the duration of the rebalance timeout. If we stop sending heartbeats,
                                        // however, then the session timeout may expire before we can rejoin.
                                        heartbeat.receiveHeartbeat(time.milliseconds());
                                    } else {
                                        heartbeat.failHeartbeat();

                                        // wake up the thread if it's sleeping to reschedule the heartbeat
                                        synchronized (AbstractCoordinator.this) {
                                            AbstractCoordinator.this.notify();
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            } catch (RuntimeException e) {
                log.error("Heartbeat thread for group {} failed due to unexpected error" , groupId, e);
                this.failed.set(e);
            }
        }

    }

}
