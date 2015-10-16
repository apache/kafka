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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
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
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.GroupMetadataRequest;
import org.apache.kafka.common.requests.GroupMetadataResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
 * by the leader in {@link #doSync(String, String, Map)} and becomes available to members in
 * {@link #onJoin(int, String, String, ByteBuffer)}.
 *
 */
public abstract class AbstractCoordinator {

    private static final Logger log = LoggerFactory.getLogger(AbstractCoordinator.class);

    private final Heartbeat heartbeat;
    private final HeartbeatTask heartbeatTask;
    private final int sessionTimeoutMs;
    private final GroupCoordinatorMetrics sensors;
    protected final String groupId;
    protected final ConsumerNetworkClient client;
    protected final Time time;
    protected final long retryBackoffMs;
    protected final long requestTimeoutMs;

    private boolean rejoinNeeded = true;
    protected Node coordinator;
    protected String memberId;
    protected String protocol;
    protected int generation;

    /**
     * Initialize the coordination manager.
     */
    public AbstractCoordinator(ConsumerNetworkClient client,
                               String groupId,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Map<String, String> metricTags,
                               Time time,
                               long requestTimeoutMs,
                               long retryBackoffMs) {
        this.client = client;
        this.time = time;
        this.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID;
        this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        this.groupId = groupId;
        this.coordinator = null;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.heartbeat = new Heartbeat(this.sessionTimeoutMs, heartbeatIntervalMs, time.milliseconds());
        this.heartbeatTask = new HeartbeatTask();
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix, metricTags);
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Unique identifier for the class of protocols implements (e.g. "consumer" or "copycat").
     * @return Non-null protocol type namej
     */
    protected abstract String protocolType();

    /**
     * Get the current list of protocols and their associated metadata supported
     * by the local member. The order of the protocols in the map indicates the preference
     * of the protocol (the first entry is the most preferred). The coordinator takes this
     * preference into account when selecting the generation protocol (generally more preferred
     * protocols will be selected as long as all members support them and there is no disagreement
     * on the preference).
     * @return Non-empty map of supported protocols and metadata
     */
    protected abstract LinkedHashMap<String, ByteBuffer> metadata();

    /**
     * Invoked when a group member has successfully joined a group.
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param protocol The protocol selected by the coordinator
     * @param memberAssignment The assignment propagated from the group leader
     */
    protected abstract void onJoin(int generation,
                                   String memberId,
                                   String protocol,
                                   ByteBuffer memberAssignment);

    /**
     * Perform synchronization for the group. This is used by the leader to push state to all the members
     * of the group (e.g. to push partition assignments in the case of the new consumer)
     * @param leaderId The id of the leader (which is this member)
     * @param allMemberMetadata Metadata from all members of the group
     * @return A map from each member to their state assignment
     */
    protected abstract Map<String, ByteBuffer> doSync(String leaderId,
                                                      String protocol,
                                                      Map<String, ByteBuffer> allMemberMetadata);

    /**
     * Invoked when the group is left (whether because of shutdown, metadata change, stale generation, etc.)
     * @param generation The generation that was left
     * @param memberId The identifier of the local member in the group
     */
    protected abstract void onLeave(int generation, String memberId);


    /**
     * Block until the coordinator for this group is known.
     */
    public void ensureCoordinatorKnown() {
        while (coordinatorUnknown()) {
            RequestFuture<Void> future = sendGroupMetadataRequest();
            client.poll(future, requestTimeoutMs);

            if (future.failed())
                client.awaitMetadataUpdate();
        }
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes)
     * @return true if it should, false otherwise
     */
    protected boolean needRejoin() {
        return rejoinNeeded;
    }

    /**
     * Reset the generation/memberId tracked by this member
     */
    public void resetGeneration() {
        this.generation = OffsetCommitRequest.DEFAULT_GENERATION_ID;
        this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        rejoinNeeded = true;
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     */
    public void ensureActiveGroup() {
        if (!needRejoin())
            return;

        // onLeave only invoked if we have a valid current generation
        onLeave(generation, memberId);

        while (needRejoin()) {
            ensureCoordinatorKnown();

            // ensure that there are no pending requests to the coordinator. This is important
            // in particular to avoid resending a pending JoinGroup request.
            if (client.pendingRequestCount(this.coordinator) > 0) {
                client.awaitPendingRequests(this.coordinator);
                continue;
            }

            RequestFuture<ByteBuffer> future = sendJoinGroupRequest();
            client.poll(future);

            if (future.succeeded()) {
                onJoin(generation, memberId, protocol, future.value());
                heartbeatTask.reset();
            } else {
                if (future.exception() instanceof UnknownMemberIdException)
                    continue;
                else if (!future.isRetriable())
                    throw future.exception();
                Utils.sleep(retryBackoffMs);
            }
        }
    }

    private class HeartbeatTask implements DelayedTask {

        public void reset() {
            // start or restart the heartbeat task to be executed at the next chance
            long now = time.milliseconds();
            heartbeat.resetSessionTimeout(now);
            client.unschedule(this);
            client.schedule(this, now);
        }

        @Override
        public void run(final long now) {
            if (generation < 0 || needRejoin() || coordinatorUnknown()) {
                // no need to send the heartbeat we're not using auto-assignment or if we are
                // awaiting a rebalance
                return;
            }

            if (heartbeat.sessionTimeoutExpired(now)) {
                // we haven't received a successful heartbeat in one session interval
                // so mark the coordinator dead
                coordinatorDead();
                return;
            }

            if (!heartbeat.shouldHeartbeat(now)) {
                // we don't need to heartbeat now, so reschedule for when we do
                client.schedule(this, now + heartbeat.timeToNextHeartbeat(now));
            } else {
                heartbeat.sentHeartbeat(now);
                RequestFuture<Void> future = sendHeartbeatRequest();
                future.addListener(new RequestFutureListener<Void>() {
                    @Override
                    public void onSuccess(Void value) {
                        long now = time.milliseconds();
                        heartbeat.receiveHeartbeat(now);
                        long nextHeartbeatTime = now + heartbeat.timeToNextHeartbeat(now);
                        client.schedule(HeartbeatTask.this, nextHeartbeatTime);
                    }

                    @Override
                    public void onFailure(RuntimeException e) {
                        client.schedule(HeartbeatTask.this, time.milliseconds() + retryBackoffMs);
                    }
                });
            }
        }
    }

    /**
     * Send a request to get a new partition assignment. This is a non-blocking call which sends
     * a JoinGroup request to the coordinator (if it is available). The returned future must
     * be polled to see if the request completed successfully.
     * @return A request future whose completion indicates the result of the JoinGroup request.
     */
    private RequestFuture<ByteBuffer> sendJoinGroupRequest() {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // send a join group request to the coordinator
        log.debug("(Re-)joining group {}", groupId);

        List<JoinGroupRequest.GroupProtocol> protocols = new ArrayList<>();
        for (Map.Entry<String, ByteBuffer> metadataEntry : metadata().entrySet())
            protocols.add(new JoinGroupRequest.GroupProtocol(metadataEntry.getKey(), metadataEntry.getValue()));

        JoinGroupRequest request = new JoinGroupRequest(
                groupId,
                this.sessionTimeoutMs,
                this.memberId,
                protocolType(),
                protocols);

        // create the request for the coordinator
        log.debug("Issuing request ({}: {}) to coordinator {}", ApiKeys.JOIN_GROUP, request, this.coordinator.id());
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
            // process the response
            short errorCode = joinResponse.errorCode();
            if (errorCode == Errors.NONE.code()) {
                log.debug("Joined group: {}", joinResponse.toStruct());
                AbstractCoordinator.this.memberId = joinResponse.memberId();
                AbstractCoordinator.this.generation = joinResponse.generationId();
                AbstractCoordinator.this.rejoinNeeded = false;
                AbstractCoordinator.this.protocol = joinResponse.groupProtocol();
                sensors.joinLatency.record(response.requestLatencyMs());
                performSync(joinResponse).chain(future);
            } else if (errorCode == Errors.UNKNOWN_MEMBER_ID.code()) {
                // reset the member id and retry immediately
                AbstractCoordinator.this.memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                log.info("Attempt to join group {} failed due to unknown member id, resetting and retrying.",
                        groupId);
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (errorCode == Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code()
                    || errorCode == Errors.NOT_COORDINATOR_FOR_GROUP.code()) {
                // re-discover the coordinator and retry with backoff
                coordinatorDead();
                log.info("Attempt to join group {} failed due to obsolete coordinator information, retrying.",
                        groupId);
                future.raise(Errors.forCode(errorCode));
            } else if (errorCode == Errors.INCONSISTENT_GROUP_PROTOCOL.code()
                    || errorCode == Errors.INVALID_SESSION_TIMEOUT.code()) {
                // log the error and re-throw the exception
                Errors error = Errors.forCode(errorCode);
                log.error("Attempt to join group {} failed due to: {}",
                        groupId, error.exception().getMessage());
                future.raise(error);
            } else {
                // unexpected error, throw the exception
                future.raise(new KafkaException("Unexpected error in join group response: "
                        + Errors.forCode(joinResponse.errorCode()).exception().getMessage()));
            }
        }
    }

    private RequestFuture<ByteBuffer> performSync(JoinGroupResponse joinResponse) {
        if (joinResponse.isLeader()) {
            try {
                // perform the leader synchronization and send back the assignment for the group
                Map<String, ByteBuffer> groupAssignment = doSync(joinResponse.leaderId(), joinResponse.groupProtocol(),
                        joinResponse.members());

                SyncGroupRequest request = new SyncGroupRequest(groupId, generation, memberId, groupAssignment);
                log.debug("Issuing leader SyncGroup ({}: {}) to coordinator {}", ApiKeys.SYNC_GROUP, request, this.coordinator.id());
                return sendSyncGroupRequest(request);
            } catch (RuntimeException e) {
                return RequestFuture.failure(e);
            }
        } else {
            // send follower's sync group with an empty assignment
            SyncGroupRequest request = new SyncGroupRequest(groupId, generation,
                    memberId, Collections.<String, ByteBuffer>emptyMap());
            log.debug("Issuing follower SyncGroup ({}: {}) to coordinator {}", ApiKeys.SYNC_GROUP, request, this.coordinator.id());
            return sendSyncGroupRequest(request);
        }
    }

    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest request) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();
        return client.send(coordinator, ApiKeys.SYNC_GROUP, request)
                .compose(new SyncGroupRequestHandler());
    }

    private class SyncGroupRequestHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {

        @Override
        public SyncGroupResponse parse(ClientResponse response) {
            return new SyncGroupResponse(response.responseBody());
        }

        @Override
        public void handle(SyncGroupResponse syncResponse,
                           RequestFuture<ByteBuffer> future) {
            short errorCode = syncResponse.errorCode();
            if (errorCode == Errors.NONE.code()) {
                try {
                    future.complete(syncResponse.memberAssignment());
                    sensors.syncLatency.record(response.requestLatencyMs());
                } catch (SchemaException e) {
                    future.raise(e);
                }
            } else {
                AbstractCoordinator.this.rejoinNeeded = true;
                future.raise(Errors.forCode(errorCode));
            }
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendGroupMetadataRequest() {
        // initiate the group metadata request
        // find a node to ask about the coordinator
        Node node = this.client.leastLoadedNode();
        if (node == null) {
            // TODO: If there are no brokers left, perhaps we should use the bootstrap set
            // from configuration?
            return RequestFuture.noBrokersAvailable();
        } else {
            // create a group  metadata request
            log.debug("Issuing group metadata request to broker {}", node.id());
            GroupMetadataRequest metadataRequest = new GroupMetadataRequest(this.groupId);
            return client.send(node, ApiKeys.GROUP_METADATA, metadataRequest)
                    .compose(new RequestFutureAdapter<ClientResponse, Void>() {
                        @Override
                        public void onSuccess(ClientResponse response, RequestFuture<Void> future) {
                            handleGroupMetadataResponse(response, future);
                        }
                    });
        }
    }

    private void handleGroupMetadataResponse(ClientResponse resp, RequestFuture<Void> future) {
        log.debug("Group metadata response {}", resp);

        // parse the response to get the coordinator info if it is not disconnected,
        // otherwise we need to request metadata update
        if (resp.wasDisconnected()) {
            future.raise(new DisconnectException());
        } else if (!coordinatorUnknown()) {
            // We already found the coordinator, so ignore the request
            future.complete(null);
        } else {
            GroupMetadataResponse groupMetadataResponse = new GroupMetadataResponse(resp.responseBody());
            // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
            // for the coordinator in the underlying network client layer
            // TODO: this needs to be better handled in KAFKA-1935
            if (groupMetadataResponse.errorCode() == Errors.NONE.code()) {
                this.coordinator = new Node(Integer.MAX_VALUE - groupMetadataResponse.node().id(),
                        groupMetadataResponse.node().host(),
                        groupMetadataResponse.node().port());

                // start sending heartbeats only if we have a valid generation
                if (generation > 0)
                    heartbeatTask.reset();
                future.complete(null);
            } else {
                future.raise(Errors.forCode(groupMetadataResponse.errorCode()));
            }
        }
    }

    /**
     * Check if we know who the coordinator is.
     * @return true if the coordinator is unknown
     */
    public boolean coordinatorUnknown() {
        return this.coordinator == null;
    }


    /**
     * Mark the current coordinator as dead.
     */
    protected void coordinatorDead() {
        if (this.coordinator != null) {
            log.info("Marking the coordinator {} dead.", this.coordinator.id());
            this.coordinator = null;
        }
    }

    /**
     * Send a heartbeat request now (visible only for testing).
     */
    public RequestFuture<Void> sendHeartbeatRequest() {
        HeartbeatRequest req = new HeartbeatRequest(this.groupId, this.generation, this.memberId);
        return client.send(coordinator, ApiKeys.HEARTBEAT, req)
                .compose(new HeartbeatCompletionHandler());
    }

    private class HeartbeatCompletionHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        @Override
        public HeartbeatResponse parse(ClientResponse response) {
            return new HeartbeatResponse(response.responseBody());
        }

        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatLatency.record(response.requestLatencyMs());
            short error = heartbeatResponse.errorCode();
            if (error == Errors.NONE.code()) {
                log.debug("Received successful heartbeat response.");
                future.complete(null);
            } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code()
                    || error == Errors.NOT_COORDINATOR_FOR_GROUP.code()) {
                log.info("Attempt to heart beat failed since coordinator is either not started or not valid, marking it as dead.");
                coordinatorDead();
                future.raise(Errors.forCode(error));
            } else if (error == Errors.REBALANCE_IN_PROGRESS.code()) {
                log.info("Attempt to heart beat failed since the group is rebalancing, try to re-join group.");
                AbstractCoordinator.this.rejoinNeeded = true;
                future.raise(Errors.REBALANCE_IN_PROGRESS);
            } else if (error == Errors.ILLEGAL_GENERATION.code()) {
                log.info("Attempt to heart beat failed since generation id is not legal, try to re-join group.");
                AbstractCoordinator.this.rejoinNeeded = true;
                future.raise(Errors.ILLEGAL_GENERATION);
            } else if (error == Errors.UNKNOWN_MEMBER_ID.code()) {
                log.info("Attempt to heart beat failed since member id is not valid, reset it and try to re-join group.");
                memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
                AbstractCoordinator.this.rejoinNeeded = true;
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: "
                        + Errors.forCode(error).exception().getMessage()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T>
            extends RequestFutureAdapter<ClientResponse, T> {
        protected ClientResponse response;

        public abstract R parse(ClientResponse response);

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            this.response = clientResponse;

            if (clientResponse.wasDisconnected()) {
                int correlation = response.request().request().header().correlationId();
                log.debug("Cancelled request {} with correlation id {} due to coordinator {} being disconnected",
                        response.request(),
                        correlation,
                        response.request().request().destination());

                // mark the coordinator as dead
                coordinatorDead();
                future.raise(new DisconnectException());
                return;
            }

            R response = parse(clientResponse);
            handle(response, future);
        }

    }

    private class GroupCoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor heartbeatLatency;
        public final Sensor joinLatency;
        public final Sensor syncLatency;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> tags) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(new MetricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a heartbeat request",
                tags), new Max());
            this.heartbeatLatency.add(new MetricName("heartbeat-rate",
                this.metricGrpName,
                "The average number of heartbeats per second",
                tags), new Rate(new Count()));

            this.joinLatency = metrics.sensor("join-latency");
            this.joinLatency.add(new MetricName("join-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group rejoin",
                    tags), new Avg());
            this.joinLatency.add(new MetricName("join-time-max",
                    this.metricGrpName,
                    "The max time taken for a group rejoin",
                    tags), new Avg());
            this.joinLatency.add(new MetricName("join-rate",
                    this.metricGrpName,
                    "The number of group joins per second",
                    tags), new Rate(new Count()));

            this.syncLatency = metrics.sensor("sync-latency");
            this.syncLatency.add(new MetricName("sync-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group sync",
                    tags), new Avg());
            this.syncLatency.add(new MetricName("sync-time-max",
                    this.metricGrpName,
                    "The max time taken for a group sync",
                    tags), new Avg());
            this.syncLatency.add(new MetricName("sync-rate",
                    this.metricGrpName,
                    "The number of group syncs per second",
                    tags), new Rate(new Count()));

            Measurable lastHeartbeat =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
                    }
                };
            metrics.addMetric(new MetricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last controller heartbeat",
                tags),
                lastHeartbeat);
        }
    }

}
