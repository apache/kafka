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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
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
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.ConsumerMetadataRequest;
import org.apache.kafka.common.requests.ConsumerMetadataResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This class manage the coordination process with the consumer coordinator.
 */
public final class Coordinator {

    private static final Logger log = LoggerFactory.getLogger(Coordinator.class);

    private final KafkaClient client;

    private final Time time;
    private final String groupId;
    private final Metadata metadata;
    private final Heartbeat heartbeat;
    private final long sessionTimeoutMs;
    private final String assignmentStrategy;
    private final SubscriptionState subscriptions;
    private final CoordinatorMetrics sensors;
    private final long retryBackoffMs;
    private Node consumerCoordinator;
    private String consumerId;
    private int generation;

    /**
     * Initialize the coordination manager.
     */
    public Coordinator(KafkaClient client,
                       String groupId,
                       long retryBackoffMs,
                       long sessionTimeoutMs,
                       String assignmentStrategy,
                       Metadata metadata,
                       SubscriptionState subscriptions,
                       Metrics metrics,
                       String metricGrpPrefix,
                       Map<String, String> metricTags,
                       Time time) {

        this.time = time;
        this.client = client;
        this.generation = -1;
        this.consumerId = "";
        this.groupId = groupId;
        this.metadata = metadata;
        this.consumerCoordinator = null;
        this.subscriptions = subscriptions;
        this.retryBackoffMs = retryBackoffMs;
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.assignmentStrategy = assignmentStrategy;
        this.heartbeat = new Heartbeat(this.sessionTimeoutMs, time.milliseconds());
        this.sensors = new CoordinatorMetrics(metrics, metricGrpPrefix, metricTags);
    }

    /**
     * Assign partitions for the subscribed topics.
     *
     * @param subscribedTopics The subscribed topics list
     * @param now The current time
     * @return The assigned partition info
     */
    public List<TopicPartition> assignPartitions(List<String> subscribedTopics, long now) {

        // send a join group request to the coordinator
        log.debug("(Re-)joining group {} with subscribed topics {}", groupId, subscribedTopics);

        JoinGroupRequest request = new JoinGroupRequest(groupId,
            (int) this.sessionTimeoutMs,
            subscribedTopics,
            this.consumerId,
            this.assignmentStrategy);
        ClientResponse resp = this.blockingCoordinatorRequest(ApiKeys.JOIN_GROUP, request.toStruct(), null, now);

        // process the response
        JoinGroupResponse response = new JoinGroupResponse(resp.responseBody());
        // TODO: needs to handle disconnects and errors, should not just throw exceptions
        Errors.forCode(response.errorCode()).maybeThrow();
        this.consumerId = response.consumerId();

        // set the flag to refresh last committed offsets
        this.subscriptions.needRefreshCommits();

        log.debug("Joined group: {}", response);

        // record re-assignment time
        this.sensors.partitionReassignments.record(time.milliseconds() - now);

        // return assigned partitions
        return response.assignedPartitions();
    }

    /**
     * Commit offsets for the specified list of topics and partitions.
     *
     * A non-blocking commit will attempt to commit offsets asychronously. No error will be thrown if the commit fails.
     * A blocking commit will wait for a response acknowledging the commit. In the event of an error it will retry until
     * the commit succeeds.
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @param blocking Control whether the commit is blocking
     * @param now The current time
     */
    public void commitOffsets(final Map<TopicPartition, Long> offsets, boolean blocking, long now) {
        if (!offsets.isEmpty()) {
            // create the offset commit request
            Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData;
            offsetData = new HashMap<TopicPartition, OffsetCommitRequest.PartitionData>(offsets.size());
            for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet())
                offsetData.put(entry.getKey(), new OffsetCommitRequest.PartitionData(entry.getValue(), ""));
            OffsetCommitRequest req = new OffsetCommitRequest(this.groupId,
                this.generation,
                this.consumerId,
                OffsetCommitRequest.DEFAULT_RETENTION_TIME,
                offsetData);

            // send request and possibly wait for response if it is blocking
            RequestCompletionHandler handler = new CommitOffsetCompletionHandler(offsets);

            if (blocking) {
                boolean done;
                do {
                    ClientResponse response = blockingCoordinatorRequest(ApiKeys.OFFSET_COMMIT, req.toStruct(), handler, now);

                    // check for errors
                    done = true;
                    OffsetCommitResponse commitResponse = new OffsetCommitResponse(response.responseBody());
                    for (short errorCode : commitResponse.responseData().values()) {
                        if (errorCode != Errors.NONE.code())
                            done = false;
                    }
                    if (!done) {
                        log.debug("Error in offset commit, backing off for {} ms before retrying again.",
                            this.retryBackoffMs);
                        Utils.sleep(this.retryBackoffMs);
                    }
                } while (!done);
            } else {
                this.client.send(initiateCoordinatorRequest(ApiKeys.OFFSET_COMMIT, req.toStruct(), handler, now));
            }
        }
    }

    /**
     * Fetch the committed offsets of the given set of partitions.
     *
     * @param partitions The list of partitions which need to ask for committed offsets
     * @param now The current time
     * @return The fetched offset values
     */
    public Map<TopicPartition, Long> fetchOffsets(Set<TopicPartition> partitions, long now) {
        log.debug("Fetching committed offsets for partitions: " + Utils.join(partitions, ", "));

        while (true) {
            // construct the request
            OffsetFetchRequest request = new OffsetFetchRequest(this.groupId, new ArrayList<TopicPartition>(partitions));

            // send the request and block on waiting for response
            ClientResponse resp = this.blockingCoordinatorRequest(ApiKeys.OFFSET_FETCH, request.toStruct(), null, now);

            // parse the response to get the offsets
            boolean offsetsReady = true;
            OffsetFetchResponse response = new OffsetFetchResponse(resp.responseBody());
            // TODO: needs to handle disconnects
            Map<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>(response.responseData().size());
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData data = entry.getValue();
                if (data.hasError()) {
                    log.debug("Error fetching offset for topic-partition {}: {}", tp, Errors.forCode(data.errorCode)
                        .exception()
                        .getMessage());
                    if (data.errorCode == Errors.OFFSET_LOAD_IN_PROGRESS.code()) {
                        // just retry
                        offsetsReady = false;
                        Utils.sleep(this.retryBackoffMs);
                    } else if (data.errorCode == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                        // re-discover the coordinator and retry
                        coordinatorDead();
                        offsetsReady = false;
                        Utils.sleep(this.retryBackoffMs);
                    } else if (data.errorCode == Errors.NO_OFFSETS_FETCHABLE.code()
                            || data.errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
                        // just ignore this partition
                        log.debug("No committed offset for partition " + tp);
                    } else {
                        throw new IllegalStateException("Unexpected error code " + data.errorCode + " while fetching offset");
                    }
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 seems to indicate no
                    // such offset known)
                    offsets.put(tp, data.offset);
                } else {
                    log.debug("No committed offset for partition " + tp);
                }
            }

            if (offsetsReady)
                return offsets;
        }
    }

    /**
     * Attempt to heartbeat the consumer coordinator if necessary, and check if the coordinator is still alive.
     *
     * @param now The current time
     */
    public void maybeHeartbeat(long now) {
        if (heartbeat.shouldHeartbeat(now)) {
            HeartbeatRequest req = new HeartbeatRequest(this.groupId, this.generation, this.consumerId);
            this.client.send(initiateCoordinatorRequest(ApiKeys.HEARTBEAT, req.toStruct(), new HeartbeatCompletionHandler(), now));
            this.heartbeat.sentHeartbeat(now);
        }
    }

    public boolean coordinatorUnknown() {
        return this.consumerCoordinator == null;
    }

    /**
     * Repeatedly attempt to send a request to the coordinator until a response is received (retry if we are
     * disconnected). Note that this means any requests sent this way must be idempotent.
     *
     * @return The response
     */
    private ClientResponse blockingCoordinatorRequest(ApiKeys api,
                                                      Struct request,
                                                      RequestCompletionHandler handler,
                                                      long now) {
        while (true) {
            ClientRequest coordinatorRequest = initiateCoordinatorRequest(api, request, handler, now);
            ClientResponse coordinatorResponse = sendAndReceive(coordinatorRequest, now);
            if (coordinatorResponse.wasDisconnected()) {
                handleCoordinatorDisconnect(coordinatorResponse);
                Utils.sleep(this.retryBackoffMs);
            } else {
                return coordinatorResponse;
            }
        }
    }

    /**
     * Ensure the consumer coordinator is known and we have a ready connection to it.
     */
    private void ensureCoordinatorReady() {
        while (true) {
            if (this.consumerCoordinator == null)
                discoverCoordinator();

            while (true) {
                boolean ready = this.client.ready(this.consumerCoordinator, time.milliseconds());
                if (ready) {
                    return;
                } else {
                    log.debug("No connection to coordinator, attempting to connect.");
                    this.client.poll(this.retryBackoffMs, time.milliseconds());

                    // if the coordinator connection has failed, we need to
                    // break the inner loop to re-discover the coordinator
                    if (this.client.connectionFailed(this.consumerCoordinator)) {
                        log.debug("Coordinator connection failed. Attempting to re-discover.");
                        coordinatorDead();
                        break;
                    }
                }
            }
        }
    }

    /**
     * Mark the current coordinator as dead.
     */
    private void coordinatorDead() {
        if (this.consumerCoordinator != null) {
            log.info("Marking the coordinator {} dead.", this.consumerCoordinator.id());
            this.consumerCoordinator = null;
        }
    }

    /**
     * Keep discovering the consumer coordinator until it is found.
     */
    private void discoverCoordinator() {
        while (this.consumerCoordinator == null) {
            log.debug("No coordinator known, attempting to discover one.");
            Node coordinator = fetchConsumerCoordinator();

            if (coordinator == null) {
                log.debug("No coordinator found, backing off.");
                Utils.sleep(this.retryBackoffMs);
            } else {
                log.debug("Found coordinator: " + coordinator);
                this.consumerCoordinator = coordinator;
            }
        }
    }

    /**
     * Get the current consumer coordinator information via consumer metadata request.
     *
     * @return the consumer coordinator node
     */
    private Node fetchConsumerCoordinator() {

        // initiate the consumer metadata request
        ClientRequest request = initiateConsumerMetadataRequest();

        // send the request and wait for its response
        ClientResponse response = sendAndReceive(request, request.createdTime());

        // parse the response to get the coordinator info if it is not disconnected,
        // otherwise we need to request metadata update
        if (!response.wasDisconnected()) {
            ConsumerMetadataResponse consumerMetadataResponse = new ConsumerMetadataResponse(response.responseBody());
            // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
            // for the coordinator in the underlying network client layer
            // TODO: this needs to be better handled in KAFKA-1935
            if (consumerMetadataResponse.errorCode() == Errors.NONE.code())
                return new Node(Integer.MAX_VALUE - consumerMetadataResponse.node().id(),
                    consumerMetadataResponse.node().host(),
                    consumerMetadataResponse.node().port());
        } else {
            this.metadata.requestUpdate();
        }

        return null;
    }

    /**
     * Handle the case when the request gets cancelled due to coordinator disconnection.
     */
    private void handleCoordinatorDisconnect(ClientResponse response) {
        int correlation = response.request().request().header().correlationId();
        log.debug("Cancelled request {} with correlation id {} due to coordinator {} being disconnected",
            response.request(),
            correlation,
            response.request().request().destination());

        // mark the coordinator as dead
        coordinatorDead();
    }

    /**
     * Initiate a consumer metadata request to the least loaded node.
     *
     * @return The created request
     */
    private ClientRequest initiateConsumerMetadataRequest() {

        // find a node to ask about the coordinator
        Node node = this.client.leastLoadedNode(time.milliseconds());
        while (node == null || !this.client.ready(node, time.milliseconds())) {
            long now = time.milliseconds();
            this.client.poll(this.retryBackoffMs, now);
            node = this.client.leastLoadedNode(now);

            // if there is no ready node, backoff before retry
            if (node == null)
                Utils.sleep(this.retryBackoffMs);
        }

        // create a consumer metadata request
        log.debug("Issuing consumer metadata request to broker {}", node.id());

        ConsumerMetadataRequest request = new ConsumerMetadataRequest(this.groupId);
        RequestSend send = new RequestSend(node.id(),
            this.client.nextRequestHeader(ApiKeys.CONSUMER_METADATA),
            request.toStruct());
        long now = time.milliseconds();
        return new ClientRequest(now, true, send, null);
    }

    /**
     * Initiate a request to the coordinator.
     */
    private ClientRequest initiateCoordinatorRequest(ApiKeys api, Struct request, RequestCompletionHandler handler, long now) {

        // first make sure the coordinator is known and ready
        ensureCoordinatorReady();

        // create the request for the coordinator
        log.debug("Issuing request ({}: {}) to coordinator {}", api, request, this.consumerCoordinator.id());

        RequestHeader header = this.client.nextRequestHeader(api);
        RequestSend send = new RequestSend(this.consumerCoordinator.id(), header, request);
        return new ClientRequest(now, true, send, handler);
    }

    /**
     * Attempt to send a request and receive its response.
     *
     * @return The response
     */
    private ClientResponse sendAndReceive(ClientRequest clientRequest, long now) {

        // send the request
        this.client.send(clientRequest);

        // drain all responses from the destination node
        List<ClientResponse> responses = this.client.completeAll(clientRequest.request().destination(), now);
        if (responses.isEmpty()) {
            throw new IllegalStateException("This should not happen.");
        } else {
            // other requests should be handled by the callback, and
            // we only care about the response of the last request
            return responses.get(responses.size() - 1);
        }
    }

    private class HeartbeatCompletionHandler implements RequestCompletionHandler {
        @Override
        public void onComplete(ClientResponse resp) {
            if (resp.wasDisconnected()) {
                handleCoordinatorDisconnect(resp);
            } else {
                HeartbeatResponse response = new HeartbeatResponse(resp.responseBody());
                if (response.errorCode() == Errors.NONE.code()) {
                    log.debug("Received successful heartbeat response.");
                } else if (response.errorCode() == Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code()
                    || response.errorCode() == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                    coordinatorDead();
                } else if (response.errorCode() == Errors.ILLEGAL_GENERATION.code()) {
                    subscriptions.needReassignment();
                } else {
                    throw new KafkaException("Unexpected error in heartbeat response: "
                        + Errors.forCode(response.errorCode()).exception().getMessage());
                }
            }
            sensors.heartbeatLatency.record(resp.requestLatencyMs());
        }
    }

    private class CommitOffsetCompletionHandler implements RequestCompletionHandler {

        private final Map<TopicPartition, Long> offsets;

        public CommitOffsetCompletionHandler(Map<TopicPartition, Long> offsets) {
            this.offsets = offsets;
        }

        @Override
        public void onComplete(ClientResponse resp) {
            if (resp.wasDisconnected()) {
                handleCoordinatorDisconnect(resp);
            } else {
                OffsetCommitResponse response = new OffsetCommitResponse(resp.responseBody());
                for (Map.Entry<TopicPartition, Short> entry : response.responseData().entrySet()) {
                    TopicPartition tp = entry.getKey();
                    short errorCode = entry.getValue();
                    long offset = this.offsets.get(tp);
                    if (errorCode == Errors.NONE.code()) {
                        log.debug("Committed offset {} for partition {}", offset, tp);
                        subscriptions.committed(tp, offset);
                    } else if (errorCode == Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.code()
                        || errorCode == Errors.NOT_COORDINATOR_FOR_CONSUMER.code()) {
                        coordinatorDead();
                    } else {
                        log.error("Error committing partition {} at offset {}: {}",
                            tp,
                            offset,
                            Errors.forCode(errorCode).exception().getMessage());
                    }
                }
            }
            sensors.commitLatency.record(resp.requestLatencyMs());
        }
    }

    private class CoordinatorMetrics {
        public final Metrics metrics;
        public final String metricGrpName;

        public final Sensor commitLatency;
        public final Sensor heartbeatLatency;
        public final Sensor partitionReassignments;

        public CoordinatorMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> tags) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(new MetricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request",
                tags), new Avg());
            this.commitLatency.add(new MetricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request",
                tags), new Max());
            this.commitLatency.add(new MetricName("commit-rate",
                this.metricGrpName,
                "The number of commit calls per second",
                tags), new Rate(new Count()));

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(new MetricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a hearbeat request",
                tags), new Max());
            this.heartbeatLatency.add(new MetricName("heartbeat-rate",
                this.metricGrpName,
                "The average number of heartbeats per second",
                tags), new Rate(new Count()));

            this.partitionReassignments = metrics.sensor("reassignment-latency");
            this.partitionReassignments.add(new MetricName("reassignment-time-avg",
                this.metricGrpName,
                "The average time taken for a partition reassignment",
                tags), new Avg());
            this.partitionReassignments.add(new MetricName("reassignment-time-max",
                this.metricGrpName,
                "The max time taken for a partition reassignment",
                tags), new Avg());
            this.partitionReassignments.add(new MetricName("reassignment-rate",
                this.metricGrpName,
                "The number of partition reassignments per second",
                tags), new Rate(new Count()));

            Measurable numParts =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return subscriptions.assignedPartitions().size();
                    }
                };
            metrics.addMetric(new MetricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer",
                tags),
                numParts);

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
