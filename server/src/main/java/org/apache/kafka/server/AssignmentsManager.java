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

package org.apache.kafka.server;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData.DirectoryData;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData.PartitionData;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData.TopicData;
import org.apache.kafka.common.message.AssignReplicasToDirsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AssignReplicasToDirsRequest;
import org.apache.kafka.common.requests.AssignReplicasToDirsResponse;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.common.requests.AssignReplicasToDirsRequest.MAX_ASSIGNMENTS_PER_REQUEST;

public final class AssignmentsManager {
    static final ExponentialBackoff STANDARD_BACKOFF = new ExponentialBackoff(
            TimeUnit.MILLISECONDS.toNanos(100),
            2,
            TimeUnit.SECONDS.toNanos(10),
            0.02);

    /**
     * The minimum amount of time we will wait before logging individual assignment failures.
     */
    static final long MIN_NOISY_FAILURE_INTERVAL_NS = TimeUnit.MINUTES.toNanos(2);

    /**
     * The metric reflecting the number of pending assignments.
     */
    static final MetricName QUEUED_REPLICA_TO_DIR_ASSIGNMENTS_METRIC =
            metricName("QueuedReplicaToDirAssignments");

    /**
     * The event at which we send assignments, if appropriate.
     */
    static final String MAYBE_SEND_ASSIGNMENTS_EVENT = "MaybeSendAssignmentsEvent";

    /**
     * The log4j object.
     */
    private final Logger log;

    /**
     * The exponential backoff strategy to use.
     */
    private final ExponentialBackoff backoff;

    /**
     * The clock object to use.
     */
    private final Time time;

    /**
     * Used to send messages to the controller.
     */
    private final NodeToControllerChannelManager channelManager;

    /**
     * The node ID.
     */
    private final int nodeId;

    /**
     * Supplies the latest MetadataImage.
     */
    private final Supplier<MetadataImage> metadataImageSupplier;

    /**
     * Maps directory IDs to descriptions for logging purposes.
     */
    private final Function<Uuid, String> directoryIdToDescription;

    /**
     * Maps partitions to assignments that are ready to send.
     */
    private final ConcurrentHashMap<TopicIdPartition, Assignment> ready;

    /**
     * Maps partitions to assignments that are in-flight. Older entries come first.
     */
    private volatile Map<TopicIdPartition, Assignment> inflight;

    /**
     * The registry to register our metrics with.
     */
    private final MetricsRegistry metricsRegistry;

    /**
     * The number of global failures we had previously (cleared after any success).
     */
    private int previousGlobalFailures;

    /**
     * The event queue.
     */
    private final KafkaEventQueue eventQueue;

    static MetricName metricName(String name) {
        return KafkaYammerMetrics.getMetricName("org.apache.kafka.server", "AssignmentsManager", name);
    }

    public AssignmentsManager(
        Time time,
        NodeToControllerChannelManager channelManager,
        int nodeId,
        Supplier<MetadataImage> metadataImageSupplier,
        Function<Uuid, String> directoryIdToDescription
    ) {
        this(STANDARD_BACKOFF,
            time,
            channelManager,
            nodeId,
            metadataImageSupplier,
            directoryIdToDescription,
            KafkaYammerMetrics.defaultRegistry());
    }

    AssignmentsManager(
        ExponentialBackoff backoff,
        Time time,
        NodeToControllerChannelManager channelManager,
        int nodeId,
        Supplier<MetadataImage> metadataImageSupplier,
        Function<Uuid, String> directoryIdToDescription,
        MetricsRegistry metricsRegistry
    ) {
        this.log = new LogContext("[AssignmentsManager id=" + nodeId + "] ").
            logger(AssignmentsManager.class);
        this.backoff = backoff;
        this.time = time;
        this.channelManager = channelManager;
        this.nodeId = nodeId;
        this.directoryIdToDescription = directoryIdToDescription;
        this.metadataImageSupplier = metadataImageSupplier;
        this.ready = new ConcurrentHashMap<>();
        this.inflight = Collections.emptyMap();
        this.metricsRegistry = metricsRegistry;
        this.metricsRegistry.newGauge(QUEUED_REPLICA_TO_DIR_ASSIGNMENTS_METRIC, new Gauge<Integer>() {
                @Override
                public Integer value() {
                    return numPending();
                }
            });
        this.previousGlobalFailures = 0;
        this.eventQueue = new KafkaEventQueue(time,
            new LogContext("[AssignmentsManager id=" + nodeId + "]"),
            "broker-" + nodeId + "-directory-assignments-manager-",
            new ShutdownEvent());
        channelManager.start();
    }

    public int numPending() {
        return ready.size() + inflight.size();
    }

    public void close() throws InterruptedException {
        eventQueue.close();
    }

    public void onAssignment(
        TopicIdPartition topicIdPartition,
        Uuid directoryId,
        String reason,
        Runnable successCallback
    ) {
        long nowNs = time.nanoseconds();
        Assignment assignment = new Assignment(
                topicIdPartition, directoryId, nowNs, successCallback);
        ready.put(topicIdPartition, assignment);
        if (log.isTraceEnabled()) {
            String topicDescription = Optional.ofNullable(metadataImageSupplier.get().topics().
                getTopic(assignment.topicIdPartition().topicId())).
                    map(TopicImage::name).orElse(assignment.topicIdPartition().topicId().toString());
            log.trace("Registered assignment {}: {}, moving {}-{} into {}",
                assignment,
                reason,
                topicDescription,
                topicIdPartition.partitionId(),
                directoryIdToDescription.apply(assignment.directoryId()));
        }
        rescheduleMaybeSendAssignmentsEvent(nowNs);
    }

    void rescheduleMaybeSendAssignmentsEvent(long nowNs) {
        eventQueue.scheduleDeferred(MAYBE_SEND_ASSIGNMENTS_EVENT,
            new AssignmentsManagerDeadlineFunction(backoff,
                nowNs, previousGlobalFailures, !inflight.isEmpty(), ready.size()),
            new MaybeSendAssignmentsEvent());
    }

    /**
     * Handles shutdown.
     */
    private class ShutdownEvent implements EventQueue.Event {
        @Override
        public void run() {
            log.info("shutting down.");
            try {
                channelManager.shutdown();
            } catch (Exception e) {
                log.error("Unexpected exception shutting down NodeToControllerChannelManager", e);
            }
            try {
                metricsRegistry.removeMetric(QUEUED_REPLICA_TO_DIR_ASSIGNMENTS_METRIC);
            } catch (Exception e) {
                log.error("Unexpected exception removing metrics.", e);
            }
        }
    }

    /**
     * An event that processes the assignments in the ready map.
     */
    private class MaybeSendAssignmentsEvent implements EventQueue.Event {
        @Override
        public void run() {
            try {
                maybeSendAssignments();
            } catch (Exception e) {
                log.error("Unexpected exception in MaybeSendAssignmentsEvent", e);
            }
        }
    }

    /**
     * An event that handles the controller's response to our request.
     */
    private class HandleResponseEvent implements EventQueue.Event {
        private final Map<TopicIdPartition, Assignment> sent;
        private final Optional<ClientResponse> response;

        HandleResponseEvent(
            Map<TopicIdPartition, Assignment> sent,
            Optional<ClientResponse> response
        ) {
            this.sent = sent;
            this.response = response;
        }

        @Override
        public void run() {
            try {
                handleResponse(sent, response);
            } catch (Exception e) {
                log.error("Unexpected exception in HandleResponseEvent", e);
            } finally {
                if (!ready.isEmpty()) {
                    rescheduleMaybeSendAssignmentsEvent(time.nanoseconds());
                }
            }
        }
    }

    /**
     * A callback object that handles the controller's response to our request.
     */
    private class CompletionHandler implements ControllerRequestCompletionHandler {
        private final Map<TopicIdPartition, Assignment> sent;

        CompletionHandler(Map<TopicIdPartition, Assignment> sent) {
            this.sent = sent;
        }

        @Override
        public void onTimeout() {
            eventQueue.append(new HandleResponseEvent(sent, Optional.empty()));
        }

        @Override
        public void onComplete(ClientResponse response) {
            eventQueue.append(new HandleResponseEvent(sent, Optional.of(response)));
        }
    }

    void maybeSendAssignments() {
        int inflightSize = inflight.size();
        if (log.isTraceEnabled()) {
            log.trace("maybeSendAssignments: inflightSize = {}.", inflightSize);
        }
        if (inflightSize > 0) {
            log.trace("maybeSendAssignments: cannot send new assignments because there are " +
                "{} still in flight.", inflightSize);
            return;
        }
        MetadataImage image = metadataImageSupplier.get();
        Map<TopicIdPartition, Assignment> newInFlight = new HashMap<>();
        int numInvalid = 0;
        for (Iterator<Assignment> iterator = ready.values().iterator();
             iterator.hasNext() && newInFlight.size() < MAX_ASSIGNMENTS_PER_REQUEST;
             ) {
            Assignment assignment = iterator.next();
            iterator.remove();
            if (assignment.valid(nodeId, image)) {
                newInFlight.put(assignment.topicIdPartition(), assignment);
            } else {
                numInvalid++;
            }
        }
        log.info("maybeSendAssignments: sending {} assignments; invalidated {} assignments " +
            "prior to sending.", newInFlight.size(), numInvalid);
        if (!newInFlight.isEmpty()) {
            sendAssignments(image.cluster().brokerEpoch(nodeId), newInFlight);
        }
    }

    void sendAssignments(long brokerEpoch, Map<TopicIdPartition, Assignment> newInflight) {
        CompletionHandler completionHandler = new CompletionHandler(newInflight);
        channelManager.sendRequest(new AssignReplicasToDirsRequest.Builder(
            buildRequestData(nodeId, brokerEpoch, newInflight)),
            completionHandler);
        inflight = newInflight;
    }

    void handleResponse(
        Map<TopicIdPartition, Assignment> sent,
        Optional<ClientResponse> assignmentResponse
    ) {
        inflight = Collections.emptyMap();
        Optional<String> globalResponseError = globalResponseError(assignmentResponse);
        if (globalResponseError.isPresent()) {
            previousGlobalFailures++;
            log.error("handleResponse: {} assignments failed; global error: {}. Retrying.",
                sent.size(), globalResponseError.get());
            sent.entrySet().forEach(e -> ready.putIfAbsent(e.getKey(), e.getValue()));
            return;
        }
        previousGlobalFailures = 0;
        AssignReplicasToDirsResponseData responseData =
            ((AssignReplicasToDirsResponse) assignmentResponse.get().responseBody()).data();
        long nowNs = time.nanoseconds();
        for (AssignReplicasToDirsResponseData.DirectoryData directoryData : responseData.directories()) {
            for (AssignReplicasToDirsResponseData.TopicData topicData : directoryData.topics()) {
                for (AssignReplicasToDirsResponseData.PartitionData partitionData : topicData.partitions()) {
                    TopicIdPartition topicIdPartition =
                        new TopicIdPartition(topicData.topicId(), partitionData.partitionIndex());
                    handleAssignmentResponse(topicIdPartition, sent,
                            Errors.forCode(partitionData.errorCode()), nowNs);
                    sent.remove(topicIdPartition);
                }
            }
        }
        for (Assignment assignment : sent.values()) {
            ready.putIfAbsent(assignment.topicIdPartition(), assignment);
            log.error("handleResponse: no result in response for partition {}.",
                assignment.topicIdPartition());
        }
    }

    void handleAssignmentResponse(
        TopicIdPartition topicIdPartition,
        Map<TopicIdPartition, Assignment> sent,
        Errors error,
        long nowNs
    ) {
        Assignment assignment = sent.get(topicIdPartition);
        if (assignment == null) {
            log.error("handleResponse: response contained topicIdPartition {}, but this was not " +
                "in the request.", topicIdPartition);
        } else if (error.equals(Errors.NONE)) {
            try {
                assignment.successCallback().run();
            } catch (Exception e) {
                log.error("handleResponse: unexpected callback exception", e);
            }
        } else {
            ready.putIfAbsent(topicIdPartition, assignment);
            if (log.isDebugEnabled() || nowNs > assignment.submissionTimeNs() + MIN_NOISY_FAILURE_INTERVAL_NS) {
                log.error("handleResponse: error assigning {}: {}.", assignment.topicIdPartition(), error);
            }
        }
    }

    int previousGlobalFailures() throws ExecutionException, InterruptedException {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        eventQueue.append(() -> future.complete(previousGlobalFailures));
        return future.get();
    }

    int numInFlight() {
        return inflight.size();
    }

    static Optional<String> globalResponseError(Optional<ClientResponse> response) {
        if (!response.isPresent()) {
            return Optional.of("Timeout");
        }
        if (response.get().authenticationException() != null) {
            return Optional.of("AuthenticationException");
        }
        if (response.get().wasTimedOut()) {
            return Optional.of("Disonnected[Timeout]");
        }
        if (response.get().wasDisconnected()) {
            return Optional.of("Disconnected");
        }
        if (response.get().versionMismatch() != null) {
            return Optional.of("UnsupportedVersionException");
        }
        if (response.get().responseBody() == null) {
            return Optional.of("EmptyResponse");
        }
        if (!(response.get().responseBody() instanceof AssignReplicasToDirsResponse)) {
            return Optional.of("ClassCastException");
        }
        AssignReplicasToDirsResponseData data = ((AssignReplicasToDirsResponse)
            response.get().responseBody()).data();
        Errors error = Errors.forCode(data.errorCode());
        if (error != Errors.NONE) {
            return Optional.of("Response-level error: " + error.name());
        }
        return Optional.empty();
    }

    static AssignReplicasToDirsRequestData buildRequestData(
        int nodeId,
        long brokerEpoch,
        Map<TopicIdPartition, Assignment> assignments
    ) {
        Map<Uuid, DirectoryData> directoryMap = new HashMap<>();
        Map<Uuid, Map<Uuid, TopicData>> topicMap = new HashMap<>();
        for (Map.Entry<TopicIdPartition, Assignment> entry : assignments.entrySet()) {
            TopicIdPartition topicPartition = entry.getKey();
            Uuid directoryId = entry.getValue().directoryId();
            DirectoryData directory = directoryMap.computeIfAbsent(directoryId, d -> new DirectoryData().setId(directoryId));
            TopicData topic = topicMap.computeIfAbsent(directoryId, d -> new HashMap<>())
                    .computeIfAbsent(topicPartition.topicId(), topicId -> {
                        TopicData data = new TopicData().setTopicId(topicId);
                        directory.topics().add(data);
                        return data;
                    });
            PartitionData partition = new PartitionData().setPartitionIndex(topicPartition.partitionId());
            topic.partitions().add(partition);
        }
        return new AssignReplicasToDirsRequestData()
                .setBrokerId(nodeId)
                .setBrokerEpoch(brokerEpoch)
                .setDirectories(new ArrayList<>(directoryMap.values()));
    }
}
