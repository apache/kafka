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

import com.yammer.metrics.core.Gauge;
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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.server.common.TopicIdPartition;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AssignmentsManager {

    private static final Logger log = LoggerFactory.getLogger(AssignmentsManager.class);

    /**
     * Assignments are dispatched to the controller this long after
     * being submitted to {@link AssignmentsManager}, if there
     * is no request in flight already.
     * The interval is reset when a new assignment is submitted.
     * If {@link AssignReplicasToDirsRequest#MAX_ASSIGNMENTS_PER_REQUEST}
     * is reached, we ignore this interval and dispatch immediately.
     */
    private static final long DISPATCH_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(500);

    private static final long MAX_BACKOFF_INTERVAL_MS = TimeUnit.SECONDS.toNanos(10);

    // visible for testing.
    static final String QUEUE_REPLICA_TO_DIR_ASSIGNMENTS_METRIC_NAME = "QueuedReplicaToDirAssignments";

    private final Time time;
    private final NodeToControllerChannelManager channelManager;
    private final int brokerId;
    private final Supplier<Long> brokerEpochSupplier;
    private final KafkaEventQueue eventQueue;
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());

    // These variables should only be mutated from the KafkaEventQueue thread,
    // but `inflight` and `pending` are also read from a Yammer metrics gauge.
    private volatile Map<TopicIdPartition, AssignmentEvent> inflight = null;
    private volatile Map<TopicIdPartition, AssignmentEvent> pending = new HashMap<>();
    private final ExponentialBackoff resendExponentialBackoff =
            new ExponentialBackoff(100, 2, MAX_BACKOFF_INTERVAL_MS, 0.02);
    private final Function<Uuid, Optional<String>> dirIdToPath;
    private final Function<Uuid, Optional<String>> topicIdToName;
    private int failedAttempts = 0;

    public AssignmentsManager(Time time,
                              NodeToControllerChannelManager channelManager,
                              int brokerId,
                              Supplier<Long> brokerEpochSupplier,
                              Function<Uuid, Optional<String>> dirIdToPath,
                              Function<Uuid, Optional<String>> topicIdToName) {
        this.time = time;
        this.channelManager = channelManager;
        this.brokerId = brokerId;
        this.brokerEpochSupplier = brokerEpochSupplier;
        this.eventQueue = new KafkaEventQueue(time,
                new LogContext("[AssignmentsManager id=" + brokerId + "]"),
                "broker-" + brokerId + "-directory-assignments-manager-",
                new ShutdownEvent());
        channelManager.start();
        this.metricsGroup.newGauge(QUEUE_REPLICA_TO_DIR_ASSIGNMENTS_METRIC_NAME, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return getMapSize(inflight) + getMapSize(pending);
            }

            private int getMapSize(Map<TopicIdPartition, AssignmentEvent> map) {
                return map == null ? 0 : map.size();
            }
        });
        if (dirIdToPath == null) dirIdToPath = id -> Optional.empty();
        this.dirIdToPath = dirIdToPath;
        if (topicIdToName == null) topicIdToName = id -> Optional.empty();
        this.topicIdToName = topicIdToName;
    }

    public void close() throws InterruptedException {
        try {
            eventQueue.close();
        } finally {
            metricsGroup.removeMetric(QUEUE_REPLICA_TO_DIR_ASSIGNMENTS_METRIC_NAME);
        }
    }

    public void onAssignment(TopicIdPartition topicPartition, Uuid dirId, String reason, Runnable callback) {
        if (callback == null) {
            callback = () -> { };
        }
        AssignmentEvent assignment = new AssignmentEvent(time.nanoseconds(), topicPartition, dirId, reason, callback);
        if (log.isDebugEnabled()) {
            log.debug("Queued assignment {}", assignment);
        }
        eventQueue.append(assignment);
    }

    // only for testing
    void wakeup() {
        eventQueue.wakeup();
    }

    /**
     * Base class for all the events handled by {@link AssignmentsManager}.
     */
    private abstract static class Event implements EventQueue.Event {
        /**
         * Override the default behavior in
         * {@link EventQueue.Event#handleException}
         * which swallows the exception.
         */
        @Override
        public void handleException(Throwable e) {
            log.error("Unexpected error handling {}", this, e);
        }
    }

    /**
     * Handles shutdown of the {@link AssignmentsManager}.
     */
    private class ShutdownEvent extends Event {
        @Override
        public void run() {
            channelManager.shutdown();
        }
    }

    /**
     * Handles new generated assignments, to be propagated to the controller.
     * Assignment events may be handled out of order, so for any two assignment
     * events for the same topic partition, the one with the oldest timestamp is
     * disregarded.
     */
    private class AssignmentEvent extends Event {
        final long timestampNs;
        final TopicIdPartition partition;
        final Uuid dirId;
        final String reason;
        final List<Runnable> completionHandlers;
        AssignmentEvent(long timestampNs, TopicIdPartition partition, Uuid dirId, String reason, Runnable onComplete) {
            this.timestampNs = timestampNs;
            this.partition = Objects.requireNonNull(partition);
            this.dirId = Objects.requireNonNull(dirId);
            this.reason = reason;
            this.completionHandlers = new ArrayList<>();
            if (onComplete != null) {
                completionHandlers.add(onComplete);
            }
        }
        void merge(AssignmentEvent other) {
            if (!partition.equals(other.partition)) {
                throw new IllegalArgumentException("Cannot merge events for different partitions");
            }
            completionHandlers.addAll(other.completionHandlers);
        }
        void onComplete() {
            for (Runnable onComplete : completionHandlers) {
                onComplete.run();
            }
        }
        @Override
        public void run() {
            log.trace("Received assignment {}", this);
            AssignmentEvent existing = pending.getOrDefault(partition, null);
            boolean existingIsInFlight = false;
            if (existing == null && inflight != null) {
                existing = inflight.getOrDefault(partition, null);
                existingIsInFlight = true;
            }
            if (existing != null) {
                if (existing.dirId.equals(dirId)) {
                    existing.merge(this);
                    log.debug("Ignoring duplicate assignment {}", this);
                    return;
                }
                if (existing.timestampNs > timestampNs) {
                    existing.merge(this);
                    log.debug("Dropping assignment {} because it's older than existing {}", this, existing);
                    return;
                } else if (!existingIsInFlight) {
                    this.merge(existing);
                    log.debug("Dropping existing assignment {} because it's older than {}", existing, this);
                }
            }
            log.debug("Queueing new assignment {}", this);
            pending.put(partition, this);

            if (inflight == null || inflight.isEmpty()) {
                scheduleDispatch();
            }
        }
        @Override
        public String toString() {
            String partitionString = topicIdToName.apply(partition.topicId())
                    .map(name -> name + ":" + partition.partitionId())
                    .orElseGet(() -> "<topic name unknown id: " + partition.topicId() + " partition: " + partition.partitionId() + ">");
            String dirString = dirIdToPath.apply(dirId)
                    .orElseGet(() -> "<dir path unknown id:" + dirId + ">");
            return "Assignment{" +
                    "timestampNs=" + timestampNs +
                    ", partition=" + partitionString +
                    ", dir=" + dirString +
                    ", reason='" + reason + '\'' +
                    '}';
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AssignmentEvent that = (AssignmentEvent) o;
            return timestampNs == that.timestampNs
                    && Objects.equals(partition, that.partition)
                    && Objects.equals(dirId, that.dirId)
                    && Objects.equals(reason, that.reason);
        }
        @Override
        public int hashCode() {
            return Objects.hash(timestampNs, partition, dirId, reason);
        }
    }

    /**
     * Gathers pending assignments and pushes them to the controller in a {@link AssignReplicasToDirsRequest}.
     */
    private class DispatchEvent extends Event {
        static final String TAG = "dispatch";
        @Override
        public void run() {
            if (inflight != null) {
                throw new IllegalStateException("Bug. Should not be dispatching while there are assignments in flight");
            }
            if (pending.isEmpty()) {
                log.trace("No pending assignments, no-op dispatch");
                return;
            }
            Collection<AssignmentEvent> events = pending.values();
            pending = new HashMap<>();
            inflight = new HashMap<>();
            for (AssignmentEvent event : events) {
                if (inflight.size() < AssignReplicasToDirsRequest.MAX_ASSIGNMENTS_PER_REQUEST) {
                    inflight.put(event.partition, event);
                } else {
                    pending.put(event.partition, event);
                }
            }
            if (!pending.isEmpty()) {
                log.warn("Too many assignments ({}) to fit in one call, sending only {} and queueing the rest",
                        AssignReplicasToDirsRequest.MAX_ASSIGNMENTS_PER_REQUEST + pending.size(),
                        AssignReplicasToDirsRequest.MAX_ASSIGNMENTS_PER_REQUEST);
            }
            Map<TopicIdPartition, Uuid> assignment = inflight.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().dirId));
            log.debug("Dispatching {} assignments:  {}", assignment.size(), assignment);
            channelManager.sendRequest(new AssignReplicasToDirsRequest.Builder(
                    buildRequestData(brokerId, brokerEpochSupplier.get(), assignment)),
                    new AssignReplicasToDirsRequestCompletionHandler());
        }
    }

    /**
     * Handles the response to a dispatched {@link AssignReplicasToDirsRequest}.
     */
    private class AssignmentResponseEvent extends Event {
        private final ClientResponse response;
        public AssignmentResponseEvent(ClientResponse response) {
            this.response = response;
        }
        @Override
        public void run() {
            if (inflight == null) {
                throw new IllegalStateException("Bug. Cannot not be handling a client response if there is are no assignments in flight");
            }
            if (responseIsError(response)) {
                requeueAllAfterFailure();
            } else {
                failedAttempts = 0;
                AssignReplicasToDirsResponseData data = ((AssignReplicasToDirsResponse) response.responseBody()).data();

                Set<AssignmentEvent> failed = filterFailures(data, inflight);
                Set<AssignmentEvent> completed = Utils.diff(HashSet::new, new HashSet<>(inflight.values()), failed);
                for (AssignmentEvent assignmentEvent : completed) {
                    if (log.isDebugEnabled()) {
                        log.debug("Successfully propagated assignment {}", assignmentEvent);
                    }
                    assignmentEvent.onComplete();
                }

                if (!failed.isEmpty()) {
                    log.warn("Re-queueing assignments: {}", failed);
                    for (AssignmentEvent event : failed) {
                        pending.put(event.partition, event);
                    }
                }
                inflight = null;
                if (!pending.isEmpty()) {
                    scheduleDispatch();
                }
            }
        }
    }

    /**
     * Callback for a {@link AssignReplicasToDirsRequest}.
     */
    private class AssignReplicasToDirsRequestCompletionHandler implements ControllerRequestCompletionHandler {
        @Override
        public void onTimeout() {
            log.warn("Request to controller timed out");
            appendResponseEvent(null);
        }
        @Override
        public void onComplete(ClientResponse response) {
            log.debug("Received controller response: {}", response);
            appendResponseEvent(response);
        }
        void appendResponseEvent(ClientResponse response) {
            eventQueue.prepend(new AssignmentResponseEvent(response));
        }
    }

    private void scheduleDispatch() {
        if (pending.size() < AssignReplicasToDirsRequest.MAX_ASSIGNMENTS_PER_REQUEST) {
            scheduleDispatch(DISPATCH_INTERVAL_NS);
        } else {
            log.debug("Too many pending assignments, dispatching immediately");
            eventQueue.enqueue(EventQueue.EventInsertionType.APPEND, DispatchEvent.TAG + "-immediate",
                    new EventQueue.NoDeadlineFunction(), new DispatchEvent());
        }
    }

    private void scheduleDispatch(long delayNs) {
        log.debug("Scheduling dispatch in {}ns", delayNs);
        eventQueue.enqueue(EventQueue.EventInsertionType.DEFERRED, DispatchEvent.TAG,
                new EventQueue.LatestDeadlineFunction(time.nanoseconds() + delayNs), new DispatchEvent());
    }

    private void requeueAllAfterFailure() {
        if (inflight != null) {
            log.debug("Re-queueing all in-flight assignments after failure");
            for (AssignmentEvent event : inflight.values()) {
                pending.put(event.partition, event);
            }
            inflight = null;
            ++failedAttempts;
            long backoffNs = TimeUnit.MILLISECONDS.toNanos(resendExponentialBackoff.backoff(failedAttempts));
            scheduleDispatch(DISPATCH_INTERVAL_NS + backoffNs);
        }
    }

    private static boolean responseIsError(ClientResponse response) {
        if (response == null) {
            log.error("Response is null");
            return true;
        }
        if (response.authenticationException() != null) {
            log.error("Failed to propagate directory assignments because authentication failed", response.authenticationException());
            return true;
        }
        if (response.versionMismatch() != null) {
            log.error("Failed to propagate directory assignments because the request version is unsupported", response.versionMismatch());
            return true;
        }
        if (response.wasDisconnected()) {
            log.error("Failed to propagate directory assignments because the connection to the controller was disconnected");
            return true;
        }
        if (response.wasTimedOut()) {
            log.error("Failed to propagate directory assignments because the request timed out");
            return true;
        }
        if (response.responseBody() == null) {
            log.error("Failed to propagate directory assignments because the Controller returned an empty response");
            return true;
        }
        if (!(response.responseBody() instanceof AssignReplicasToDirsResponse)) {
            log.error("Failed to propagate directory assignments because the Controller returned an invalid response type");
            return true;
        }
        AssignReplicasToDirsResponseData data = ((AssignReplicasToDirsResponse) response.responseBody()).data();
        Errors error = Errors.forCode(data.errorCode());
        if (error != Errors.NONE) {
            log.error("Failed to propagate directory assignments because the Controller returned error {}", error.name());
            return true;
        }
        return false;
    }

    private static Set<AssignmentEvent> filterFailures(
            AssignReplicasToDirsResponseData data,
            Map<TopicIdPartition, AssignmentEvent> sent) {
        Set<AssignmentEvent> failures = new HashSet<>();
        Set<TopicIdPartition> acknowledged = new HashSet<>();
        for (AssignReplicasToDirsResponseData.DirectoryData directory : data.directories()) {
            for (AssignReplicasToDirsResponseData.TopicData topic : directory.topics()) {
                for (AssignReplicasToDirsResponseData.PartitionData partition : topic.partitions()) {
                    TopicIdPartition topicPartition = new TopicIdPartition(topic.topicId(), partition.partitionIndex());
                    AssignmentEvent event = sent.get(topicPartition);
                    if (event == null) {
                        log.error("AssignReplicasToDirsResponse contains unexpected partition {} into directory {}", partition, directory.id());
                    } else {
                        acknowledged.add(topicPartition);
                        Errors error = Errors.forCode(partition.errorCode());
                        if (error != Errors.NONE) {
                            log.error("Controller returned error {} for assignment of partition {} into directory {}",
                                    error.name(), partition, event.dirId);
                            failures.add(event);
                        }
                    }
                }
            }
        }
        for (AssignmentEvent event : sent.values()) {
            if (!acknowledged.contains(event.partition)) {
                log.error("AssignReplicasToDirsResponse is missing assignment of partition {} into directory {}", event.partition, event.dirId);
                failures.add(event);
            }
        }
        return failures;
    }

    // visible for testing
    static AssignReplicasToDirsRequestData buildRequestData(int brokerId, long brokerEpoch, Map<TopicIdPartition, Uuid> assignment) {
        Map<Uuid, DirectoryData> directoryMap = new HashMap<>();
        Map<Uuid, Map<Uuid, TopicData>> topicMap = new HashMap<>();
        for (Map.Entry<TopicIdPartition, Uuid> entry : assignment.entrySet()) {
            TopicIdPartition topicPartition = entry.getKey();
            Uuid directoryId = entry.getValue();
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
                .setBrokerId(brokerId)
                .setBrokerEpoch(brokerEpoch)
                .setDirectories(new ArrayList<>(directoryMap.values()));
    }
}
