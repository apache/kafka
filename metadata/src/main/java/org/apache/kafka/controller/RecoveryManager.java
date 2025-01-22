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
package org.apache.kafka.controller;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.GetReplicaLogInfoRequestData;
import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.GetReplicaLogInfoRequest;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.TopicIdPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

class RecoveryManager {
    interface QueueAccessor {
        void scheduleDeferred(String tag, long timeFromNowNs, Supplier<ControllerResult<Void>> op);
        void enqueueWriteOp(String name, Supplier<ControllerResult<Void>> op);
    }

    interface ReplicationFacade {
        List<ApiError> electLeadersWithLogInfo(List<TopicIdPartition> topicIdPartitions, LogLengthInfoStore store, List<ApiMessageAndVersion> records);
    }

    static class Builder {
        private Time time;
        private int nodeId;
        private long timeoutMs;
        private boolean enabled;
        private QueueAccessor queueAccessor;
        private RecoveryFetcher.Sender sender;
        private String interbrokerListenerName;
        private ReplicationFacade replicationFacade;

        Builder setRecoveryFetcherSender(RecoveryFetcher.Sender sender) {
            this.sender = sender;
            return this;
        }

        Builder setTimeout(long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        Builder setEnabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        Builder setQueueAccessor(QueueAccessor queueAccessor) {
            this.queueAccessor = queueAccessor;
            return this;
        }

        Builder setInterbrokerListenerName(String interbrokerListenerName) {
            this.interbrokerListenerName = interbrokerListenerName;
            return this;
        }

        Builder setReplicationControlManager(ReplicationFacade replicationProxy) {
            this.replicationFacade = replicationProxy;
            return this;
        }

        Builder disable() {
            this.enabled = false;
            return this;
        }

        RecoveryManager build() {
            // We convert to nano since controller code mostly uses nano-seconds
            long timeoutNs = TimeUnit.MINUTES.toMillis(timeoutMs);
            return new RecoveryManager(nodeId,
                    sender,
                    queueAccessor,
                    replicationFacade,
                    time,
                    interbrokerListenerName,
                    timeoutNs,
                    enabled);
        }
    }

    public static class TopicPartitionReplicas {
        final TopicIdPartition topicIdPartition;
        final int[] replicas;

        public TopicPartitionReplicas(TopicIdPartition topicIdPartition, int[] replicas) {
            this.topicIdPartition = topicIdPartition;
            this.replicas = replicas;
        }
    }

    static class BatchIterator implements Iterator<List<TopicIdPartition>> {
        private final List<TopicIdPartition> elections;
        private final int batchSize;

        private int index = 0;

        BatchIterator(List<TopicIdPartition> elections, int batchSize) {
            this.elections = elections;
            this.batchSize = batchSize;
        }

        @Override
        public boolean hasNext() {
            return index < elections.size();
        }

        @Override
        public List<TopicIdPartition> next() {
            if (!hasNext()) {
                return null;
            }

            ArrayList<TopicIdPartition> batch = new ArrayList<>(Math.min(batchSize, elections.size() - index));
            for (int i = 0; i < batchSize && index < elections.size(); i++, index++) {
                batch.add(elections.get(index));
            }
            return batch;
        }
    }

    private ControllerResult<Void> electBatchAndMaybeEnqueueAnother() {
        List<TopicIdPartition> nextBatch = machine.handleReadNextBatch();
        List<ApiMessageAndVersion> records = new ArrayList<>(nextBatch.size());
        List<ApiError> errors =
                replication.electLeadersWithLogInfo(nextBatch, machine.store(), records);
        int errorCount = 0;
        for (ApiError error : errors) {
            if (error.is(Errors.NONE)) {
                errorCount++;
            }
        }
        if (errorCount > 0) {
            log.debug("Failed to elect leaders for {} out of batch size {}", errorCount, nextBatch.size());
        }
        if (machine.isControllerWriteState()) {
            queueAccessor.enqueueWriteOp("ElectBatchOfLeaders", new ElectBatchOfLeaders());
        }
        return ControllerResult.of(records, null);
    }

    class ElectBatchOfLeaders implements Supplier<ControllerResult<Void>> {
        @Override
        public ControllerResult<Void> get() {
            return electBatchAndMaybeEnqueueAnother();
        }
    }

    class LogInfoReceivedEvent implements Supplier<ControllerResult<Void>> {
        final RecoveryFetcher.Result result;

        LogInfoReceivedEvent(RecoveryFetcher.Result result) {
            this.result = result;
        }

        @Override
        public ControllerResult<Void> get() {
            // Two possible cases:
            // 1. We already finished fetching, in which case stop looking
            // 2. An election already finished but we get a "stray" retry in which case we have difference electionId
            if (!machine.isFetchingState() || machine.electionId != electionId  - 1) {
                return ControllerResult.of(List.of(), null);
            }
            if (result.hasResults()) {
                machine.handleRequestReceived(result.previous.node.id(), result.response);
            }
            if (machine.isControllerWriteState()) {
                return electBatchAndMaybeEnqueueAnother();
            }
            int nextRetryCount = result.previous.retryCount + 1;
            long backoff = new ExponentialBackoff(BACKOFF_INITIAL_INTERVAL_NS,
                    BACKOFF_MULTIPLIER,
                    BACKOFF_MAX_NS,
                    BACKOFF_JITTER).
                    backoff(nextRetryCount);
            RecoveryFetcher.Request nextRequest;
            if (result.hasResults()) {
                GetReplicaLogInfoRequestData newData =
                        new GetReplicaLogInfoRequestData().setBrokerId(result.previous.node.id());
                int total = 0;
                int errors = 0;
                for (GetReplicaLogInfoResponseData.TopicPartitionLogInfo tp: result.response.topicPartitionLogInfoList()) {
                    List<Integer> partitions = new ArrayList<>();
                    for (GetReplicaLogInfoResponseData.PartitionLogInfo li: tp.partitionLogInfo()) {
                        total += 1;
                        if (li.errorCode() != Errors.NONE.code()) {
                            partitions.add(li.partition());
                            errors += 1;
                        }
                    }
                    if (!partitions.isEmpty()) {
                        newData.topicPartitions().add(new GetReplicaLogInfoRequestData.TopicPartitions()
                                .setTopicId(tp.topicId())
                                .setPartitions(partitions));
                    }
                }
                // TODO rework this code; the decision whether to retry should be made before we reach this point
                if (errors == 0) {
                    log.debug("Finished processing request {} to broker {}", result.previous.requestId, result.previous.node.id());
                    return ControllerResult.of(List.of(), null);
                }
                log.debug("Retrying partially failed request to {}. {} of {} topics had errors", result.previous.node.id(), errors, total);
                nextRequest = new RecoveryFetcher.Request(new GetReplicaLogInfoRequest.Builder(newData),
                        result.previous.node, nextRetryCount, result.previous.requestId);
            } else {
                log.debug("Retrying request to {} with previous data", result.previous.node.id());
                nextRequest = new RecoveryFetcher.Request(result.previous.builder,
                        result.previous.node, nextRetryCount, result.previous.requestId);
            }

            log.info("Retrying GetReplicaLogInfo for broker {} attempt {} with backoff {}", result.previous.node.id(), nextRetryCount, backoff);
            queueAccessor.scheduleDeferred(
                    String.format("retry-broker=%d;election=%d;request=%d", result.previous.node.id(), electionId, result.previous.requestId),
                    TimeUnit.MILLISECONDS.toNanos(backoff),
                    () -> {
                        uncleanRecoveryRequestThread.enqueueRequest(receiver, nextRequest);
                        return ControllerResult.of(null, null);
                    });
            return ControllerResult.of(List.of(), null);
        }
    }

    class StopFetchingEvent implements Supplier<ControllerResult<Void>> {
        @Override
        public ControllerResult<Void> get() {
            machine.handleStopFetching();
            return electBatchAndMaybeEnqueueAnother();
        }
    }

    static class StateMachine {
        private static final Logger log = LoggerFactory.getLogger(StateMachine.class);

        enum State {
            Fetching,
            Controller,
            Done,
        }

        private final List<TopicIdPartition> elections;
        private final LogLengthInfoStore store;
        private final Map<Integer, Long> brokerEpochMap;
        private final long startTimeNs;
        private final int electionId;
        private final Time time;
        private final int initialRequestCount;
        private final BatchIterator batches;

        private int remainingRequests;
        private State currentState = State.Fetching;

        StateMachine(Map<Integer, Long> brokerEpochMap,
                     List<TopicIdPartition> elections,
                     Time time,
                     int remainingRequests,
                     int batchSize,
                     int electionId) {
            this.store = new LogLengthInfoStore(elections.size());
            this.elections = elections;
            this.time = time;
            this.brokerEpochMap = brokerEpochMap;
            this.remainingRequests = remainingRequests;
            this.initialRequestCount = remainingRequests;
            this.startTimeNs = time.nanoseconds();
            this.electionId = electionId;
            this.batches = new BatchIterator(elections, batchSize);
        }

        private void changeState(State newState) {
            log.debug("New state: {}; Old State {}; RequestCount: {}", currentState, newState, remainingRequests);
            currentState = newState;
        }

        boolean isFetchingState() {
            return currentState == State.Fetching;
        }

        boolean isControllerWriteState() {
            return currentState == State.Controller;
        }

        boolean isDoneState() {
            return currentState == State.Done;
        }

        void handleRequestReceived(int brokerId, GetReplicaLogInfoResponseData responseData) {
            if (currentState != State.Fetching) {
                log.trace("Received request from {} but state is {}", brokerId, currentState);
                return;
            }
            assert brokerEpochMap.containsKey(brokerId);
            long brokerEpoch = brokerEpochMap.get(brokerId);
            if (brokerEpochMap.get(brokerId) != responseData.brokerEpoch()) {
                log.debug("BrokerId: {} epoch changed from {} to {}", brokerId, brokerEpoch, responseData.brokerEpoch());
                remainingRequests--;
                if (remainingRequests == 0) {
                    changeState(State.Controller);
                }
                return;
            }
            boolean noErrors = true;
            for (GetReplicaLogInfoResponseData.TopicPartitionLogInfo tp: responseData.topicPartitionLogInfoList()) {
                for (GetReplicaLogInfoResponseData.PartitionLogInfo info: tp.partitionLogInfo()) {
                    if (info.errorCode() == Errors.NONE.code()) {
                        TopicIdPartition tip = new TopicIdPartition(tp.topicId(), info.partition());
                        store.add(tip, brokerId, LogLengthInfoStore.EpochOffset.from(info));
                    } else {
                        noErrors = false;
                    }
                }
            }
            if (noErrors) {
                remainingRequests--;
                if (remainingRequests == 0) {
                    changeState(State.Controller);
                }
            }
        }

        void handleStopFetching() {
            changeState(State.Controller);
        }

        List<TopicIdPartition> handleReadNextBatch() {
            if (currentState != State.Controller) {
                log.warn("Warning; called from state other than Controller {}", currentState);
                return List.of();
            }
            List<TopicIdPartition> nextBatch = batches.next();
            if (!batches.hasNext()) {
                log.info("Completed elections of {} topicPartitions", elections.size());
                changeState(State.Done);
            }
            return nextBatch;
        }

        private void logStatus() {
            long duration = TimeUnit.NANOSECONDS.toMillis(time.nanoseconds() - startTimeNs);
            log.debug("Current State: {}, DurationMs: {}, Id at Start: {}", currentState, duration, electionId);
        }

        LogLengthInfoStore store() {
            return store;
        }
    }

    static class RequestsAmortizer {
        private static class Dimension {
            final int brokerId;
            final Uuid uuid;

            Dimension(int brokerId, Uuid uuid) {
                this.brokerId = brokerId;
                this.uuid = uuid;
            }

            @Override
            public boolean equals(Object o) {
                if (o == null || getClass() != o.getClass()) return false;
                Dimension dimension = (Dimension) o;
                return brokerId == dimension.brokerId && Objects.equals(uuid, dimension.uuid);
            }

            @Override
            public int hashCode() {
                return Objects.hash(brokerId, uuid);
            }
        }

        private final Map<Dimension, List<Integer>> dimensionPartitions;

        RequestsAmortizer() {
            dimensionPartitions = new HashMap<>();
        }

        void addTopic(int brokerId, TopicIdPartition tp) {
            Dimension dimension = new Dimension(brokerId, tp.topicId());
            if (dimensionPartitions.containsKey(dimension)) {
                dimensionPartitions.get(dimension).add(tp.partitionId());
            } else {
                List<Integer> partitions = new ArrayList<>();
                partitions.add(tp.partitionId());
                dimensionPartitions.put(dimension, partitions);
            }
        }


        List<List<GetReplicaLogInfoRequestData>> buildRequests() {
            Map<Integer, Integer> brokerCounts = new HashMap<>();
            // Prefer linkedlist since there should not be too many elements and we only need to add at the end.
            Map<Integer, List<GetReplicaLogInfoRequestData>> requestData = new HashMap<>();
            for (Map.Entry<Dimension, List<Integer>> entry : dimensionPartitions.entrySet()) {
                Dimension d = entry.getKey();
                List<Integer> partitions = entry.getValue();
                GetReplicaLogInfoRequestData datum;
                if (requestData.containsKey(d.brokerId)) {
                    int s = requestData.get(d.brokerId).size();
                    datum = requestData.get(d.brokerId).get(s - 1);
                } else {
                    datum = new GetReplicaLogInfoRequestData().setBrokerId(d.brokerId);
                    LinkedList<GetReplicaLogInfoRequestData> brokerRequests = new LinkedList<>();
                    brokerRequests.add(datum);
                    requestData.put(d.brokerId, brokerRequests);
                }
                int count = brokerCounts.computeIfAbsent(d.brokerId, k -> 0);
                GetReplicaLogInfoRequestData.TopicPartitions tps =
                        new GetReplicaLogInfoRequestData.TopicPartitions().
                                setTopicId(d.uuid);
                datum.topicPartitions().add(tps);
                for (Integer partitionId : partitions) {
                    if (count + 1 > GetReplicaLogInfoRequest.MAX_PARTITIONS_PER_REQUEST) {
                        count = 1;
                        tps = new GetReplicaLogInfoRequestData.TopicPartitions().setTopicId(d.uuid);
                        tps.partitions().add(partitionId);
                        datum = new GetReplicaLogInfoRequestData().setBrokerId(d.brokerId);
                        datum.topicPartitions().add(tps);
                        requestData.get(d.brokerId).add(datum);
                    } else {
                        tps.partitions().add(partitionId);
                        count += 1;
                    }
                }
                brokerCounts.put(d.brokerId, count);
            }
            // TODO Need to pick whether this does anything nefarious perf wise to convert to List
            return requestData.values().stream().toList();
        }
    }

    private static final long BACKOFF_MAX_NS = TimeUnit.SECONDS.toNanos(3);
    private static final int BACKOFF_INITIAL_INTERVAL_NS = 0;
    private static final int BACKOFF_MULTIPLIER = 2;
    private static final double BACKOFF_JITTER = 0.3;

    private static final Logger log = LoggerFactory.getLogger(RecoveryManager.class);
    private final RecoveryFetcher.Sender uncleanRecoveryRequestThread;
    private final Time time;
    private final QueueAccessor queueAccessor;
    private final String interBrokerListnerName;
    private final long timeoutNs;
    private final ReplicationFacade replication;
    private final boolean enabled;
    private final RecoveryFetcher.Receiver receiver;

    private int electionId = 0;
    // viewable for testing
    StateMachine machine;

    RecoveryManager(int nodeId,
                    RecoveryFetcher.Sender sender,
                    QueueAccessor queueAccessor,
                    ReplicationFacade replication,
                    Time time,
                    String interBrokerListenerName,
                    long timeoutNs,
                    boolean enabled) {
        this.uncleanRecoveryRequestThread = sender;
        this.queueAccessor = queueAccessor;
        this.replication = replication;
        this.time = time;
        this.interBrokerListnerName = interBrokerListenerName;
        this.timeoutNs = timeoutNs;
        this.enabled = enabled;
        this.receiver = response -> queueAccessor.enqueueWriteOp("LogInfoReceivedEvent", new LogInfoReceivedEvent(response));
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean startRecovery(List<TopicPartitionReplicas> topicsAndReplicas,
                                 Map<Integer, BrokerRegistration> brokerRegistrations,
                                 int batchSize) {
        if (machine != null && !machine.isDoneState()) {
            machine.logStatus();
            return true;
        }
        RequestsAmortizer amoritizer = new RequestsAmortizer();
        Map<Integer, Node> brokerToNodeMap = new HashMap<>(brokerRegistrations.size());
        Map<Integer, Long> brokerEpochMap = new HashMap<>(brokerRegistrations.size());
        List<TopicIdPartition> topicIdPartitions = new ArrayList<>(topicsAndReplicas.size());
        for (TopicPartitionReplicas tr : topicsAndReplicas) {
            topicIdPartitions.add(tr.topicIdPartition);
            for (int replica : tr.replicas) {
                BrokerRegistration reg = brokerRegistrations.get(replica);
                brokerEpochMap.putIfAbsent(reg.id(), reg.epoch());
                // TODO Potentially we may desire skipping this check?
                if (reg.fenced()) {
                    continue;
                }
                // TODO Remove assertions
                assert !reg.listeners().isEmpty();
                assert reg.node(interBrokerListnerName).isPresent();
                Node node = reg.node(interBrokerListnerName).get();
                // TODO should never happen twice
                brokerToNodeMap.putIfAbsent(replica, node);
                amoritizer.addTopic(replica, tr.topicIdPartition);
            }
        }
        long switchPointTimeMs = time.hiResClockMs() + timeoutNs;
        List<List<GetReplicaLogInfoRequestData>> brokerRequestsData = amoritizer.buildRequests();
        machine = new StateMachine(brokerEpochMap, topicIdPartitions, time, brokerRequestsData.size(), batchSize, electionId);
        for (List<GetReplicaLogInfoRequestData> brokerRequestData : brokerRequestsData) {
            int requestId = 0;
            for (GetReplicaLogInfoRequestData data : brokerRequestData) {
                Node node = brokerToNodeMap.get(data.brokerId());
                // TODO we should never get to a node which doesn't exist
                assert node != null;
                RecoveryFetcher.Request request
                        = new RecoveryFetcher.Request(new GetReplicaLogInfoRequest.Builder(data), node, 0, ++requestId);
                uncleanRecoveryRequestThread.enqueueRequest(receiver, request);
            }
        }
        queueAccessor.scheduleDeferred(
                String.format("unclean-recovery-stop-event-%d", electionId),
                TimeUnit.MILLISECONDS.toNanos(switchPointTimeMs),
                new StopFetchingEvent());
        electionId += 1;
        return false;
    }
}
