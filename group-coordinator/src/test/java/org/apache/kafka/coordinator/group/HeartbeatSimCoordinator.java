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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedAssignorException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorShardScheduler;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimerImpl;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorExecutor;
import org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.server.util.timer.TimerTaskEntry;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.LEAVE_GROUP_STATIC_MEMBER_EPOCH;
import static org.apache.kafka.coordinator.common.runtime.CoordinatorOperationExceptionHelper.handleOperationException;

/**
 * The group coordinator of the heartbeat simulation: a real {@link GroupCoordinatorShard} (with
 * the real {@link CoordinatorTimerImpl}) driven by a model of the coordinator runtime and of the
 * request handling of {@code KafkaApis} and {@link GroupCoordinatorService}.
 * <p>
 * The runtime model follows {@code CoordinatorRuntime} with a linger of zero, so every write
 * operation is its own batch:
 * <ul>
 *   <li>An operation that throws completes right away with the error.</li>
 *   <li>An operation without records completes once all the pending writes are committed
 *       ({@code waitForPendingWrites}).</li>
 *   <li>The records of an operation are replayed, appended as one batch, and the operation
 *       completes when the high watermark passes the end of the batch. If the append fails, the
 *       state is reverted to the start of the batch and the operation fails. If the batch is not
 *       committed within the write timeout, the operation fails with a timeout but the batch stays
 *       in the log.</li>
 *   <li>When the coordinator moves, the pending operations fail with NOT_COORDINATOR. The new
 *       leader keeps the log up to a point between the high watermark and the log end, then
 *       loads it: every record is replayed and {@code onLoaded} runs.</li>
 * </ul>
 * The snapshot handling is the one of {@code SnapshottableCoordinator}.
 */
public class HeartbeatSimCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatSimCoordinator.class);

    public static final TopicPartition COORDINATOR_PARTITION = new TopicPartition("__consumer_offsets", 0);

    /**
     * Hooks into the simulation.
     */
    public interface Hooks {
        /** Runs the action on the simulation clock. */
        void schedule(long delayMs, String description, Runnable action);

        /** The time until a written batch is committed. */
        long replicationDelayMs();

        /** Whether the next append fails. */
        boolean failAppend();

        /** An error that the real coordinator would log as unexpected. */
        void onInternalError(String description, Throwable error);

        /** A write operation applied its records to the state. */
        void onWriteApplied(String description);

        /** A heartbeat was applied: the member before and after it, null if absent. */
        void onHeartbeatApplied(ConsumerGroupHeartbeatRequestData request, ConsumerGroupMember before, ConsumerGroupMember after);

        /** The coordinator validated an offset commit; the group is the state it was validated against. */
        void onOffsetCommit(OffsetCommitRequestData request, OffsetCommitResponseData response, Optional<ConsumerGroup> group);
    }

    enum Status {
        ACTIVE, LOADING, NONE
    }

    private record Batch(long baseOffset, List<CoordinatorRecord> records) {
        long endOffset() {
            return baseOffset + records.size();
        }
    }

    private static final class Deferred {
        final long offset;
        final Consumer<Throwable> completion;
        boolean done;

        Deferred(long offset, Consumer<Throwable> completion) {
            this.offset = offset;
            this.completion = completion;
        }

        void complete(Throwable error) {
            if (done) return;
            done = true;
            completion.accept(error);
        }
    }

    /**
     * The coordinator loaded by one leader of the partition.
     */
    final class Incarnation implements CoordinatorShardScheduler<CoordinatorRecord> {
        final int generation;
        final SnapshotRegistry snapshotRegistry;
        final GroupMetadataManager groupMetadataManager;
        final GroupCoordinatorShard shard;
        final CoordinatorTimerImpl<CoordinatorRecord> timer;
        final TreeMap<Long, List<Deferred>> deferred = new TreeMap<>();
        long lastWrittenOffset;
        long lastCommittedOffset;
        boolean active;

        Incarnation(int generation) {
            this.generation = generation;
            LogContext logContext = new LogContext("[Coordinator " + generation + "] ");
            this.snapshotRegistry = new SnapshotRegistry(logContext);
            this.timer = new CoordinatorTimerImpl<>(logContext, new SimTimer(this), this);
            // GroupCoordinatorShard.Builder#build, keeping the group metadata manager.
            GroupCoordinatorMetricsShard metricsShard = metrics.newMetricsShard(snapshotRegistry, COORDINATOR_PARTITION);
            this.groupMetadataManager = new GroupMetadataManager.Builder()
                .withLogContext(logContext)
                .withSnapshotRegistry(snapshotRegistry)
                .withTime(time)
                .withTimer(timer)
                .withExecutor(new MockCoordinatorExecutor<>())
                .withConfig(config)
                .withGroupConfigManager(groupConfigManager)
                .withGroupCoordinatorMetricsShard(metricsShard)
                .withShareGroupAssignor(config.shareGroupAssignors().get(0))
                .withStreamsGroupAssignors(config.streamsGroupAssignors())
                .withStreamsGroupAssignmentRefiner(config.streamsGroupAssignmentRefiner())
                .withAuthorizerPlugin(Optional.empty())
                .build();
            OffsetMetadataManager offsetMetadataManager = new OffsetMetadataManager.Builder()
                .withLogContext(logContext)
                .withSnapshotRegistry(snapshotRegistry)
                .withTime(time)
                .withGroupMetadataManager(groupMetadataManager)
                .withGroupCoordinatorConfig(config)
                .withGroupCoordinatorMetricsShard(metricsShard)
                .build();
            this.shard = new GroupCoordinatorShard(
                logContext,
                groupMetadataManager,
                offsetMetadataManager,
                time,
                timer,
                config,
                metrics,
                metricsShard
            );
            // SnapshottableCoordinator
            snapshotRegistry.idempotentCreateSnapshot(0);
        }

        void updateLastWrittenOffset(long offset) {
            lastWrittenOffset = offset;
            snapshotRegistry.idempotentCreateSnapshot(offset);
        }

        void revertLastWrittenOffset(long offset) {
            lastWrittenOffset = offset;
            snapshotRegistry.revertToSnapshot(offset);
        }

        void updateLastCommittedOffset(long offset) {
            lastCommittedOffset = offset;
            snapshotRegistry.deleteSnapshotsUpTo(offset);
            // The completions may start new writes, so they run after the events are removed.
            List<Deferred> completed = new ArrayList<>();
            Iterator<Map.Entry<Long, List<Deferred>>> it = deferred.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, List<Deferred>> entry = it.next();
                if (entry.getKey() > offset) break;
                it.remove();
                completed.addAll(entry.getValue());
            }
            completed.forEach(event -> event.complete(null));
        }

        void defer(long offset, Consumer<Throwable> completion) {
            Deferred event = new Deferred(offset, completion);
            deferred.computeIfAbsent(offset, __ -> new ArrayList<>()).add(event);
            hooks.schedule(config.offsetCommitTimeoutMs(), "write timeout", () -> {
                if (!event.done) {
                    // OperationTimeout of the runtime: the batch stays in the log.
                    event.complete(new TimeoutException("Write timed out"));
                }
            });
        }

        @Override
        public CompletableFuture<Void> scheduleWriteOperation(String operationName, WriteOperation<CoordinatorRecord> operation) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            write(this, operationName, shard -> operation.generate(), () -> { }, (response, error) -> {
                if (error != null) future.completeExceptionally(error);
                else future.complete(null);
            });
            return future;
        }
    }

    /**
     * A {@link Timer} on the simulation clock for the {@link CoordinatorTimerImpl}.
     */
    private final class SimTimer implements Timer {
        private final Incarnation incarnation;

        SimTimer(Incarnation incarnation) {
            this.incarnation = incarnation;
        }

        @Override
        public void add(TimerTask timerTask) {
            TimerTaskEntry entry = new TimerTaskEntry(timerTask, time.milliseconds() + timerTask.delayMs);
            hooks.schedule(timerTask.delayMs, "coordinator timer", () -> {
                if (!entry.cancelled() && incarnation.active) {
                    timerTask.run();
                }
            });
        }

        @Override
        public boolean advanceClock(long timeoutMs) {
            return false;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public void close() { }
    }

    private final Time time;
    private final Hooks hooks;
    private final GroupCoordinatorConfig config;
    private final GroupConfigManager groupConfigManager;
    private final GroupCoordinatorMetrics metrics;
    private final List<String> consumerGroupAssignors;
    private final CoordinatorMetadataImage image;
    private final Map<String, Uuid> topicIds;

    private final List<Batch> log = new ArrayList<>();
    private long highWatermark;
    private int generation;
    private Status status = Status.NONE;
    private int coordinatorNodeId = -1;
    private Incarnation current;

    public HeartbeatSimCoordinator(
        Time time,
        Hooks hooks,
        Map<String, Object> configs,
        CoordinatorMetadataImage image,
        Map<String, Uuid> topicIds
    ) {
        this.time = time;
        this.hooks = hooks;
        this.config = GroupCoordinatorConfigTest.createConfig(configs);
        this.groupConfigManager = new GroupConfigManager(config, new ShareGroupConfig(new AbstractConfig(
            Utils.mergeConfigs(List.of(ShareGroupConfig.CONFIG_DEF, GroupCoordinatorConfig.CONFIG_DEF)),
            Map.of()
        )));
        this.metrics = new GroupCoordinatorMetrics();
        this.consumerGroupAssignors = config.consumerGroupAssignors().stream().map(ConsumerGroupPartitionAssignor::name).toList();
        this.image = image;
        this.topicIds = topicIds;
    }

    // ---------------------------------------------------------------------------------------------
    // Leadership of the coordinator partition.
    // ---------------------------------------------------------------------------------------------

    /**
     * The broker becomes the leader and starts loading the log.
     */
    public void elect(int nodeId, long loadDurationMs) {
        if (status != Status.NONE) throw new IllegalStateException("Coordinator already elected");
        coordinatorNodeId = nodeId;
        status = Status.LOADING;
        Incarnation incarnation = new Incarnation(++generation);
        current = incarnation;
        hooks.schedule(loadDurationMs, "load coordinator", () -> load(incarnation));
    }

    private void load(Incarnation incarnation) {
        if (current != incarnation) return;
        // CoordinatorLoaderImpl replays the log, then the runtime activates the shard.
        long offset = 0;
        for (Batch batch : log) {
            for (CoordinatorRecord record : batch.records()) {
                try {
                    incarnation.shard.replay(offset, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, record);
                } catch (Throwable t) {
                    hooks.onInternalError("replay of offset " + offset + " on load", t);
                }
                offset++;
            }
        }
        if (offset > 0) incarnation.updateLastWrittenOffset(offset);
        // The new leader commits its log.
        highWatermark = offset;
        incarnation.updateLastCommittedOffset(offset);
        incarnation.active = true;
        status = Status.ACTIVE;
        incarnation.shard.onLoaded(image);
    }

    /**
     * The leader resigns: the pending operations fail and the timers are cancelled. The new
     * leader's log ends at {@code keptOffset}, between the high watermark and the log end.
     */
    public void resign(Function<List<Long>, Long> keptOffset) {
        if (status == Status.NONE) return;
        Incarnation incarnation = current;
        incarnation.active = false;
        incarnation.timer.cancelAll();
        if (status == Status.ACTIVE) incarnation.shard.onUnloaded();
        List<Deferred> pending = new ArrayList<>();
        incarnation.deferred.values().forEach(pending::addAll);
        incarnation.deferred.clear();
        pending.forEach(event -> event.complete(new NotCoordinatorException("Not coordinator")));

        // The new leader has every committed batch and maybe some of the others.
        List<Long> candidates = new ArrayList<>();
        candidates.add(highWatermark);
        for (Batch batch : log) {
            if (batch.endOffset() > highWatermark) candidates.add(batch.endOffset());
        }
        long kept = keptOffset.apply(candidates);
        log.removeIf(batch -> batch.endOffset() > kept);

        current = null;
        status = Status.NONE;
        coordinatorNodeId = -1;
    }

    public Status status() {
        return status;
    }

    public int coordinatorNodeId() {
        return coordinatorNodeId;
    }

    public long logEndOffset() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).endOffset();
    }

    /**
     * @return The consumer group in the latest state of the active coordinator, if any.
     */
    public Optional<ConsumerGroup> consumerGroup(String groupId) {
        if (current == null || !current.active) return Optional.empty();
        try {
            return Optional.of(current.groupMetadataManager.consumerGroup(groupId, Long.MAX_VALUE));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Runtime.
    // ---------------------------------------------------------------------------------------------

    /**
     * Schedules a write operation on the active coordinator.
     */
    private <T> void write(
        String name,
        Function<GroupCoordinatorShard, CoordinatorResult<T, CoordinatorRecord>> op,
        BiConsumer<T, Throwable> callback
    ) {
        if (current == null || !current.active) {
            callback.accept(null, status == Status.LOADING ?
                new CoordinatorLoadInProgressException("Loading") :
                new NotCoordinatorException("Not coordinator"));
            return;
        }
        write(current, name, op, () -> { }, callback);
    }

    /**
     * Runs a write operation of the incarnation, then {@code afterApply} once its records are
     * applied to the state. The callback runs when the operation completes.
     */
    private <T> void write(
        Incarnation incarnation,
        String name,
        Function<GroupCoordinatorShard, CoordinatorResult<T, CoordinatorRecord>> op,
        Runnable afterApply,
        BiConsumer<T, Throwable> callback
    ) {
        if (!incarnation.active || incarnation != current) {
            callback.accept(null, new NotCoordinatorException("Not coordinator"));
            return;
        }

        CoordinatorResult<T, CoordinatorRecord> result;
        try {
            result = op.apply(incarnation.shard);
        } catch (Throwable t) {
            callback.accept(null, t);
            return;
        }

        Consumer<Throwable> complete = error -> completeWrite(result, error, callback);
        List<CoordinatorRecord> records = result.records();
        if (records.isEmpty()) {
            // waitForPendingWrites
            if (incarnation.lastCommittedOffset < incarnation.lastWrittenOffset) {
                incarnation.defer(incarnation.lastWrittenOffset, complete);
            } else {
                complete.accept(null);
            }
            return;
        }

        long baseOffset = incarnation.lastWrittenOffset;
        Throwable error = result.replayRecords() ? replay(incarnation, name, baseOffset, records) : null;
        if (error == null && hooks.failAppend()) {
            error = new NotEnoughReplicasException("Append failed");
        }
        if (error != null) {
            // failCurrentBatch reverts the state to the start of the batch.
            incarnation.revertLastWrittenOffset(baseOffset);
            callback.accept(null, error);
            return;
        }

        Batch batch = new Batch(baseOffset, List.copyOf(records));
        log.add(batch);
        incarnation.updateLastWrittenOffset(batch.endOffset());
        hooks.onWriteApplied(name);
        afterApply.run();
        incarnation.defer(batch.endOffset(), complete);
        hooks.schedule(hooks.replicationDelayMs(), "replication", () -> commit(incarnation, batch));
    }

    private static <T> void completeWrite(CoordinatorResult<T, CoordinatorRecord> result, Throwable error, BiConsumer<T, Throwable> callback) {
        callback.accept(error == null ? result.response() : null, error);
    }

    /**
     * The batch is replicated: the high watermark moves to its end, unless the log was truncated
     * since.
     */
    private void commit(Incarnation incarnation, Batch batch) {
        if (incarnation != current || !incarnation.active) return;
        long offset = Math.min(batch.endOffset(), logEndOffset());
        if (offset > highWatermark) {
            highWatermark = offset;
            incarnation.updateLastCommittedOffset(offset);
        }
    }

    /**
     * Replays the records of a write.
     *
     * @return The error of the replay, or null.
     */
    private Throwable replay(Incarnation incarnation, String name, long baseOffset, List<CoordinatorRecord> records) {
        for (int i = 0; i < records.size(); i++) {
            try {
                incarnation.shard.replay(baseOffset + i, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, records.get(i));
            } catch (Throwable t) {
                hooks.onInternalError("replay of " + name, t);
                return t;
            }
        }
        return null;
    }

    // ---------------------------------------------------------------------------------------------
    // Requests.
    // ---------------------------------------------------------------------------------------------

    private static RequestContext context(ApiKeys key, short version, String clientId) {
        return new RequestContext(
            new RequestHeader(key, version, clientId, 0),
            "1",
            InetAddress.getLoopbackAddress(),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false
        );
    }

    private Optional<Errors> notCoordinator(int nodeId) {
        if (nodeId != coordinatorNodeId || status == Status.NONE) return Optional.of(Errors.NOT_COORDINATOR);
        if (status == Status.LOADING) return Optional.of(Errors.COORDINATOR_LOAD_IN_PROGRESS);
        return Optional.empty();
    }

    /**
     * GroupCoordinatorService#consumerGroupHeartbeat on the broker {@code nodeId}.
     */
    public void consumerGroupHeartbeat(
        int nodeId,
        short version,
        String clientId,
        ConsumerGroupHeartbeatRequestData request,
        Consumer<ConsumerGroupHeartbeatResponseData> respond
    ) {
        Optional<Errors> error = notCoordinator(nodeId);
        if (error.isPresent()) {
            respond.accept(new ConsumerGroupHeartbeatResponseData().setErrorCode(error.get().code()));
            return;
        }
        try {
            throwIfConsumerGroupHeartbeatRequestIsInvalid(request);
        } catch (Throwable ex) {
            ApiError apiError = ApiError.fromThrowable(ex);
            respond.accept(new ConsumerGroupHeartbeatResponseData()
                .setErrorCode(apiError.error().code())
                .setErrorMessage(apiError.message()));
            return;
        }
        RequestContext context = context(ApiKeys.CONSUMER_GROUP_HEARTBEAT, version, clientId);
        ConsumerGroupMember[] before = new ConsumerGroupMember[1];
        if (current == null || !current.active) {
            respond.accept(new ConsumerGroupHeartbeatResponseData().setErrorCode(
                (status == Status.LOADING ? Errors.COORDINATOR_LOAD_IN_PROGRESS : Errors.NOT_COORDINATOR).code()));
            return;
        }
        write(current, "consumer-group-heartbeat", shard -> {
            before[0] = member(request.groupId(), request.memberId());
            return shard.consumerGroupHeartbeat(context, request);
        }, () -> hooks.onHeartbeatApplied(request, before[0], member(request.groupId(), request.memberId())
        ), (response, exception) -> {
            if (exception != null) {
                if (ApiError.fromThrowable(exception).error() == Errors.UNKNOWN_SERVER_ERROR) {
                    hooks.onInternalError("consumer-group-heartbeat " + request, exception);
                }
                respond.accept(handleOperationException(
                    "consumer-group-heartbeat",
                    request,
                    exception,
                    (err, message) -> new ConsumerGroupHeartbeatResponseData()
                        .setErrorCode(err.code())
                        .setErrorMessage(message),
                    LOG
                ));
            } else {
                respond.accept(response);
            }
        });
    }

    private ConsumerGroupMember member(String groupId, String memberId) {
        return consumerGroup(groupId).map(group -> group.members().get(memberId)).orElse(null);
    }

    /**
     * GroupCoordinatorService#throwIfConsumerGroupHeartbeatRequestIsInvalid for the latest
     * version.
     */
    private void throwIfConsumerGroupHeartbeatRequestIsInvalid(ConsumerGroupHeartbeatRequestData request) {
        throwIfIdsAreInvalid(request);
        if (request.memberEpoch() == 0) {
            throwIfJoinRequestIsInvalid(request);
        } else if (request.memberEpoch() == LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            if (request.instanceId() == null) throw new InvalidRequestException("InstanceId can't be null.");
        } else if (request.memberEpoch() < LEAVE_GROUP_STATIC_MEMBER_EPOCH) {
            throw new InvalidRequestException("MemberEpoch is invalid.");
        }

        if (request.serverAssignor() != null && !consumerGroupAssignors.contains(request.serverAssignor())) {
            throw new UnsupportedAssignorException("ServerAssignor " + request.serverAssignor() + " is not supported.");
        }
        if (request.memberEpoch() == LEAVE_GROUP_MEMBER_EPOCH && request.memberId().isEmpty()) {
            throw new InvalidRequestException("MemberId can't be empty.");
        }
    }

    private static void throwIfIdsAreInvalid(ConsumerGroupHeartbeatRequestData request) {
        if (request.memberId().isEmpty()) throw new InvalidRequestException("MemberId can't be empty.");
        if (request.groupId().isEmpty()) throw new InvalidRequestException("GroupId can't be empty.");
        if (request.instanceId() != null && request.instanceId().isEmpty()) throw new InvalidRequestException("InstanceId can't be empty.");
        if (request.rackId() != null && request.rackId().isEmpty()) throw new InvalidRequestException("RackId can't be empty.");
    }

    private static void throwIfJoinRequestIsInvalid(ConsumerGroupHeartbeatRequestData request) {
        if (request.rebalanceTimeoutMs() == -1) {
            throw new InvalidRequestException("RebalanceTimeoutMs must be provided in first request.");
        }
        if (request.topicPartitions() == null || !request.topicPartitions().isEmpty()) {
            throw new InvalidRequestException("TopicPartitions must be empty when (re-)joining.");
        }
        if (request.subscribedTopicNames() == null && request.subscribedTopicRegex() == null) {
            throw new InvalidRequestException("Either SubscribedTopicNames or SubscribedTopicRegex must" +
                " be non-null when (re-)joining.");
        }
    }

    /**
     * KafkaApis#handleOffsetCommitRequest (topic ids and names) and
     * GroupCoordinatorService#commitOffsets.
     */
    public void commitOffsets(
        int nodeId,
        short version,
        String clientId,
        OffsetCommitRequestData request,
        BiConsumer<OffsetCommitRequestData, OffsetCommitResponseData> respond
    ) {
        for (OffsetCommitRequestData.OffsetCommitRequestTopic topic : request.topics()) {
            if (topic.name() == null || topic.name().isEmpty()) {
                topicIds.forEach((name, id) -> {
                    if (id.equals(topic.topicId())) topic.setName(name);
                });
            }
            if (topic.topicId() == null || topic.topicId().equals(Uuid.ZERO_UUID)) {
                topic.setTopicId(topicIds.getOrDefault(topic.name(), Uuid.ZERO_UUID));
            }
        }
        Optional<Errors> error = notCoordinator(nodeId);
        if (error.isPresent()) {
            respond.accept(request, OffsetCommitRequest.getErrorResponse(request, error.get()));
            return;
        }
        RequestContext context = context(ApiKeys.OFFSET_COMMIT, version, clientId);
        write("commit-offset", shard -> {
            CoordinatorResult<OffsetCommitResponseData, CoordinatorRecord> result = shard.commitOffset(context, request);
            hooks.onOffsetCommit(request, result.response(), consumerGroup(request.groupId()));
            return result;
        }, (response, exception) -> {
            if (exception != null) {
                respond.accept(request, handleOperationException(
                    "commit-offset",
                    request,
                    exception,
                    (err, __) -> OffsetCommitRequest.getErrorResponse(request, err),
                    LOG
                ));
            } else {
                respond.accept(request, response);
            }
        });
    }

    /**
     * GroupCoordinatorService#fetchOffsets for one group.
     */
    public void fetchOffsets(
        int nodeId,
        short version,
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        boolean requireStable,
        Consumer<OffsetFetchResponseData.OffsetFetchResponseGroup> respond
    ) {
        if (request.topics() != null) {
            for (OffsetFetchRequestData.OffsetFetchRequestTopics topic : request.topics()) {
                if (topic.name() == null || topic.name().isEmpty()) {
                    topicIds.forEach((name, id) -> {
                        if (id.equals(topic.topicId())) topic.setName(name);
                    });
                }
            }
        }
        Optional<Errors> error = notCoordinator(nodeId);
        if (error.isPresent()) {
            respond.accept(OffsetFetchResponse.groupError(request, error.get(), version));
            return;
        }
        BiConsumer<OffsetFetchResponseData.OffsetFetchResponseGroup, Throwable> complete = (response, exception) -> {
            if (exception != null) {
                respond.accept(OffsetFetchResponse.groupError(request, Errors.forException(exception), version));
            } else {
                if (response.topics() != null) {
                    response.topics().forEach(topic -> topic.setTopicId(topicIds.getOrDefault(topic.name(), Uuid.ZERO_UUID)));
                }
                respond.accept(response);
            }
        };
        if (requireStable) {
            write("fetch-offsets", shard -> new CoordinatorResult<>(List.of(), shard.fetchOffsets(request, Long.MAX_VALUE)), complete);
        } else {
            OffsetFetchResponseData.OffsetFetchResponseGroup response;
            try {
                response = current.shard.fetchOffsets(request, current.lastCommittedOffset);
            } catch (Throwable t) {
                complete.accept(null, t);
                return;
            }
            complete.accept(response, null);
        }
    }
}
