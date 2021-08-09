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

import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.message.AlterIsrRequestData;
import org.apache.kafka.common.message.AlterIsrResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.controller.SnapshotGenerator.Section;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metadata.FeatureMapAndEpoch;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.EventQueue.EarliestDeadlineFunction;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.SnapshotWriter;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;


/**
 * QuorumController implements the main logic of the KRaft (Kafka Raft Metadata) mode controller.
 *
 * The node which is the leader of the metadata log becomes the active controller.  All
 * other nodes remain in standby mode.  Standby controllers cannot create new metadata log
 * entries.  They just replay the metadata log entries that the current active controller
 * has created.
 *
 * The QuorumController is single-threaded.  A single event handler thread performs most
 * operations.  This avoids the need for complex locking.
 *
 * The controller exposes an asynchronous, futures-based API to the world.  This reflects
 * the fact that the controller may have several operations in progress at any given
 * point.  The future associated with each operation will not be completed until the
 * results of the operation have been made durable to the metadata log.
 */
public final class QuorumController implements Controller {
    /**
     * A builder class which creates the QuorumController.
     */
    static public class Builder {
        private final int nodeId;
        private Time time = Time.SYSTEM;
        private String threadNamePrefix = null;
        private LogContext logContext = null;
        private Map<ConfigResource.Type, ConfigDef> configDefs = Collections.emptyMap();
        private RaftClient<ApiMessageAndVersion> raftClient = null;
        private Map<String, VersionRange> supportedFeatures = Collections.emptyMap();
        private short defaultReplicationFactor = 3;
        private int defaultNumPartitions = 1;
        private ReplicaPlacer replicaPlacer = new StripedReplicaPlacer(new Random());
        private long snapshotMaxNewRecordBytes = Long.MAX_VALUE;
        private long sessionTimeoutNs = NANOSECONDS.convert(18, TimeUnit.SECONDS);
        private ControllerMetrics controllerMetrics = null;

        public Builder(int nodeId) {
            this.nodeId = nodeId;
        }

        public Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder setThreadNamePrefix(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
            return this;
        }

        public Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        public Builder setConfigDefs(Map<ConfigResource.Type, ConfigDef> configDefs) {
            this.configDefs = configDefs;
            return this;
        }

        public Builder setRaftClient(RaftClient<ApiMessageAndVersion> logManager) {
            this.raftClient = logManager;
            return this;
        }

        public Builder setSupportedFeatures(Map<String, VersionRange> supportedFeatures) {
            this.supportedFeatures = supportedFeatures;
            return this;
        }

        public Builder setDefaultReplicationFactor(short defaultReplicationFactor) {
            this.defaultReplicationFactor = defaultReplicationFactor;
            return this;
        }

        public Builder setDefaultNumPartitions(int defaultNumPartitions) {
            this.defaultNumPartitions = defaultNumPartitions;
            return this;
        }

        public Builder setReplicaPlacer(ReplicaPlacer replicaPlacer) {
            this.replicaPlacer = replicaPlacer;
            return this;
        }

        public Builder setSnapshotMaxNewRecordBytes(long value) {
            this.snapshotMaxNewRecordBytes = value;
            return this;
        }

        public Builder setSessionTimeoutNs(long sessionTimeoutNs) {
            this.sessionTimeoutNs = sessionTimeoutNs;
            return this;
        }

        public Builder setMetrics(ControllerMetrics controllerMetrics) {
            this.controllerMetrics = controllerMetrics;
            return this;
        }

        @SuppressWarnings("unchecked")
        public QuorumController build() throws Exception {
            if (raftClient == null) {
                throw new RuntimeException("You must set a raft client.");
            }
            if (threadNamePrefix == null) {
                threadNamePrefix = String.format("Node%d_", nodeId);
            }
            if (logContext == null) {
                logContext = new LogContext(String.format("[Controller %d] ", nodeId));
            }
            if (controllerMetrics == null) {
                controllerMetrics = (ControllerMetrics) Class.forName(
                    "org.apache.kafka.controller.MockControllerMetrics").getConstructor().newInstance();
            }
            KafkaEventQueue queue = null;
            try {
                queue = new KafkaEventQueue(time, logContext, threadNamePrefix);
                return new QuorumController(logContext, nodeId, queue, time, configDefs,
                    raftClient, supportedFeatures, defaultReplicationFactor,
                    defaultNumPartitions, replicaPlacer, snapshotMaxNewRecordBytes,
                    sessionTimeoutNs, controllerMetrics);
            } catch (Exception e) {
                Utils.closeQuietly(queue, "event queue");
                throw e;
            }
        }
    }

    private static final String ACTIVE_CONTROLLER_EXCEPTION_TEXT_PREFIX =
        "The active controller appears to be node ";

    private NotControllerException newNotControllerException() {
        OptionalInt latestController = raftClient.leaderAndEpoch().leaderId();
        if (latestController.isPresent()) {
            return new NotControllerException(ACTIVE_CONTROLLER_EXCEPTION_TEXT_PREFIX +
                latestController.getAsInt());
        } else {
            return new NotControllerException("No controller appears to be active.");
        }
    }

    public static int exceptionToApparentController(NotControllerException e) {
        if (e.getMessage().startsWith(ACTIVE_CONTROLLER_EXCEPTION_TEXT_PREFIX)) {
            return Integer.parseInt(e.getMessage().substring(
                ACTIVE_CONTROLLER_EXCEPTION_TEXT_PREFIX.length()));
        } else {
            return -1;
        }
    }

    private void handleEventEnd(String name, long startProcessingTimeNs) {
        long endProcessingTime = time.nanoseconds();
        long deltaNs = endProcessingTime - startProcessingTimeNs;
        log.debug("Processed {} in {} us", name,
            MICROSECONDS.convert(deltaNs, NANOSECONDS));
        controllerMetrics.updateEventQueueProcessingTime(NANOSECONDS.toMillis(deltaNs));
    }

    private Throwable handleEventException(String name,
                                           Optional<Long> startProcessingTimeNs,
                                           Throwable exception) {
        if (!startProcessingTimeNs.isPresent()) {
            log.info("unable to start processing {} because of {}.", name,
                exception.getClass().getSimpleName());
            if (exception instanceof ApiException) {
                return exception;
            } else {
                return new UnknownServerException(exception);
            }
        }
        long endProcessingTime = time.nanoseconds();
        long deltaNs = endProcessingTime - startProcessingTimeNs.get();
        long deltaUs = MICROSECONDS.convert(deltaNs, NANOSECONDS);
        if (exception instanceof ApiException) {
            log.info("{}: failed with {} in {} us", name,
                exception.getClass().getSimpleName(), deltaUs);
            return exception;
        }
        log.warn("{}: failed with unknown server exception {} at epoch {} in {} us.  " +
            "Reverting to last committed offset {}.",
            this, exception.getClass().getSimpleName(), curClaimEpoch, deltaUs,
            lastCommittedOffset, exception);
        raftClient.resign(curClaimEpoch);
        renounce();
        return new UnknownServerException(exception);
    }

    /**
     * A controller event for handling internal state changes, such as Raft inputs.
     */
    class ControlEvent implements EventQueue.Event {
        private final String name;
        private final Runnable handler;
        private final long eventCreatedTimeNs = time.nanoseconds();
        private Optional<Long> startProcessingTimeNs = Optional.empty();

        ControlEvent(String name, Runnable handler) {
            this.name = name;
            this.handler = handler;
        }

        @Override
        public void run() throws Exception {
            long now = time.nanoseconds();
            controllerMetrics.updateEventQueueTime(NANOSECONDS.toMillis(now - eventCreatedTimeNs));
            startProcessingTimeNs = Optional.of(now);
            log.debug("Executing {}.", this);
            handler.run();
            handleEventEnd(this.toString(), startProcessingTimeNs.get());
        }

        @Override
        public void handleException(Throwable exception) {
            handleEventException(name, startProcessingTimeNs, exception);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private void appendControlEvent(String name, Runnable handler) {
        ControlEvent event = new ControlEvent(name, handler);
        queue.append(event);
    }

    private static final String GENERATE_SNAPSHOT = "generateSnapshot";

    private static final int MAX_BATCHES_PER_GENERATE_CALL = 10;

    class SnapshotGeneratorManager implements Runnable {
        private final ExponentialBackoff exponentialBackoff = new ExponentialBackoff(10, 2, 5000, 0);
        private SnapshotGenerator generator = null;

        void createSnapshotGenerator(long committedOffset, int committedEpoch, long committedTimestamp) {
            if (generator != null) {
                throw new RuntimeException("Snapshot generator already exists.");
            }
            if (!snapshotRegistry.hasSnapshot(committedOffset)) {
                throw new RuntimeException(
                    String.format(
                        "Cannot generate a snapshot at committed offset %s because it does not exists in the snapshot registry.",
                        committedOffset
                    )
                );
            }
            Optional<SnapshotWriter<ApiMessageAndVersion>> writer = raftClient.createSnapshot(
                committedOffset,
                committedEpoch,
                committedTimestamp
            );
            if (writer.isPresent()) {
                generator = new SnapshotGenerator(
                    logContext,
                    writer.get(),
                    MAX_BATCHES_PER_GENERATE_CALL,
                    exponentialBackoff,
                    Arrays.asList(
                        new Section("features", featureControl.iterator(committedOffset)),
                        new Section("cluster", clusterControl.iterator(committedOffset)),
                        new Section("replication", replicationControl.iterator(committedOffset)),
                        new Section("configuration", configurationControl.iterator(committedOffset)),
                        new Section("clientQuotas", clientQuotaControlManager.iterator(committedOffset)),
                        new Section("producerIds", producerIdControlManager.iterator(committedOffset))
                    )
                );
                reschedule(0);
            } else {
                log.info(
                    "Skipping generation of snapshot for committed offset {} and epoch {} since it already exists",
                    committedOffset,
                    committedEpoch
                );
            }
        }

        void cancel() {
            if (generator == null) return;
            log.error("Cancelling snapshot {}", generator.lastContainedLogOffset());
            generator.writer().close();
            generator = null;

            // Delete every in-memory snapshot up to the committed offset. They are not needed since this
            // snapshot generation was canceled.
            snapshotRegistry.deleteSnapshotsUpTo(lastCommittedOffset);

            queue.cancelDeferred(GENERATE_SNAPSHOT);
        }

        void reschedule(long delayNs) {
            ControlEvent event = new ControlEvent(GENERATE_SNAPSHOT, this);
            queue.scheduleDeferred(event.name,
                new EarliestDeadlineFunction(time.nanoseconds() + delayNs), event);
        }

        @Override
        public void run() {
            if (generator == null) {
                log.debug("No snapshot is in progress.");
                return;
            }
            OptionalLong nextDelay;
            try {
                nextDelay = generator.generateBatches();
            } catch (Exception e) {
                log.error("Error while generating snapshot {}", generator.lastContainedLogOffset(), e);
                generator.writer().close();
                generator = null;
                return;
            }
            if (!nextDelay.isPresent()) {
                log.info("Finished generating snapshot {}.", generator.lastContainedLogOffset());
                generator.writer().close();
                generator = null;

                // Delete every in-memory snapshot up to the committed offset. They are not needed since this
                // snapshot generation finished.
                snapshotRegistry.deleteSnapshotsUpTo(lastCommittedOffset);
                return;
            }
            reschedule(nextDelay.getAsLong());
        }

        OptionalLong snapshotLastOffsetFromLog() {
            if (generator == null) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(generator.lastContainedLogOffset());
        }
    }

    /**
     * A controller event that reads the committed internal state in order to expose it
     * to an API.
     */
    class ControllerReadEvent<T> implements EventQueue.Event {
        private final String name;
        private final CompletableFuture<T> future;
        private final Supplier<T> handler;
        private final long eventCreatedTimeNs = time.nanoseconds();
        private Optional<Long> startProcessingTimeNs = Optional.empty();

        ControllerReadEvent(String name, Supplier<T> handler) {
            this.name = name;
            this.future = new CompletableFuture<T>();
            this.handler = handler;
        }

        CompletableFuture<T> future() {
            return future;
        }

        @Override
        public void run() throws Exception {
            long now = time.nanoseconds();
            controllerMetrics.updateEventQueueTime(NANOSECONDS.toMillis(now - eventCreatedTimeNs));
            startProcessingTimeNs = Optional.of(now);
            T value = handler.get();
            handleEventEnd(this.toString(), startProcessingTimeNs.get());
            future.complete(value);
        }

        @Override
        public void handleException(Throwable exception) {
            future.completeExceptionally(
                handleEventException(name, startProcessingTimeNs, exception));
        }

        @Override
        public String toString() {
            return name + "(" + System.identityHashCode(this) + ")";
        }
    }

    // VisibleForTesting
    ReplicationControlManager replicationControl() {
        return replicationControl;
    }

    // VisibleForTesting
    <T> CompletableFuture<T> appendReadEvent(String name, Supplier<T> handler) {
        ControllerReadEvent<T> event = new ControllerReadEvent<T>(name, handler);
        queue.append(event);
        return event.future();
    }

    <T> CompletableFuture<T> appendReadEvent(String name, long deadlineNs, Supplier<T> handler) {
        ControllerReadEvent<T> event = new ControllerReadEvent<T>(name, handler);
        queue.appendWithDeadline(deadlineNs, event);
        return event.future();
    }

    interface ControllerWriteOperation<T> {
        /**
         * Generate the metadata records needed to implement this controller write
         * operation.  In general, this operation should not modify the "hard state" of
         * the controller.  That modification will happen later on, when we replay the
         * records generated by this function.
         *
         * There are cases where this function modifies the "soft state" of the
         * controller.  Mainly, this happens when we process cluster heartbeats.
         *
         * This function also generates an RPC result.  In general, if the RPC resulted in
         * an error, the RPC result will be an error, and the generated record list will
         * be empty.  This would happen if we tried to create a topic with incorrect
         * parameters, for example.  Of course, partial errors are possible for batch
         * operations.
         *
         * @return              A result containing a list of records, and the RPC result.
         */
        ControllerResult<T> generateRecordsAndResult() throws Exception;

        /**
         * Once we've passed the records to the Raft layer, we will invoke this function
         * with the end offset at which those records were placed.  If there were no
         * records to write, we'll just pass the last write offset.
         */
        default void processBatchEndOffset(long offset) {}
    }

    /**
     * A controller event that modifies the controller state.
     */
    class ControllerWriteEvent<T> implements EventQueue.Event, DeferredEvent {
        private final String name;
        private final CompletableFuture<T> future;
        private final ControllerWriteOperation<T> op;
        private final long eventCreatedTimeNs = time.nanoseconds();
        private Optional<Long> startProcessingTimeNs = Optional.empty();
        private ControllerResultAndOffset<T> resultAndOffset;

        ControllerWriteEvent(String name, ControllerWriteOperation<T> op) {
            this.name = name;
            this.future = new CompletableFuture<T>();
            this.op = op;
            this.resultAndOffset = null;
        }

        CompletableFuture<T> future() {
            return future;
        }

        @Override
        public void run() throws Exception {
            long now = time.nanoseconds();
            controllerMetrics.updateEventQueueTime(NANOSECONDS.toMillis(now - eventCreatedTimeNs));
            int controllerEpoch = curClaimEpoch;
            if (controllerEpoch == -1) {
                throw newNotControllerException();
            }
            startProcessingTimeNs = Optional.of(now);
            ControllerResult<T> result = op.generateRecordsAndResult();
            if (result.records().isEmpty()) {
                op.processBatchEndOffset(writeOffset);
                // If the operation did not return any records, then it was actually just
                // a read after all, and not a read + write.  However, this read was done
                // from the latest in-memory state, which might contain uncommitted data.
                Optional<Long> maybeOffset = purgatory.highestPendingOffset();
                if (!maybeOffset.isPresent()) {
                    // If the purgatory is empty, there are no pending operations and no
                    // uncommitted state.  We can return immediately.
                    resultAndOffset = ControllerResultAndOffset.of(-1, result);
                    log.debug("Completing read-only operation {} immediately because " +
                        "the purgatory is empty.", this);
                    complete(null);
                    return;
                }
                // If there are operations in the purgatory, we want to wait for the latest
                // one to complete before returning our result to the user.
                resultAndOffset = ControllerResultAndOffset.of(maybeOffset.get(), result);
                log.debug("Read-only operation {} will be completed when the log " +
                    "reaches offset {}", this, resultAndOffset.offset());
            } else {
                // If the operation returned a batch of records, those records need to be
                // written before we can return our result to the user.  Here, we hand off
                // the batch of records to the raft client.  They will be written out
                // asynchronously.
                final long offset;
                if (result.isAtomic()) {
                    offset = raftClient.scheduleAtomicAppend(controllerEpoch, result.records());
                } else {
                    offset = raftClient.scheduleAppend(controllerEpoch, result.records());
                }
                op.processBatchEndOffset(offset);
                writeOffset = offset;
                resultAndOffset = ControllerResultAndOffset.of(offset, result);
                for (ApiMessageAndVersion message : result.records()) {
                    replay(message.message(), Optional.empty(), offset);
                }
                snapshotRegistry.getOrCreateSnapshot(offset);
                log.debug("Read-write operation {} will be completed when the log " +
                    "reaches offset {}.", this, resultAndOffset.offset());
            }
            purgatory.add(resultAndOffset.offset(), this);
        }

        @Override
        public void handleException(Throwable exception) {
            complete(exception);
        }

        @Override
        public void complete(Throwable exception) {
            if (exception == null) {
                handleEventEnd(this.toString(), startProcessingTimeNs.get());
                future.complete(resultAndOffset.response());
            } else {
                future.completeExceptionally(
                    handleEventException(name, startProcessingTimeNs, exception));
            }
        }

        @Override
        public String toString() {
            return name + "(" + System.identityHashCode(this) + ")";
        }
    }

    private <T> CompletableFuture<T> appendWriteEvent(String name,
                                                      long deadlineNs,
                                                      ControllerWriteOperation<T> op) {
        ControllerWriteEvent<T> event = new ControllerWriteEvent<>(name, op);
        queue.appendWithDeadline(deadlineNs, event);
        return event.future();
    }

    private <T> CompletableFuture<T> appendWriteEvent(String name,
                                                      ControllerWriteOperation<T> op) {
        ControllerWriteEvent<T> event = new ControllerWriteEvent<>(name, op);
        queue.append(event);
        return event.future();
    }

    class QuorumMetaLogListener implements RaftClient.Listener<ApiMessageAndVersion> {

        @Override
        public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
            appendRaftEvent("handleCommit[baseOffset=" + reader.baseOffset() + "]", () -> {
                try {
                    boolean isActiveController = curClaimEpoch != -1;
                    long processedRecordsSize = 0;
                    while (reader.hasNext()) {
                        Batch<ApiMessageAndVersion> batch = reader.next();
                        long offset = batch.lastOffset();
                        int epoch = batch.epoch();
                        List<ApiMessageAndVersion> messages = batch.records();

                        if (isActiveController) {
                            // If the controller is active, the records were already replayed,
                            // so we don't need to do it here.
                            log.debug("Completing purgatory items up to offset {} and epoch {}.", offset, epoch);

                            // Complete any events in the purgatory that were waiting for this offset.
                            purgatory.completeUpTo(offset);

                            // Delete all the in-memory snapshots that we no longer need.
                            // If we are writing a new snapshot, then we need to keep that around;
                            // otherwise, we should delete up to the current committed offset.
                            snapshotRegistry.deleteSnapshotsUpTo(
                                snapshotGeneratorManager.snapshotLastOffsetFromLog().orElse(offset));

                        } else {
                            // If the controller is a standby, replay the records that were
                            // created by the active controller.
                            if (log.isDebugEnabled()) {
                                if (log.isTraceEnabled()) {
                                    log.trace("Replaying commits from the active node up to " +
                                        "offset {} and epoch {}: {}.", offset, epoch, messages.stream()
                                        .map(ApiMessageAndVersion::toString)
                                        .collect(Collectors.joining(", ")));
                                } else {
                                    log.debug("Replaying commits from the active node up to " +
                                        "offset {} and epoch {}.", offset, epoch);
                                }
                            }
                            for (ApiMessageAndVersion messageAndVersion : messages) {
                                replay(messageAndVersion.message(), Optional.empty(), offset);
                            }
                        }

                        lastCommittedOffset = offset;
                        lastCommittedEpoch = epoch;
                        lastCommittedTimestamp = batch.appendTimestamp();
                        processedRecordsSize += batch.sizeInBytes();
                    }

                    maybeGenerateSnapshot(processedRecordsSize);
                } finally {
                    reader.close();
                }
            });
        }

        @Override
        public void handleSnapshot(SnapshotReader<ApiMessageAndVersion> reader) {
            appendRaftEvent(String.format("handleSnapshot[snapshotId=%s]", reader.snapshotId()), () -> {
                try {
                    boolean isActiveController = curClaimEpoch != -1;
                    if (isActiveController) {
                        throw new IllegalStateException(
                            String.format(
                                "Asked to load snasphot (%s) when it is the active controller (%s)",
                                reader.snapshotId(),
                                curClaimEpoch
                            )
                        );
                    }
                    log.info("Starting to replay snapshot ({}), from last commit offset ({}) and epoch ({})",
                        reader.snapshotId(), lastCommittedOffset, lastCommittedEpoch);

                    resetState();

                    while (reader.hasNext()) {
                        Batch<ApiMessageAndVersion> batch = reader.next();
                        long offset = batch.lastOffset();
                        List<ApiMessageAndVersion> messages = batch.records();

                        if (log.isDebugEnabled()) {
                            if (log.isTraceEnabled()) {
                                log.trace(
                                    "Replaying snapshot ({}) batch with last offset of {}: {}",
                                    reader.snapshotId(),
                                    offset,
                                    messages
                                      .stream()
                                      .map(ApiMessageAndVersion::toString)
                                      .collect(Collectors.joining(", "))
                                );
                            } else {
                                log.debug(
                                    "Replaying snapshot ({}) batch with last offset of {}",
                                    reader.snapshotId(),
                                    offset
                                );
                            }
                        }

                        for (ApiMessageAndVersion messageAndVersion : messages) {
                            replay(messageAndVersion.message(), Optional.of(reader.snapshotId()), offset);
                        }
                    }

                    lastCommittedOffset = reader.lastContainedLogOffset();
                    lastCommittedEpoch = reader.lastContainedLogEpoch();
                    lastCommittedTimestamp = reader.lastContainedLogTimestamp();
                    snapshotRegistry.getOrCreateSnapshot(lastCommittedOffset);
                } finally {
                    reader.close();
                }
            });
        }

        @Override
        public void handleLeaderChange(LeaderAndEpoch newLeader) {
            if (newLeader.isLeader(nodeId)) {
                final int newEpoch = newLeader.epoch();
                appendRaftEvent("handleLeaderChange[" + newEpoch + "]", () -> {
                    int curEpoch = curClaimEpoch;
                    if (curEpoch != -1) {
                        throw new RuntimeException("Tried to claim controller epoch " +
                            newEpoch + ", but we never renounced controller epoch " +
                            curEpoch);
                    }
                    log.info(
                        "Becoming the active controller at epoch {}, committed offset {} and committed epoch {}.",
                        newEpoch, lastCommittedOffset, lastCommittedEpoch
                    );

                    curClaimEpoch = newEpoch;
                    controllerMetrics.setActive(true);
                    writeOffset = lastCommittedOffset;
                    clusterControl.activate();

                    // Before switching to active, create an in-memory snapshot at the last committed offset. This is
                    // required because the active controller assumes that there is always an in-memory snapshot at the
                    // last committed offset.
                    snapshotRegistry.getOrCreateSnapshot(lastCommittedOffset);
                });
            } else if (curClaimEpoch != -1) {
                appendRaftEvent("handleRenounce[" + curClaimEpoch + "]", () -> {
                    log.warn("Renouncing the leadership at oldEpoch {} due to a metadata " +
                            "log event. Reverting to last committed offset {}.", curClaimEpoch,
                        lastCommittedOffset);
                    renounce();
                });
            }
        }

        @Override
        public void beginShutdown() {
            queue.beginShutdown("MetaLogManager.Listener");
        }

        private void appendRaftEvent(String name, Runnable runnable) {
            appendControlEvent(name, () -> {
                if (this != metaLogListener) {
                    log.debug("Ignoring {} raft event from an old registration", name);
                } else {
                    runnable.run();
                }
            });
        }
    }

    private void renounce() {
        curClaimEpoch = -1;
        controllerMetrics.setActive(false);
        purgatory.failAll(newNotControllerException());

        if (snapshotRegistry.hasSnapshot(lastCommittedOffset)) {
            snapshotRegistry.revertToSnapshot(lastCommittedOffset);
        } else {
            resetState();
            raftClient.unregister(metaLogListener);
            metaLogListener = new QuorumMetaLogListener();
            raftClient.register(metaLogListener);
        }

        writeOffset = -1;
        clusterControl.deactivate();
        cancelMaybeFenceReplicas();
    }

    private <T> void scheduleDeferredWriteEvent(String name, long deadlineNs,
                                                ControllerWriteOperation<T> op) {
        ControllerWriteEvent<T> event = new ControllerWriteEvent<>(name, op);
        queue.scheduleDeferred(name, new EarliestDeadlineFunction(deadlineNs), event);
        event.future.exceptionally(e -> {
            if (e instanceof UnknownServerException && e.getCause() != null &&
                    e.getCause() instanceof RejectedExecutionException) {
                log.error("Cancelling deferred write event {} because the event queue " +
                    "is now closed.", name);
                return null;
            } else if (e instanceof NotControllerException) {
                log.debug("Cancelling deferred write event {} because this controller " +
                    "is no longer active.", name);
                return null;
            }
            log.error("Unexpected exception while executing deferred write event {}. " +
                "Rescheduling for a minute from now.", name, e);
            scheduleDeferredWriteEvent(name,
                deadlineNs + NANOSECONDS.convert(1, TimeUnit.MINUTES), op);
            return null;
        });
    }

    static final String MAYBE_FENCE_REPLICAS = "maybeFenceReplicas";

    private void rescheduleMaybeFenceStaleBrokers() {
        long nextCheckTimeNs = clusterControl.heartbeatManager().nextCheckTimeNs();
        if (nextCheckTimeNs == Long.MAX_VALUE) {
            cancelMaybeFenceReplicas();
            return;
        }
        scheduleDeferredWriteEvent(MAYBE_FENCE_REPLICAS, nextCheckTimeNs, () -> {
            ControllerResult<Void> result = replicationControl.maybeFenceStaleBrokers();
            rescheduleMaybeFenceStaleBrokers();
            return result;
        });
    }

    private void cancelMaybeFenceReplicas() {
        queue.cancelDeferred(MAYBE_FENCE_REPLICAS);
    }

    @SuppressWarnings("unchecked")
    private void replay(ApiMessage message, Optional<OffsetAndEpoch> snapshotId, long offset) {
        try {
            MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
            switch (type) {
                case REGISTER_BROKER_RECORD:
                    clusterControl.replay((RegisterBrokerRecord) message);
                    break;
                case UNREGISTER_BROKER_RECORD:
                    clusterControl.replay((UnregisterBrokerRecord) message);
                    break;
                case TOPIC_RECORD:
                    replicationControl.replay((TopicRecord) message);
                    break;
                case PARTITION_RECORD:
                    replicationControl.replay((PartitionRecord) message);
                    break;
                case CONFIG_RECORD:
                    configurationControl.replay((ConfigRecord) message);
                    break;
                case PARTITION_CHANGE_RECORD:
                    replicationControl.replay((PartitionChangeRecord) message);
                    break;
                case FENCE_BROKER_RECORD:
                    clusterControl.replay((FenceBrokerRecord) message);
                    break;
                case UNFENCE_BROKER_RECORD:
                    clusterControl.replay((UnfenceBrokerRecord) message);
                    break;
                case REMOVE_TOPIC_RECORD:
                    replicationControl.replay((RemoveTopicRecord) message);
                    break;
                case FEATURE_LEVEL_RECORD:
                    featureControl.replay((FeatureLevelRecord) message);
                    break;
                case CLIENT_QUOTA_RECORD:
                    clientQuotaControlManager.replay((ClientQuotaRecord) message);
                    break;
                case PRODUCER_IDS_RECORD:
                    producerIdControlManager.replay((ProducerIdsRecord) message);
                    break;
                default:
                    throw new RuntimeException("Unhandled record type " + type);
            }
        } catch (Exception e) {
            if (snapshotId.isPresent()) {
                log.error("Error replaying record {} from snapshot {} at last offset {}.",
                    message.toString(), snapshotId.get(), offset, e);
            } else {
                log.error("Error replaying record {} at last offset {}.",
                    message.toString(), offset, e);
            }
        }
    }

    private void maybeGenerateSnapshot(long batchSizeInBytes) {
        newBytesSinceLastSnapshot += batchSizeInBytes;
        if (newBytesSinceLastSnapshot >= snapshotMaxNewRecordBytes &&
            snapshotGeneratorManager.generator == null
        ) {
            boolean isActiveController = curClaimEpoch != -1;
            if (!isActiveController) {
                // The active controller creates in-memory snapshot every time an uncommitted
                // batch gets appended. The in-active controller can be more efficient and only
                // create an in-memory snapshot when needed.
                snapshotRegistry.getOrCreateSnapshot(lastCommittedOffset);
            }

            log.info("Generating a snapshot that includes (epoch={}, offset={}) after {} committed bytes since the last snapshot.",
                lastCommittedEpoch, lastCommittedOffset, newBytesSinceLastSnapshot);

            snapshotGeneratorManager.createSnapshotGenerator(lastCommittedOffset, lastCommittedEpoch, lastCommittedTimestamp);
            newBytesSinceLastSnapshot = 0;
        }
    }

    private void resetState() {
        snapshotGeneratorManager.cancel();
        snapshotRegistry.reset();

        newBytesSinceLastSnapshot = 0;
        lastCommittedOffset = -1;
        lastCommittedEpoch = -1;
        lastCommittedTimestamp = -1;
    }

    private final LogContext logContext;

    private final Logger log;

    /**
     * The ID of this controller node.
     */
    private final int nodeId;

    /**
     * The single-threaded queue that processes all of our events.
     * It also processes timeouts.
     */
    private final KafkaEventQueue queue;

    /**
     * The Kafka clock object to use.
     */
    private final Time time;

    /**
     * The controller metrics.
     */
    private final ControllerMetrics controllerMetrics;

    /**
     * A registry for snapshot data.  This must be accessed only by the event queue thread.
     */
    private final SnapshotRegistry snapshotRegistry;

    /**
     * The purgatory which holds deferred operations which are waiting for the metadata
     * log's high water mark to advance.  This must be accessed only by the event queue thread.
     */
    private final ControllerPurgatory purgatory;

    /**
     * An object which stores the controller's dynamic configuration.
     * This must be accessed only by the event queue thread.
     */
    private final ConfigurationControlManager configurationControl;

    /**
     * An object which stores the controller's dynamic client quotas.
     * This must be accessed only by the event queue thread.
     */
    private final ClientQuotaControlManager clientQuotaControlManager;

    /**
     * An object which stores the controller's view of the cluster.
     * This must be accessed only by the event queue thread.
     */
    private final ClusterControlManager clusterControl;

    /**
     * An object which stores the controller's view of the cluster features.
     * This must be accessed only by the event queue thread.
     */
    private final FeatureControlManager featureControl;

    /**
     * An object which stores the controller's view of the latest producer ID
     * that has been generated. This must be accessed only by the event queue thread.
     */
    private final ProducerIdControlManager producerIdControlManager;

    /**
     * An object which stores the controller's view of topics and partitions.
     * This must be accessed only by the event queue thread.
     */
    private final ReplicationControlManager replicationControl;

    /**
     * Manages generating controller snapshots.
     */
    private final SnapshotGeneratorManager snapshotGeneratorManager = new SnapshotGeneratorManager();

    /**
     * The interface that we use to mutate the Raft log.
     */
    private final RaftClient<ApiMessageAndVersion> raftClient;

    /**
     * The interface that receives callbacks from the Raft log.  These callbacks are
     * invoked from the Raft thread(s), not from the controller thread. Control events
     * from this callbacks need to compare against this value to verify that the event
     * was not from a previous registration.
     */
    private QuorumMetaLogListener metaLogListener;

    /**
     * If this controller is active, this is the non-negative controller epoch.
     * Otherwise, this is -1.  This variable must be modified only from the controller
     * thread, but it can be read from other threads.
     */
    private volatile int curClaimEpoch;

    /**
     * The last offset we have committed, or -1 if we have not committed any offsets.
     */
    private long lastCommittedOffset = -1;

    /**
     * The epoch of the last offset we have committed, or -1 if we have not committed any offsets.
     */
    private int lastCommittedEpoch = -1;

    /**
     * The timestamp in milliseconds of the last batch we have committed, or -1 if we have not commmitted any offset.
     */
    private long lastCommittedTimestamp = -1;

    /**
     * If we have called scheduleWrite, this is the last offset we got back from it.
     */
    private long writeOffset;

    /**
     * Maximum number of bytes processed through handling commits before generating a snapshot.
     */
    private final long snapshotMaxNewRecordBytes;

    /**
     * Number of bytes processed through handling commits since the last snapshot was generated.
     */
    private long newBytesSinceLastSnapshot = 0;

    private QuorumController(LogContext logContext,
                             int nodeId,
                             KafkaEventQueue queue,
                             Time time,
                             Map<ConfigResource.Type, ConfigDef> configDefs,
                             RaftClient<ApiMessageAndVersion> raftClient,
                             Map<String, VersionRange> supportedFeatures,
                             short defaultReplicationFactor,
                             int defaultNumPartitions,
                             ReplicaPlacer replicaPlacer,
                             long snapshotMaxNewRecordBytes,
                             long sessionTimeoutNs,
                             ControllerMetrics controllerMetrics) {
        this.logContext = logContext;
        this.log = logContext.logger(QuorumController.class);
        this.nodeId = nodeId;
        this.queue = queue;
        this.time = time;
        this.controllerMetrics = controllerMetrics;
        this.snapshotRegistry = new SnapshotRegistry(logContext);
        this.purgatory = new ControllerPurgatory();
        this.configurationControl = new ConfigurationControlManager(logContext,
            snapshotRegistry, configDefs);
        this.clientQuotaControlManager = new ClientQuotaControlManager(snapshotRegistry);
        this.clusterControl = new ClusterControlManager(logContext, time,
            snapshotRegistry, sessionTimeoutNs, replicaPlacer);
        this.featureControl = new FeatureControlManager(supportedFeatures, snapshotRegistry);
        this.producerIdControlManager = new ProducerIdControlManager(clusterControl, snapshotRegistry);
        this.snapshotMaxNewRecordBytes = snapshotMaxNewRecordBytes;
        this.replicationControl = new ReplicationControlManager(snapshotRegistry,
            logContext, defaultReplicationFactor, defaultNumPartitions,
            configurationControl, clusterControl, controllerMetrics);
        this.raftClient = raftClient;
        this.metaLogListener = new QuorumMetaLogListener();
        this.curClaimEpoch = -1;
        this.writeOffset = -1L;

        resetState();

        this.raftClient.register(metaLogListener);
    }

    @Override
    public CompletableFuture<AlterIsrResponseData> alterIsr(AlterIsrRequestData request) {
        if (request.topics().isEmpty()) {
            return CompletableFuture.completedFuture(new AlterIsrResponseData());
        }
        return appendWriteEvent("alterIsr", () ->
            replicationControl.alterIsr(request));
    }

    @Override
    public CompletableFuture<CreateTopicsResponseData>
            createTopics(CreateTopicsRequestData request) {
        if (request.topics().isEmpty()) {
            return CompletableFuture.completedFuture(new CreateTopicsResponseData());
        }
        return appendWriteEvent("createTopics",
            time.nanoseconds() + NANOSECONDS.convert(request.timeoutMs(), MILLISECONDS),
            () -> replicationControl.createTopics(request));
    }

    @Override
    public CompletableFuture<Void> unregisterBroker(int brokerId) {
        return appendWriteEvent("unregisterBroker",
            () -> replicationControl.unregisterBroker(brokerId));
    }

    @Override
    public CompletableFuture<Map<String, ResultOrError<Uuid>>> findTopicIds(long deadlineNs,
                                                                            Collection<String> names) {
        if (names.isEmpty()) return CompletableFuture.completedFuture(Collections.emptyMap());
        return appendReadEvent("findTopicIds", deadlineNs,
            () -> replicationControl.findTopicIds(lastCommittedOffset, names));
    }

    @Override
    public CompletableFuture<Map<Uuid, ResultOrError<String>>> findTopicNames(long deadlineNs,
                                                                              Collection<Uuid> ids) {
        if (ids.isEmpty()) return CompletableFuture.completedFuture(Collections.emptyMap());
        return appendReadEvent("findTopicNames", deadlineNs,
            () -> replicationControl.findTopicNames(lastCommittedOffset, ids));
    }

    @Override
    public CompletableFuture<Map<Uuid, ApiError>> deleteTopics(long deadlineNs,
                                                               Collection<Uuid> ids) {
        if (ids.isEmpty()) return CompletableFuture.completedFuture(Collections.emptyMap());
        return appendWriteEvent("deleteTopics", deadlineNs,
            () -> replicationControl.deleteTopics(ids));
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ResultOrError<Map<String, String>>>>
            describeConfigs(Map<ConfigResource, Collection<String>> resources) {
        return appendReadEvent("describeConfigs", () ->
            configurationControl.describeConfigs(lastCommittedOffset, resources));
    }

    @Override
    public CompletableFuture<ElectLeadersResponseData>
            electLeaders(ElectLeadersRequestData request) {
        // If topicPartitions is null, we will try to trigger a new leader election on
        // all partitions (!).  But if it's empty, there is nothing to do.
        if (request.topicPartitions() != null && request.topicPartitions().isEmpty()) {
            return CompletableFuture.completedFuture(new ElectLeadersResponseData());
        }
        return appendWriteEvent("electLeaders",
            time.nanoseconds() + NANOSECONDS.convert(request.timeoutMs(), MILLISECONDS),
            () -> replicationControl.electLeaders(request));
    }

    @Override
    public CompletableFuture<FeatureMapAndEpoch> finalizedFeatures() {
        return appendReadEvent("getFinalizedFeatures",
            () -> featureControl.finalizedFeatures(lastCommittedOffset));
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ApiError>> incrementalAlterConfigs(
        Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges,
        boolean validateOnly) {
        if (configChanges.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        return appendWriteEvent("incrementalAlterConfigs", () -> {
            ControllerResult<Map<ConfigResource, ApiError>> result =
                configurationControl.incrementalAlterConfigs(configChanges);
            if (validateOnly) {
                return result.withoutRecords();
            } else {
                return result;
            }
        });
    }

    @Override
    public CompletableFuture<AlterPartitionReassignmentsResponseData>
            alterPartitionReassignments(AlterPartitionReassignmentsRequestData request) {
        if (request.topics().isEmpty()) {
            return CompletableFuture.completedFuture(new AlterPartitionReassignmentsResponseData());
        }
        return appendWriteEvent("alterPartitionReassignments",
            time.nanoseconds() + NANOSECONDS.convert(request.timeoutMs(), MILLISECONDS),
            () -> replicationControl.alterPartitionReassignments(request));
    }

    @Override
    public CompletableFuture<ListPartitionReassignmentsResponseData>
            listPartitionReassignments(ListPartitionReassignmentsRequestData request) {
        if (request.topics() != null && request.topics().isEmpty()) {
            return CompletableFuture.completedFuture(
                new ListPartitionReassignmentsResponseData().setErrorMessage(null));
        }
        return appendReadEvent("listPartitionReassignments",
            time.nanoseconds() + NANOSECONDS.convert(request.timeoutMs(), MILLISECONDS),
            () -> replicationControl.listPartitionReassignments(request.topics()));
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ApiError>> legacyAlterConfigs(
            Map<ConfigResource, Map<String, String>> newConfigs, boolean validateOnly) {
        if (newConfigs.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        return appendWriteEvent("legacyAlterConfigs", () -> {
            ControllerResult<Map<ConfigResource, ApiError>> result =
                configurationControl.legacyAlterConfigs(newConfigs);
            if (validateOnly) {
                return result.withoutRecords();
            } else {
                return result;
            }
        });
    }

    @Override
    public CompletableFuture<BrokerHeartbeatReply>
            processBrokerHeartbeat(BrokerHeartbeatRequestData request) {
        return appendWriteEvent("processBrokerHeartbeat",
            new ControllerWriteOperation<BrokerHeartbeatReply>() {
                private final int brokerId = request.brokerId();
                private boolean inControlledShutdown = false;

                @Override
                public ControllerResult<BrokerHeartbeatReply> generateRecordsAndResult() {
                    ControllerResult<BrokerHeartbeatReply> result = replicationControl.
                        processBrokerHeartbeat(request, lastCommittedOffset);
                    inControlledShutdown = result.response().inControlledShutdown();
                    rescheduleMaybeFenceStaleBrokers();
                    return result;
                }

                @Override
                public void processBatchEndOffset(long offset) {
                    if (inControlledShutdown) {
                        clusterControl.heartbeatManager().
                            updateControlledShutdownOffset(brokerId, offset);
                    }
                }
            });
    }

    @Override
    public CompletableFuture<BrokerRegistrationReply>
            registerBroker(BrokerRegistrationRequestData request) {
        return appendWriteEvent("registerBroker", () -> {
            ControllerResult<BrokerRegistrationReply> result = clusterControl.
                registerBroker(request, writeOffset + 1, featureControl.
                    finalizedFeatures(Long.MAX_VALUE));
            rescheduleMaybeFenceStaleBrokers();
            return result;
        });
    }

    @Override
    public CompletableFuture<Map<ClientQuotaEntity, ApiError>> alterClientQuotas(
            Collection<ClientQuotaAlteration> quotaAlterations, boolean validateOnly) {
        if (quotaAlterations.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        return appendWriteEvent("alterClientQuotas", () -> {
            ControllerResult<Map<ClientQuotaEntity, ApiError>> result =
                clientQuotaControlManager.alterClientQuotas(quotaAlterations);
            if (validateOnly) {
                return result.withoutRecords();
            } else {
                return result;
            }
        });
    }

    @Override
    public CompletableFuture<AllocateProducerIdsResponseData> allocateProducerIds(
            AllocateProducerIdsRequestData request) {
        return appendWriteEvent("allocateProducerIds",
            () -> producerIdControlManager.generateNextProducerId(request.brokerId(), request.brokerEpoch()))
            .thenApply(result -> new AllocateProducerIdsResponseData()
                    .setProducerIdStart(result.producerIdStart())
                    .setProducerIdLen(result.producerIdLen()));
    }

    @Override
    public CompletableFuture<List<CreatePartitionsTopicResult>>
            createPartitions(long deadlineNs, List<CreatePartitionsTopic> topics) {
        if (topics.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        return appendWriteEvent("createPartitions", deadlineNs,
            () -> replicationControl.createPartitions(topics));
    }

    @Override
    public CompletableFuture<Long> beginWritingSnapshot() {
        CompletableFuture<Long> future = new CompletableFuture<>();
        appendControlEvent("beginWritingSnapshot", () -> {
            if (snapshotGeneratorManager.generator == null) {
                snapshotGeneratorManager.createSnapshotGenerator(
                    lastCommittedOffset,
                    lastCommittedEpoch,
                    lastCommittedTimestamp
                );
            }
            future.complete(snapshotGeneratorManager.generator.lastContainedLogOffset());
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> waitForReadyBrokers(int minBrokers) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        appendControlEvent("waitForReadyBrokers", () -> {
            clusterControl.addReadyBrokersFuture(future, minBrokers);
        });
        return future;
    }

    @Override
    public void beginShutdown() {
        queue.beginShutdown("QuorumController#beginShutdown");
    }

    public int nodeId() {
        return nodeId;
    }

    @Override
    public int curClaimEpoch() {
        return curClaimEpoch;
    }

    @Override
    public void close() throws InterruptedException {
        queue.close();
        controllerMetrics.close();
    }

    // VisibleForTesting
    CountDownLatch pause() {
        final CountDownLatch latch = new CountDownLatch(1);
        appendControlEvent("pause", () -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                log.info("Interrupted while waiting for unpause.", e);
            }
        });
        return latch;
    }

    // VisibleForTesting
    Time time() {
        return time;
    }
}
