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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.UnknownServerException;
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
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.QuotaRecord;
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
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metadata.FeatureMapAndEpoch;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.metalog.MetaLogLeader;
import org.apache.kafka.metalog.MetaLogListener;
import org.apache.kafka.metalog.MetaLogManager;
import org.apache.kafka.queue.EventQueue.EarliestDeadlineFunction;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.slf4j.Logger;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
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
        private MetaLogManager logManager = null;
        private Map<String, VersionRange> supportedFeatures = Collections.emptyMap();
        private short defaultReplicationFactor = 3;
        private int defaultNumPartitions = 1;
        private ReplicaPlacementPolicy replicaPlacementPolicy =
            new SimpleReplicaPlacementPolicy(new Random());
        private Function<Long, SnapshotWriter> snapshotWriterBuilder;
        private SnapshotReader snapshotReader;
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

        public Builder setLogManager(MetaLogManager logManager) {
            this.logManager = logManager;
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

        public Builder setReplicaPlacementPolicy(ReplicaPlacementPolicy replicaPlacementPolicy) {
            this.replicaPlacementPolicy = replicaPlacementPolicy;
            return this;
        }

        public Builder setSnapshotWriterBuilder(Function<Long, SnapshotWriter> snapshotWriterBuilder) {
            this.snapshotWriterBuilder = snapshotWriterBuilder;
            return this;
        }

        public Builder setSnapshotReader(SnapshotReader snapshotReader) {
            this.snapshotReader = snapshotReader;
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
            if (logManager == null) {
                throw new RuntimeException("You must set a metadata log manager.");
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
            if (snapshotWriterBuilder == null) {
                snapshotWriterBuilder = new NoOpSnapshotWriterBuilder();
            }
            if (snapshotReader == null) {
                snapshotReader = new EmptySnapshotReader(-1);
            }
            KafkaEventQueue queue = null;
            try {
                queue = new KafkaEventQueue(time, logContext, threadNamePrefix);
                return new QuorumController(logContext, nodeId, queue, time, configDefs,
                    logManager, supportedFeatures, defaultReplicationFactor,
                    defaultNumPartitions, replicaPlacementPolicy, snapshotWriterBuilder,
                    snapshotReader, sessionTimeoutNs, controllerMetrics);
            } catch (Exception e) {
                Utils.closeQuietly(queue, "event queue");
                throw e;
            } finally {
                Utils.closeQuietly(snapshotReader, "snapshotReader");
            }
        }
    }

    private static final String ACTIVE_CONTROLLER_EXCEPTION_TEXT_PREFIX =
        "The active controller appears to be node ";

    private NotControllerException newNotControllerException() {
        int latestController = logManager.leader().nodeId();
        if (latestController < 0) {
            return new NotControllerException("No controller appears to be active.");
        } else {
            return new NotControllerException(ACTIVE_CONTROLLER_EXCEPTION_TEXT_PREFIX +
                latestController);
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
        private final Function<Long, SnapshotWriter> writerBuilder;
        private final ExponentialBackoff exponentialBackoff =
            new ExponentialBackoff(10, 2, 5000, 0);
        private SnapshotGenerator generator = null;

        SnapshotGeneratorManager(Function<Long, SnapshotWriter> writerBuilder) {
            this.writerBuilder = writerBuilder;
        }

        void createSnapshotGenerator(long epoch) {
            if (generator != null) {
                throw new RuntimeException("Snapshot generator already exists.");
            }
            if (!snapshotRegistry.hasSnapshot(epoch)) {
                throw new RuntimeException("Can't generate a snapshot at epoch " + epoch +
                    " because no such epoch exists in the snapshot registry.");
            }
            generator = new SnapshotGenerator(logContext,
                writerBuilder.apply(epoch),
                MAX_BATCHES_PER_GENERATE_CALL,
                exponentialBackoff,
                Arrays.asList(
                    new Section("features", featureControl.iterator(epoch)),
                    new Section("cluster", clusterControl.iterator(epoch)),
                    new Section("replication", replicationControl.iterator(epoch)),
                    new Section("configuration", configurationControl.iterator(epoch)),
                    new Section("clientQuotas", clientQuotaControlManager.iterator(epoch))));
            reschedule(0);
        }

        void cancel() {
            if (generator == null) return;
            log.error("Cancelling snapshot {}", generator.epoch());
            generator.writer().close();
            generator = null;
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
                log.error("Error while generating snapshot {}", generator.epoch(), e);
                generator.writer().close();
                generator = null;
                return;
            }
            if (!nextDelay.isPresent()) {
                try {
                    generator.writer().completeSnapshot();
                    log.info("Finished generating snapshot {}.", generator.epoch());
                } catch (Exception e) {
                    log.error("Error while completing snapshot {}", generator.epoch(), e);
                } finally {
                    generator.writer().close();
                    generator = null;
                }
                return;
            }
            reschedule(nextDelay.getAsLong());
        }

        long snapshotEpoch() {
            if (generator == null) {
                return Long.MAX_VALUE;
            }
            return generator.epoch();
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
            long controllerEpoch = curClaimEpoch;
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
                // the batch of records to the metadata log manager.  They will be written
                // out asynchronously.
                final long offset;
                if (result.isAtomic()) {
                    offset = logManager.scheduleAtomicWrite(controllerEpoch, result.records());
                } else {
                    offset = logManager.scheduleWrite(controllerEpoch, result.records());
                }
                op.processBatchEndOffset(offset);
                writeOffset = offset;
                resultAndOffset = ControllerResultAndOffset.of(offset, result);
                for (ApiMessageAndVersion message : result.records()) {
                    replay(message.message(), -1, offset);
                }
                snapshotRegistry.createSnapshot(offset);
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
                                                      long timeoutMs,
                                                      ControllerWriteOperation<T> op) {
        ControllerWriteEvent<T> event = new ControllerWriteEvent<>(name, op);
        queue.appendWithDeadline(time.nanoseconds() +
            NANOSECONDS.convert(timeoutMs, TimeUnit.MILLISECONDS), event);
        return event.future();
    }

    private <T> CompletableFuture<T> appendWriteEvent(String name,
                                                      ControllerWriteOperation<T> op) {
        ControllerWriteEvent<T> event = new ControllerWriteEvent<>(name, op);
        queue.append(event);
        return event.future();
    }

    class QuorumMetaLogListener implements MetaLogListener {
        @Override
        public void handleCommits(long offset, List<ApiMessage> messages) {
            appendControlEvent("handleCommits[" + offset + "]", () -> {
                if (curClaimEpoch == -1) {
                    // If the controller is a standby, replay the records that were
                    // created by the active controller.
                    if (log.isDebugEnabled()) {
                        if (log.isTraceEnabled()) {
                            log.trace("Replaying commits from the active node up to " +
                                "offset {}: {}.", offset, messages.stream().
                                map(m -> m.toString()).collect(Collectors.joining(", ")));
                        } else {
                            log.debug("Replaying commits from the active node up to " +
                                "offset {}.", offset);
                        }
                    }
                    for (ApiMessage message : messages) {
                        replay(message, -1, offset);
                    }
                } else {
                    // If the controller is active, the records were already replayed,
                    // so we don't need to do it here.
                    log.debug("Completing purgatory items up to offset {}.", offset);

                    // Complete any events in the purgatory that were waiting for this offset.
                    purgatory.completeUpTo(offset);

                    // Delete all the in-memory snapshots that we no longer need.
                    // If we are writing a new snapshot, then we need to keep that around;
                    // otherwise, we should delete up to the current committed offset.
                    snapshotRegistry.deleteSnapshotsUpTo(
                        Math.min(offset, snapshotGeneratorManager.snapshotEpoch()));
                }
                lastCommittedOffset = offset;
            });
        }

        @Override
        public void handleNewLeader(MetaLogLeader newLeader) {
            if (newLeader.nodeId() == nodeId) {
                final long newEpoch = newLeader.epoch();
                appendControlEvent("handleClaim[" + newEpoch + "]", () -> {
                    long curEpoch = curClaimEpoch;
                    if (curEpoch != -1) {
                        throw new RuntimeException("Tried to claim controller epoch " +
                            newEpoch + ", but we never renounced controller epoch " +
                            curEpoch);
                    }
                    log.warn("Becoming active at controller epoch {}.", newEpoch);
                    curClaimEpoch = newEpoch;
                    controllerMetrics.setActive(true);
                    writeOffset = lastCommittedOffset;
                    clusterControl.activate();
                });
            }
        }

        @Override
        public void handleRenounce(long oldEpoch) {
            appendControlEvent("handleRenounce[" + oldEpoch + "]", () -> {
                if (curClaimEpoch == oldEpoch) {
                    log.warn("Renouncing the leadership at oldEpoch {} due to a metadata " +
                             "log event. Reverting to last committed offset {}.", curClaimEpoch,
                             lastCommittedOffset);
                    renounce();
                }
            });
        }

        @Override
        public void beginShutdown() {
            queue.beginShutdown("MetaLogManager.Listener");
        }
    }

    private void renounce() {
        curClaimEpoch = -1;
        controllerMetrics.setActive(false);
        purgatory.failAll(newNotControllerException());
        snapshotRegistry.revertToSnapshot(lastCommittedOffset);
        snapshotRegistry.deleteSnapshotsUpTo(lastCommittedOffset);
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
    private void replay(ApiMessage message, long snapshotEpoch, long offset) {
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
                case QUOTA_RECORD:
                    clientQuotaControlManager.replay((QuotaRecord) message);
                    break;
                default:
                    throw new RuntimeException("Unhandled record type " + type);
            }
        } catch (Exception e) {
            if (snapshotEpoch < 0) {
                log.error("Error replaying record {} at offset {}.",
                    message.toString(), offset, e);
            } else {
                log.error("Error replaying record {} from snapshot {} at index {}.",
                    message.toString(), snapshotEpoch, offset, e);
            }
        }
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
     * An object which stores the controller's view of topics and partitions.
     * This must be accessed only by the event queue thread.
     */
    private final ReplicationControlManager replicationControl;

    /**
     * Manages generating controller snapshots.
     */
    private final SnapshotGeneratorManager snapshotGeneratorManager;

    /**
     * The interface that we use to mutate the Raft log.
     */
    private final MetaLogManager logManager;

    /**
     * The interface that receives callbacks from the Raft log.  These callbacks are
     * invoked from the Raft thread(s), not from the controller thread.
     */
    private final QuorumMetaLogListener metaLogListener;

    /**
     * If this controller is active, this is the non-negative controller epoch.
     * Otherwise, this is -1.  This variable must be modified only from the controller
     * thread, but it can be read from other threads.
     */
    private volatile long curClaimEpoch;

    /**
     * The last offset we have committed, or -1 if we have not committed any offsets.
     */
    private long lastCommittedOffset;

    /**
     * If we have called scheduleWrite, this is the last offset we got back from it.
     */
    private long writeOffset;

    private QuorumController(LogContext logContext,
                             int nodeId,
                             KafkaEventQueue queue,
                             Time time,
                             Map<ConfigResource.Type, ConfigDef> configDefs,
                             MetaLogManager logManager,
                             Map<String, VersionRange> supportedFeatures,
                             short defaultReplicationFactor,
                             int defaultNumPartitions,
                             ReplicaPlacementPolicy replicaPlacementPolicy,
                             Function<Long, SnapshotWriter> snapshotWriterBuilder,
                             SnapshotReader snapshotReader,
                             long sessionTimeoutNs,
                             ControllerMetrics controllerMetrics) throws Exception {
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
            snapshotRegistry, sessionTimeoutNs, replicaPlacementPolicy);
        this.featureControl = new FeatureControlManager(supportedFeatures, snapshotRegistry);
        this.snapshotGeneratorManager = new SnapshotGeneratorManager(snapshotWriterBuilder);
        this.replicationControl = new ReplicationControlManager(snapshotRegistry,
            logContext, defaultReplicationFactor, defaultNumPartitions,
            configurationControl, clusterControl);
        this.logManager = logManager;
        this.metaLogListener = new QuorumMetaLogListener();
        this.curClaimEpoch = -1L;
        this.lastCommittedOffset = snapshotReader.epoch();
        this.writeOffset = -1L;

        while (snapshotReader.hasNext()) {
            List<ApiMessage> batch = snapshotReader.next();
            long index = 0;
            for (ApiMessage message : batch) {
                replay(message, snapshotReader.epoch(), index++);
            }
        }
        snapshotRegistry.createSnapshot(lastCommittedOffset);
        this.logManager.register(metaLogListener);
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
        return appendWriteEvent("createTopics", () ->
            replicationControl.createTopics(request));
    }

    @Override
    public CompletableFuture<Void> unregisterBroker(int brokerId) {
        return appendWriteEvent("unregisterBroker",
            () -> replicationControl.unregisterBroker(brokerId));
    }

    @Override
    public CompletableFuture<Map<String, ResultOrError<Uuid>>> findTopicIds(Collection<String> names) {
        if (names.isEmpty()) return CompletableFuture.completedFuture(Collections.emptyMap());
        return appendReadEvent("findTopicIds",
            () -> replicationControl.findTopicIds(lastCommittedOffset, names));
    }

    @Override
    public CompletableFuture<Map<Uuid, ResultOrError<String>>> findTopicNames(Collection<Uuid> ids) {
        if (ids.isEmpty()) return CompletableFuture.completedFuture(Collections.emptyMap());
        return appendReadEvent("findTopicNames",
            () -> replicationControl.findTopicNames(lastCommittedOffset, ids));
    }

    @Override
    public CompletableFuture<Map<Uuid, ApiError>> deleteTopics(Collection<Uuid> ids) {
        if (ids.isEmpty()) return CompletableFuture.completedFuture(Collections.emptyMap());
        return appendWriteEvent("deleteTopics",
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
        return appendWriteEvent("electLeaders", request.timeoutMs(),
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
        CompletableFuture<AlterPartitionReassignmentsResponseData> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException());
        return future;
    }

    @Override
    public CompletableFuture<ListPartitionReassignmentsResponseData>
            listPartitionReassignments(ListPartitionReassignmentsRequestData request) {
        CompletableFuture<ListPartitionReassignmentsResponseData> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedOperationException());
        return future;
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
    public CompletableFuture<List<CreatePartitionsTopicResult>>
            createPartitions(List<CreatePartitionsTopic> topics) {
        if (topics.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        return appendWriteEvent("createPartitions", () ->
            replicationControl.createPartitions(topics));
    }

    @Override
    public CompletableFuture<Long> beginWritingSnapshot() {
        CompletableFuture<Long> future = new CompletableFuture<>();
        appendControlEvent("beginWritingSnapshot", () -> {
            if (snapshotGeneratorManager.generator == null) {
                snapshotGeneratorManager.createSnapshotGenerator(lastCommittedOffset);
            }
            future.complete(snapshotGeneratorManager.generator.epoch());
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
    public long curClaimEpoch() {
        return curClaimEpoch;
    }

    @Override
    public void close() throws InterruptedException {
        queue.close();
    }
}
