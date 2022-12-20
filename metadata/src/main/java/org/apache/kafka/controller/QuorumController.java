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
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.BrokerIdNotRegisteredException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionResponseData;
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
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateFeaturesResponseData;
import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.NoOpRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.ProducerIdsRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.placement.ReplicaPlacer;
import org.apache.kafka.metadata.placement.StripedReplicaPlacer;
import org.apache.kafka.queue.EventQueue.EarliestDeadlineFunction;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.server.policy.AlterConfigPolicy;
import org.apache.kafka.server.policy.CreateTopicPolicy;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
 *
 * The QuorumController uses the "metadata.version" feature flag as a mechanism to control
 * the usage of new log record schemas. Starting with 3.3, this version must be set before
 * the controller can fully initialize.
 */
public final class QuorumController implements Controller {
    private final static int MAX_RECORDS_PER_BATCH = 10000;

    /**
     * A builder class which creates the QuorumController.
     */
    static public class Builder {
        private final int nodeId;
        private final String clusterId;
        private FaultHandler fatalFaultHandler = null;
        private Time time = Time.SYSTEM;
        private String threadNamePrefix = null;
        private LogContext logContext = null;
        private KafkaConfigSchema configSchema = KafkaConfigSchema.EMPTY;
        private RaftClient<ApiMessageAndVersion> raftClient = null;
        private QuorumFeatures quorumFeatures = null;
        private short defaultReplicationFactor = 3;
        private int defaultNumPartitions = 1;
        private ReplicaPlacer replicaPlacer = new StripedReplicaPlacer(new Random());
        private OptionalLong leaderImbalanceCheckIntervalNs = OptionalLong.empty();
        private OptionalLong maxIdleIntervalNs = OptionalLong.empty();
        private long sessionTimeoutNs = ClusterControlManager.DEFAULT_SESSION_TIMEOUT_NS;
        private ControllerMetrics controllerMetrics = null;
        private Optional<CreateTopicPolicy> createTopicPolicy = Optional.empty();
        private Optional<AlterConfigPolicy> alterConfigPolicy = Optional.empty();
        private ConfigurationValidator configurationValidator = ConfigurationValidator.NO_OP;
        private Optional<ClusterMetadataAuthorizer> authorizer = Optional.empty();
        private Map<String, Object> staticConfig = Collections.emptyMap();
        private BootstrapMetadata bootstrapMetadata = null;
        private int maxRecordsPerBatch = MAX_RECORDS_PER_BATCH;
        private boolean zkMigrationEnabled = false;

        public Builder(int nodeId, String clusterId) {
            this.nodeId = nodeId;
            this.clusterId = clusterId;
        }

        public Builder setFatalFaultHandler(FaultHandler fatalFaultHandler) {
            this.fatalFaultHandler = fatalFaultHandler;
            return this;
        }

        public int nodeId() {
            return nodeId;
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

        public Builder setConfigSchema(KafkaConfigSchema configSchema) {
            this.configSchema = configSchema;
            return this;
        }

        public Builder setRaftClient(RaftClient<ApiMessageAndVersion> logManager) {
            this.raftClient = logManager;
            return this;
        }

        public Builder setQuorumFeatures(QuorumFeatures quorumFeatures) {
            this.quorumFeatures = quorumFeatures;
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

        public Builder setLeaderImbalanceCheckIntervalNs(OptionalLong value) {
            this.leaderImbalanceCheckIntervalNs = value;
            return this;
        }

        public Builder setMaxIdleIntervalNs(OptionalLong value) {
            this.maxIdleIntervalNs = value;
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

        public Builder setBootstrapMetadata(BootstrapMetadata bootstrapMetadata) {
            this.bootstrapMetadata = bootstrapMetadata;
            return this;
        }

        public Builder setMaxRecordsPerBatch(int maxRecordsPerBatch) {
            this.maxRecordsPerBatch = maxRecordsPerBatch;
            return this;
        }

        public Builder setCreateTopicPolicy(Optional<CreateTopicPolicy> createTopicPolicy) {
            this.createTopicPolicy = createTopicPolicy;
            return this;
        }

        public Builder setAlterConfigPolicy(Optional<AlterConfigPolicy> alterConfigPolicy) {
            this.alterConfigPolicy = alterConfigPolicy;
            return this;
        }

        public Builder setConfigurationValidator(ConfigurationValidator configurationValidator) {
            this.configurationValidator = configurationValidator;
            return this;
        }

        public Builder setAuthorizer(ClusterMetadataAuthorizer authorizer) {
            this.authorizer = Optional.of(authorizer);
            return this;
        }

        public Builder setStaticConfig(Map<String, Object> staticConfig) {
            this.staticConfig = staticConfig;
            return this;
        }

        public Builder setZkMigrationEnabled(boolean zkMigrationEnabled) {
            this.zkMigrationEnabled = zkMigrationEnabled;
            return this;
        }

        @SuppressWarnings("unchecked")
        public QuorumController build() throws Exception {
            if (raftClient == null) {
                throw new IllegalStateException("You must set a raft client.");
            } else if (bootstrapMetadata == null) {
                throw new IllegalStateException("You must specify an initial metadata.version using the kafka-storage tool.");
            } else if (quorumFeatures == null) {
                throw new IllegalStateException("You must specify the quorum features");
            } else if (fatalFaultHandler == null) {
                throw new IllegalStateException("You must specify a fatal fault handler.");
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
                queue = new KafkaEventQueue(time, logContext, threadNamePrefix + "QuorumController");
                return new QuorumController(
                    fatalFaultHandler,
                    logContext,
                    nodeId,
                    clusterId,
                    queue,
                    time,
                    configSchema,
                    raftClient,
                    quorumFeatures,
                    defaultReplicationFactor,
                    defaultNumPartitions,
                    replicaPlacer,
                    leaderImbalanceCheckIntervalNs,
                    maxIdleIntervalNs,
                    sessionTimeoutNs,
                    controllerMetrics,
                    createTopicPolicy,
                    alterConfigPolicy,
                    configurationValidator,
                    authorizer,
                    staticConfig,
                    bootstrapMetadata,
                    maxRecordsPerBatch,
                    zkMigrationEnabled
                );
            } catch (Exception e) {
                Utils.closeQuietly(queue, "event queue");
                throw e;
            }
        }
    }

    /**
     * Checks that a configuration resource exists.
     * <p>
     * This object must be used only from the controller event thread.
     */
    class ConfigResourceExistenceChecker implements Consumer<ConfigResource> {
        @Override
        public void accept(ConfigResource configResource) {
            switch (configResource.type()) {
                case BROKER_LOGGER:
                    break;
                case BROKER:
                    // Cluster configs are always allowed.
                    if (configResource.name().isEmpty()) break;

                    // Otherwise, check that the node ID is valid.
                    int nodeId;
                    try {
                        nodeId = Integer.parseInt(configResource.name());
                    } catch (NumberFormatException e) {
                        throw new InvalidRequestException("Invalid broker name " +
                            configResource.name());
                    }
                    if (!(clusterControl.brokerRegistrations().containsKey(nodeId) ||
                            featureControl.isControllerId(nodeId))) {
                        throw new BrokerIdNotRegisteredException("No node with id " +
                            nodeId + " found.");
                    }
                    break;
                case TOPIC:
                    if (replicationControl.getTopicId(configResource.name()) == null) {
                        throw new UnknownTopicOrPartitionException("The topic '" +
                            configResource.name() + "' does not exist.");
                    }
                    break;
                default:
                    break;
            }
        }
    }

    public static final String CONTROLLER_THREAD_SUFFIX = "QuorumControllerEventHandler";

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
                                           OptionalLong startProcessingTimeNs,
                                           Throwable exception) {
        if (!startProcessingTimeNs.isPresent()) {
            log.error("{}: unable to start processing because of {}.", name,
                exception.getClass().getSimpleName());
            if (exception instanceof ApiException) {
                return exception;
            } else {
                return new UnknownServerException(exception);
            }
        }
        long endProcessingTime = time.nanoseconds();
        long deltaNs = endProcessingTime - startProcessingTimeNs.getAsLong();
        long deltaUs = MICROSECONDS.convert(deltaNs, NANOSECONDS);
        if (exception instanceof ApiException) {
            log.info("{}: failed with {} in {} us", name,
                exception.getClass().getSimpleName(), deltaUs);
            return exception;
        }
        if (isActiveController()) {
            log.warn("{}: failed with unknown server exception {} at epoch {} in {} us.  " +
                    "Renouncing leadership and reverting to the last committed offset {}.",
                    name, exception.getClass().getSimpleName(), curClaimEpoch, deltaUs,
                    lastCommittedOffset, exception);
            renounce();
        } else {
            log.warn("{}: failed with unknown server exception {} in {} us.  " +
                    "The controller is already in standby mode.",
                    name, exception.getClass().getSimpleName(), deltaUs,
                    exception);
        }
        return new UnknownServerException(exception);
    }

    /**
     * A controller event for handling internal state changes, such as Raft inputs.
     */
    class ControllerEvent implements EventQueue.Event {
        private final String name;
        private final Runnable handler;
        private final long eventCreatedTimeNs = time.nanoseconds();
        private OptionalLong startProcessingTimeNs = OptionalLong.empty();

        ControllerEvent(String name, Runnable handler) {
            this.name = name;
            this.handler = handler;
        }

        @Override
        public void run() throws Exception {
            long now = time.nanoseconds();
            controllerMetrics.updateEventQueueTime(NANOSECONDS.toMillis(now - eventCreatedTimeNs));
            startProcessingTimeNs = OptionalLong.of(now);
            log.debug("Executing {}.", this);
            handler.run();
            handleEventEnd(this.toString(), startProcessingTimeNs.getAsLong());
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
        ControllerEvent event = new ControllerEvent(name, handler);
        queue.append(event);
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
        private OptionalLong startProcessingTimeNs = OptionalLong.empty();

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
            startProcessingTimeNs = OptionalLong.of(now);
            T value = handler.get();
            handleEventEnd(this.toString(), startProcessingTimeNs.getAsLong());
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

    // Visible for testing
    ReplicationControlManager replicationControl() {
        return replicationControl;
    }

    // Visible for testing
    ClusterControlManager clusterControl() {
        return clusterControl;
    }

    // Visible for testing
    FeatureControlManager featureControl() {
        return featureControl;
    }

    // Visible for testing
    ConfigurationControlManager configurationControl() {
        return configurationControl;
    }

    <T> CompletableFuture<T> appendReadEvent(
        String name,
        OptionalLong deadlineNs,
        Supplier<T> handler
    ) {
        ControllerReadEvent<T> event = new ControllerReadEvent<T>(name, handler);
        if (deadlineNs.isPresent()) {
            queue.appendWithDeadline(deadlineNs.getAsLong(), event);
        } else {
            queue.append(event);
        }
        return event.future();
    }

    interface ControllerWriteOperation<T> {
        /**
         * Generate the metadata records needed to implement this controller write
         * operation.  In general, this operation should not modify the "hard state" of
         * the controller.  That modification will happen later on, when we replay the
         * records generated by this function.
         * <p>
         * There are cases where this function modifies the "soft state" of the
         * controller.  Mainly, this happens when we process cluster heartbeats.
         * <p>
         * This function also generates an RPC result.  In general, if the RPC resulted in
         * an error, the RPC result will be an error, and the generated record list will
         * be empty.  This would happen if we tried to create a topic with incorrect
         * parameters, for example.  Of course, partial errors are possible for batch
         * operations.
         *
         * @return A result containing a list of records, and the RPC result.
         */
        ControllerResult<T> generateRecordsAndResult() throws Exception;

        /**
         * Once we've passed the records to the Raft layer, we will invoke this function
         * with the end offset at which those records were placed.  If there were no
         * records to write, we'll just pass the last write offset.
         */
        default void processBatchEndOffset(long offset) {
        }
    }

    /**
     * A controller event that modifies the controller state.
     */
    class ControllerWriteEvent<T> implements EventQueue.Event, DeferredEvent {
        private final String name;
        private final CompletableFuture<T> future;
        private final ControllerWriteOperation<T> op;
        private final long eventCreatedTimeNs = time.nanoseconds();
        private final boolean deferred;
        private OptionalLong startProcessingTimeNs = OptionalLong.empty();
        private ControllerResultAndOffset<T> resultAndOffset;

        ControllerWriteEvent(String name, ControllerWriteOperation<T> op) {
            this(name, op, false);
        }

        ControllerWriteEvent(String name, ControllerWriteOperation<T> op, boolean deferred) {
            this.name = name;
            this.future = new CompletableFuture<T>();
            this.op = op;
            this.deferred = deferred;
            this.resultAndOffset = null;
        }

        CompletableFuture<T> future() {
            return future;
        }

        @Override
        public void run() throws Exception {
            long now = time.nanoseconds();
            if (!deferred) {
                // We exclude deferred events from the event queue time metric to prevent
                // incorrectly including the deferral time in the queue time.
                controllerMetrics.updateEventQueueTime(NANOSECONDS.toMillis(now - eventCreatedTimeNs));
            }
            int controllerEpoch = curClaimEpoch;
            if (!isActiveController()) {
                throw newNotControllerException();
            }
            startProcessingTimeNs = OptionalLong.of(now);
            ControllerResult<T> result = op.generateRecordsAndResult();
            if (result.records().isEmpty()) {
                op.processBatchEndOffset(writeOffset);
                // If the operation did not return any records, then it was actually just
                // a read after all, and not a read + write.  However, this read was done
                // from the latest in-memory state, which might contain uncommitted data.
                OptionalLong maybeOffset = purgatory.highestPendingOffset();
                if (!maybeOffset.isPresent()) {
                    // If the purgatory is empty, there are no pending operations and no
                    // uncommitted state.  We can complete immediately.
                    resultAndOffset = ControllerResultAndOffset.of(-1, result);
                    log.debug("Completing read-only operation {} immediately because " +
                        "the purgatory is empty.", this);
                    complete(null);
                } else {
                    // If there are operations in the purgatory, we want to wait for the latest
                    // one to complete before returning our result to the user.
                    resultAndOffset = ControllerResultAndOffset.of(maybeOffset.getAsLong(), result);
                    log.debug("Read-only operation {} will be completed when the log " +
                        "reaches offset {}", this, resultAndOffset.offset());
                }
            } else {
                // Pass the records to the Raft layer. This will start the process of committing
                // them to the log.
                long offset = appendRecords(log, result, maxRecordsPerBatch,
                    new Function<List<ApiMessageAndVersion>, Long>() {
                        private long prevEndOffset = writeOffset;

                        @Override
                        public Long apply(List<ApiMessageAndVersion> records) {
                            // Start by trying to apply the record to our in-memory state. This should always
                            // succeed; if it does not, that's a fatal error. It is important to do this before
                            // scheduling the record for Raft replication.
                            int i = 1;
                            for (ApiMessageAndVersion message : records) {
                                try {
                                    replay(message.message(), Optional.empty(), prevEndOffset + records.size());
                                } catch (Throwable e) {
                                    String failureMessage = String.format("Unable to apply %s record, which was " +
                                            "%d of %d record(s) in the batch following last write offset %d.",
                                            message.message().getClass().getSimpleName(), i, records.size(),
                                            prevEndOffset);
                                    throw fatalFaultHandler.handleFault(failureMessage, e);
                                }
                                i++;
                            }
                            prevEndOffset = raftClient.scheduleAtomicAppend(controllerEpoch, records);
                            snapshotRegistry.getOrCreateSnapshot(prevEndOffset);
                            return prevEndOffset;
                        }
                    });
                op.processBatchEndOffset(offset);
                updateWriteOffset(offset);
                resultAndOffset = ControllerResultAndOffset.of(offset, result);

                log.debug("Read-write operation {} will be completed when the log " +
                    "reaches offset {}.", this, resultAndOffset.offset());
            }

            // After every controller write event, schedule a leader rebalance if there are any topic partition
            // with leader that is not the preferred leader.
            maybeScheduleNextBalancePartitionLeaders();

            // Remember the latest offset and future if it is not already completed
            if (!future.isDone()) {
                purgatory.add(resultAndOffset.offset(), this);
            }
        }

        @Override
        public void handleException(Throwable exception) {
            complete(exception);
        }

        @Override
        public void complete(Throwable exception) {
            if (exception == null) {
                handleEventEnd(this.toString(), startProcessingTimeNs.getAsLong());
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

    /**
     * Append records to the Raft log. They will be written out asynchronously.
     *
     * @param log                   The log4j logger.
     * @param result                The controller result we are writing out.
     * @param maxRecordsPerBatch    The maximum number of records to allow in a batch.
     * @param appender              The callback to invoke for each batch. The arguments are last
     *                              write offset, record list, and the return result is the new
     *                              last write offset.
     * @return                      The final offset that was returned from the Raft layer.
     */
    static long appendRecords(
        Logger log,
        ControllerResult<?> result,
        int maxRecordsPerBatch,
        Function<List<ApiMessageAndVersion>, Long> appender
    ) {
        try {
            List<ApiMessageAndVersion> records = result.records();
            if (result.isAtomic()) {
                // If the result must be written out atomically, check that it is not too large.
                // In general, we create atomic batches when it is important to commit "all, or
                // nothing". They are limited in size and must only be used when the batch size
                // is bounded.
                if (records.size() > maxRecordsPerBatch) {
                    throw new IllegalStateException("Attempted to atomically commit " +
                            records.size() + " records, but maxRecordsPerBatch is " +
                            maxRecordsPerBatch);
                }
                long offset = appender.apply(records);
                if (log.isTraceEnabled()) {
                    log.trace("Atomically appended {} record(s) ending with offset {}.",
                            records.size(), offset);
                }
                return offset;
            } else {
                // If the result is non-atomic, then split it into as many batches as needed.
                // The appender callback will create an in-memory snapshot for each batch,
                // since we might need to revert to any of them. We will only return the final
                // offset of the last batch, however.
                int startIndex = 0, numBatches = 0;
                while (true) {
                    numBatches++;
                    int endIndex = startIndex + maxRecordsPerBatch;
                    if (endIndex > records.size()) {
                        long offset = appender.apply(records.subList(startIndex, records.size()));
                        if (log.isTraceEnabled()) {
                            log.trace("Appended {} record(s) in {} batch(es), ending with offset {}.",
                                    records.size(), numBatches, offset);
                        }
                        return offset;
                    } else {
                        appender.apply(records.subList(startIndex, endIndex));
                    }
                    startIndex += maxRecordsPerBatch;
                }
            }
        } catch (ApiException e) {
            // If the Raft client throws a subclass of ApiException, we need to convert it into a
            // RuntimeException so that it will be handled as the unexpected exception that it is.
            // ApiExceptions are reserved for expected errors such as incorrect uses of controller
            // APIs, permission errors, NotControllerException, etc. etc.
            throw new RuntimeException(e);
        }
    }

    <T> CompletableFuture<T> appendWriteEvent(String name,
                                              OptionalLong deadlineNs,
                                              ControllerWriteOperation<T> op) {
        ControllerWriteEvent<T> event = new ControllerWriteEvent<>(name, op);
        if (deadlineNs.isPresent()) {
            queue.appendWithDeadline(deadlineNs.getAsLong(), event);
        } else {
            queue.append(event);
        }
        return event.future();
    }

    class QuorumMetaLogListener implements RaftClient.Listener<ApiMessageAndVersion> {
        @Override
        public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
            appendRaftEvent("handleCommit[baseOffset=" + reader.baseOffset() + "]", () -> {
                try {
                    maybeCompleteAuthorizerInitialLoad();
                    long processedRecordsSize = 0;
                    boolean isActive = isActiveController();
                    while (reader.hasNext()) {
                        Batch<ApiMessageAndVersion> batch = reader.next();
                        long offset = batch.lastOffset();
                        int epoch = batch.epoch();
                        List<ApiMessageAndVersion> messages = batch.records();

                        if (isActive) {
                            // If the controller is active, the records were already replayed,
                            // so we don't need to do it here.
                            log.debug("Completing purgatory items up to offset {} and epoch {}.", offset, epoch);

                            // Complete any events in the purgatory that were waiting for this offset.
                            purgatory.completeUpTo(offset);

                            // The active controller can delete up to the current committed offset.
                            snapshotRegistry.deleteSnapshotsUpTo(offset);
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
                            int i = 1;
                            for (ApiMessageAndVersion message : messages) {
                                try {
                                    replay(message.message(), Optional.empty(), offset);
                                } catch (Throwable e) {
                                    String failureMessage = String.format("Unable to apply %s record on standby " +
                                            "controller, which was %d of %d record(s) in the batch with baseOffset %d.",
                                            message.message().getClass().getSimpleName(), i, messages.size(),
                                            batch.baseOffset());
                                    throw fatalFaultHandler.handleFault(failureMessage, e);
                                }
                                i++;
                            }
                        }

                        controllerMetricsManager.replayBatch(batch.baseOffset(), messages);

                        updateLastCommittedState(
                            offset,
                            epoch,
                            batch.appendTimestamp()
                        );

                        if (offset >= raftClient.latestSnapshotId().map(OffsetAndEpoch::offset).orElse(0L)) {
                            oldestNonSnapshottedTimestamp = Math.min(
                                oldestNonSnapshottedTimestamp,
                                batch.appendTimestamp()
                            );
                        }
                    }
                } finally {
                    reader.close();
                }
            });
        }

        @Override
        public void handleSnapshot(SnapshotReader<ApiMessageAndVersion> reader) {
            appendRaftEvent(String.format("handleSnapshot[snapshotId=%s]", reader.snapshotId()), () -> {
                try {
                    if (isActiveController()) {
                        throw fatalFaultHandler.handleFault(String.format("Asked to load snapshot " +
                            "(%s) when it is the active controller (%d)", reader.snapshotId(),
                            curClaimEpoch));
                    }
                    log.info("Starting to replay snapshot ({}), from last commit offset ({}) and epoch ({})",
                        reader.snapshotId(), lastCommittedOffset, lastCommittedEpoch);

                    resetToEmptyState();

                    while (reader.hasNext()) {
                        Batch<ApiMessageAndVersion> batch = reader.next();
                        long offset = batch.lastOffset();
                        List<ApiMessageAndVersion> messages = batch.records();

                        if (log.isDebugEnabled()) {
                            if (log.isTraceEnabled()) {
                                log.trace("Replaying snapshot ({}) batch with last offset of {}: {}",
                                    reader.snapshotId(), offset, messages.stream().map(ApiMessageAndVersion::toString).
                                        collect(Collectors.joining(", ")));
                            } else {
                                log.debug("Replaying snapshot ({}) batch with last offset of {}",
                                    reader.snapshotId(), offset);
                            }
                        }

                        int i = 1;
                        for (ApiMessageAndVersion message : messages) {
                            try {
                                replay(message.message(), Optional.of(reader.snapshotId()), reader.lastContainedLogOffset());
                                controllerMetricsManager.replay(message.message());
                            } catch (Throwable e) {
                                String failureMessage = String.format("Unable to apply %s record " +
                                        "from snapshot %s on standby controller, which was %d of " +
                                        "%d record(s) in the batch with baseOffset %d.",
                                        message.message().getClass().getSimpleName(), reader.snapshotId(),
                                        i, messages.size(), batch.baseOffset());
                                throw fatalFaultHandler.handleFault(failureMessage, e);
                            }
                            i++;
                        }
                    }

                    updateLastCommittedState(
                        reader.lastContainedLogOffset(),
                        reader.lastContainedLogEpoch(),
                        reader.lastContainedLogTimestamp());
                    snapshotRegistry.getOrCreateSnapshot(lastCommittedOffset);
                    authorizer.ifPresent(a -> a.loadSnapshot(aclControlManager.idToAcl()));
                } finally {
                    reader.close();
                }
            });
        }

        @Override
        public void handleLeaderChange(LeaderAndEpoch newLeader) {
            appendRaftEvent("handleLeaderChange[" + newLeader.epoch() + "]", () -> {
                final String newLeaderName = newLeader.leaderId().isPresent() ?
                        String.valueOf(newLeader.leaderId().getAsInt()) : "(none)";
                if (isActiveController()) {
                    if (newLeader.isLeader(nodeId)) {
                        log.warn("We were the leader in epoch {}, and are still the leader " +
                                "in the new epoch {}.", curClaimEpoch, newLeader.epoch());
                        curClaimEpoch = newLeader.epoch();
                    } else {
                        log.warn("Renouncing the leadership due to a metadata log event. " +
                            "We were the leader at epoch {}, but in the new epoch {}, " +
                            "the leader is {}. Reverting to last committed offset {}.",
                            curClaimEpoch, newLeader.epoch(), newLeaderName, lastCommittedOffset);
                        renounce();
                    }
                } else if (newLeader.isLeader(nodeId)) {
                    log.info("Becoming the active controller at epoch {}, committed offset {}, " +
                        "committed epoch {}", newLeader.epoch(), lastCommittedOffset,
                        lastCommittedEpoch);
                    claim(newLeader.epoch());
                } else {
                    log.info("In the new epoch {}, the leader is {}.",
                        newLeader.epoch(), newLeaderName);
                }
            });
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
                    try {
                        runnable.run();
                    } finally {
                        maybeCompleteAuthorizerInitialLoad();
                    }
                }
            });
        }
    }

    private void maybeCompleteAuthorizerInitialLoad() {
        if (!needToCompleteAuthorizerLoad) return;
        OptionalLong highWatermark = raftClient.highWatermark();
        if (highWatermark.isPresent()) {
            if (lastCommittedOffset + 1 >= highWatermark.getAsLong()) {
                log.info("maybeCompleteAuthorizerInitialLoad: completing authorizer " +
                    "initial load at last committed offset {}.", lastCommittedOffset);
                authorizer.get().completeInitialLoad();
                needToCompleteAuthorizerLoad = false;
            } else {
                log.trace("maybeCompleteAuthorizerInitialLoad: can't proceed because " +
                    "lastCommittedOffset  = {}, but highWatermark = {}.",
                    lastCommittedOffset, highWatermark.getAsLong());
            }
        } else {
            log.trace("maybeCompleteAuthorizerInitialLoad: highWatermark not set.");
        }
    }

    private boolean isActiveController() {
        return curClaimEpoch != -1;
    }

    private void updateWriteOffset(long offset) {
        writeOffset = offset;
        if (isActiveController()) {
            controllerMetrics.setLastAppliedRecordOffset(writeOffset);
            // This is not truly the append timestamp. The KRaft client doesn't expose the append time when scheduling a write.
            // This is good enough because this is called right after the records were given to the KRAft client for appending and
            // the default append linger for KRaft is 25ms.
            controllerMetrics.setLastAppliedRecordTimestamp(time.milliseconds());
        } else {
            // Change the last applied record metrics back to the last committed state. Inactive controllers report the last committed
            // state while active controllers report the latest state which may include uncommitted data.
            controllerMetrics.setLastAppliedRecordOffset(lastCommittedOffset);
            controllerMetrics.setLastAppliedRecordTimestamp(lastCommittedTimestamp);
        }
    }

    private void claim(int epoch) {
        try {
            if (curClaimEpoch != -1) {
                throw new RuntimeException("Cannot claim leadership because we are already the " +
                        "active controller.");
            }
            curClaimEpoch = epoch;
            controllerMetrics.setActive(true);
            updateWriteOffset(lastCommittedOffset);
            clusterControl.activate();

            // Before switching to active, create an in-memory snapshot at the last committed
            // offset. This is required because the active controller assumes that there is always
            // an in-memory snapshot at the last committed offset.
            snapshotRegistry.getOrCreateSnapshot(lastCommittedOffset);

            // Prepend the activate event. It is important that this event go at the beginning
            // of the queue rather than the end (hence prepend rather than append). It's also
            // important not to use prepend for anything else, to preserve the ordering here.
            queue.prepend(new ControllerWriteEvent<>("completeActivation[" + epoch + "]",
                    new CompleteActivationEvent()));
        } catch (Throwable e) {
            fatalFaultHandler.handleFault("exception while claiming leadership", e);
        }
    }

    class CompleteActivationEvent implements ControllerWriteOperation<Void> {
        @Override
        public ControllerResult<Void> generateRecordsAndResult() throws Exception {
            List<ApiMessageAndVersion> records = new ArrayList<>();
            if (logReplayTracker.empty()) {
                // If no records have been replayed, we need to write out the bootstrap records.
                // This will include the new metadata.version, as well as things like SCRAM
                // initialization, etc.
                log.info("The metadata log appears to be empty. Appending {} bootstrap record(s) " +
                        "at metadata.version {} from {}.", bootstrapMetadata.records().size(),
                        bootstrapMetadata.metadataVersion(), bootstrapMetadata.source());
                records.addAll(bootstrapMetadata.records());
            } else if (featureControl.metadataVersion().equals(MetadataVersion.MINIMUM_KRAFT_VERSION)) {
                log.info("No metadata.version feature level record was found in the log. " +
                        "Treating the log as version {}.", MetadataVersion.MINIMUM_KRAFT_VERSION);
            }
            return ControllerResult.atomicOf(records, null);
        }

        @Override
        public void processBatchEndOffset(long offset) {
            // As part of completing our transition to active controller, we reschedule the
            // periodic tasks here. At this point, all the records we generated in
            // generateRecordsAndResult have been applied, so we have the correct value for
            // metadata.version and other in-memory state.
            maybeScheduleNextBalancePartitionLeaders();
            maybeScheduleNextWriteNoOpRecord();
        }
    }

    private void updateLastCommittedState(
        long offset,
        int epoch,
        long timestamp
    ) {
        lastCommittedOffset = offset;
        lastCommittedEpoch = epoch;
        lastCommittedTimestamp = timestamp;

        controllerMetrics.setLastCommittedRecordOffset(offset);
        if (!isActiveController()) {
            controllerMetrics.setLastAppliedRecordOffset(offset);
            controllerMetrics.setLastAppliedRecordTimestamp(timestamp);
        }
    }

    private void renounce() {
        try {
            if (curClaimEpoch == -1) {
                throw new RuntimeException("Cannot renounce leadership because we are not the " +
                        "current leader.");
            }
            raftClient.resign(curClaimEpoch);
            curClaimEpoch = -1;
            controllerMetrics.setActive(false);
            purgatory.failAll(newNotControllerException());

            if (!snapshotRegistry.hasSnapshot(lastCommittedOffset)) {
                throw new RuntimeException("Unable to find last committed offset " +
                        lastCommittedEpoch + " in snapshot registry.");
            }
            snapshotRegistry.revertToSnapshot(lastCommittedOffset);
            authorizer.ifPresent(a -> a.loadSnapshot(aclControlManager.idToAcl()));
            updateWriteOffset(-1);
            clusterControl.deactivate();
            cancelMaybeFenceReplicas();
            cancelMaybeBalancePartitionLeaders();
            cancelNextWriteNoOpRecord();
        } catch (Throwable e) {
            fatalFaultHandler.handleFault("exception while renouncing leadership", e);
        }
    }

    private <T> void scheduleDeferredWriteEvent(String name, long deadlineNs,
                                                ControllerWriteOperation<T> op) {
        ControllerWriteEvent<T> event = new ControllerWriteEvent<>(name, op, true);
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
            ControllerResult<Void> result = replicationControl.maybeFenceOneStaleBroker();
            // This following call ensures that if there are multiple brokers that
            // are currently stale, then fencing for them is scheduled immediately
            rescheduleMaybeFenceStaleBrokers();
            return result;
        });
    }

    private void cancelMaybeFenceReplicas() {
        queue.cancelDeferred(MAYBE_FENCE_REPLICAS);
    }

    private static final String MAYBE_BALANCE_PARTITION_LEADERS = "maybeBalancePartitionLeaders";

    private void maybeScheduleNextBalancePartitionLeaders() {
        if (imbalancedScheduled != ImbalanceSchedule.SCHEDULED &&
            leaderImbalanceCheckIntervalNs.isPresent() &&
            replicationControl.arePartitionLeadersImbalanced()) {

            log.debug(
                "Scheduling write event for {} because scheduled ({}), checkIntervalNs ({}) and isImbalanced ({})",
                MAYBE_BALANCE_PARTITION_LEADERS,
                imbalancedScheduled,
                leaderImbalanceCheckIntervalNs,
                replicationControl.arePartitionLeadersImbalanced()
            );

            ControllerWriteEvent<Boolean> event = new ControllerWriteEvent<>(MAYBE_BALANCE_PARTITION_LEADERS, () -> {
                ControllerResult<Boolean> result = replicationControl.maybeBalancePartitionLeaders();

                // reschedule the operation after the leaderImbalanceCheckIntervalNs interval.
                // Mark the imbalance event as completed and reschedule if necessary
                if (result.response()) {
                    imbalancedScheduled = ImbalanceSchedule.IMMEDIATELY;
                } else {
                    imbalancedScheduled = ImbalanceSchedule.DEFERRED;
                }

                // Note that rescheduling this event here is not required because MAYBE_BALANCE_PARTITION_LEADERS
                // is a ControllerWriteEvent. ControllerWriteEvent always calls this method after the records
                // generated by a ControllerWriteEvent have been applied.

                return result;
            }, true);

            long delayNs = time.nanoseconds();
            if (imbalancedScheduled == ImbalanceSchedule.DEFERRED) {
                delayNs += leaderImbalanceCheckIntervalNs.getAsLong();
            } else {
                // The current implementation of KafkaEventQueue always picks from the deferred collection of operations
                // before picking from the non-deferred collection of operations. This can result in some unfairness if
                // deferred operation are scheduled for immediate execution. This delays them by a small amount of time.
                delayNs += NANOSECONDS.convert(10, TimeUnit.MILLISECONDS);
            }

            queue.scheduleDeferred(MAYBE_BALANCE_PARTITION_LEADERS, new EarliestDeadlineFunction(delayNs), event);

            imbalancedScheduled = ImbalanceSchedule.SCHEDULED;
        }
    }

    private void cancelMaybeBalancePartitionLeaders() {
        imbalancedScheduled = ImbalanceSchedule.DEFERRED;
        queue.cancelDeferred(MAYBE_BALANCE_PARTITION_LEADERS);
    }

    private static final String WRITE_NO_OP_RECORD = "writeNoOpRecord";

    private void maybeScheduleNextWriteNoOpRecord() {
        if (!noOpRecordScheduled &&
            maxIdleIntervalNs.isPresent() &&
            featureControl.metadataVersion().isNoOpRecordSupported()) {

            log.debug(
                "Scheduling write event for {} because maxIdleIntervalNs ({}) and metadataVersion ({})",
                WRITE_NO_OP_RECORD,
                maxIdleIntervalNs.getAsLong(),
                featureControl.metadataVersion()
            );

            ControllerWriteEvent<Void> event = new ControllerWriteEvent<>(
                WRITE_NO_OP_RECORD,
                () -> {
                    noOpRecordScheduled = false;
                    maybeScheduleNextWriteNoOpRecord();

                    return ControllerResult.of(
                        Arrays.asList(new ApiMessageAndVersion(new NoOpRecord(), (short) 0)),
                        null
                    );
                },
                true
            );

            long delayNs = time.nanoseconds() + maxIdleIntervalNs.getAsLong();
            queue.scheduleDeferred(WRITE_NO_OP_RECORD, new EarliestDeadlineFunction(delayNs), event);
            noOpRecordScheduled = true;
        }
    }

    private void cancelNextWriteNoOpRecord() {
        noOpRecordScheduled = false;
        queue.cancelDeferred(WRITE_NO_OP_RECORD);
    }

    private void handleFeatureControlChange() {
        // The feature control maybe have changed. On the active controller cancel or schedule noop
        // record writes accordingly.
        if (isActiveController()) {
            if (featureControl.metadataVersion().isNoOpRecordSupported()) {
                maybeScheduleNextWriteNoOpRecord();
            } else {
                cancelNextWriteNoOpRecord();
            }
        }
    }

    /**
     * Apply the metadata record to its corresponding in-memory state(s)
     *
     * @param message           The metadata record
     * @param snapshotId        The snapshotId if this record is from a snapshot
     * @param batchLastOffset   The offset of the last record in the log batch, or the lastContainedLogOffset
     *                          if this record is from a snapshot, this is used along with RegisterBrokerRecord
     */
    private void replay(ApiMessage message, Optional<OffsetAndEpoch> snapshotId, long batchLastOffset) {
        logReplayTracker.replay(message);
        MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
        switch (type) {
            case REGISTER_BROKER_RECORD:
                clusterControl.replay((RegisterBrokerRecord) message, batchLastOffset);
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
                handleFeatureControlChange();
                break;
            case CLIENT_QUOTA_RECORD:
                clientQuotaControlManager.replay((ClientQuotaRecord) message);
                break;
            case PRODUCER_IDS_RECORD:
                producerIdControlManager.replay((ProducerIdsRecord) message);
                break;
            case BROKER_REGISTRATION_CHANGE_RECORD:
                clusterControl.replay((BrokerRegistrationChangeRecord) message);
                break;
            case ACCESS_CONTROL_ENTRY_RECORD:
                aclControlManager.replay((AccessControlEntryRecord) message, snapshotId);
                break;
            case REMOVE_ACCESS_CONTROL_ENTRY_RECORD:
                aclControlManager.replay((RemoveAccessControlEntryRecord) message, snapshotId);
                break;
            case NO_OP_RECORD:
                // NoOpRecord is an empty record and doesn't need to be replayed
                break;
            default:
                throw new RuntimeException("Unhandled record type " + type);
        }
    }

    /**
     * Clear all data structures and reset all KRaft state.
     */
    private void resetToEmptyState() {
        snapshotRegistry.reset();
        controllerMetricsManager.reset();

        updateLastCommittedState(-1, -1, -1);
    }

    /**
     * Handles faults that should normally be fatal to the process.
     */
    private final FaultHandler fatalFaultHandler;

    /**
     * The slf4j log context, used to create new loggers.
     */
    private final LogContext logContext;

    /**
     * The slf4j logger.
     */
    private final Logger log;

    /**
     * The ID of this controller node.
     */
    private final int nodeId;

    /**
     * The ID of this cluster.
     */
    private final String clusterId;

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
     * Manager for updating controller metrics based on the committed metadata.
     */
    private final ControllerMetricsManager controllerMetricsManager;

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
     * A predicate that returns information about whether a ConfigResource exists.
     */
    private final Consumer<ConfigResource> resourceExists;

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
     * The ClusterMetadataAuthorizer, if one is configured. Note that this will still be
     * Optional.empty() if an Authorizer is configured that doesn't use __cluster_metadata.
     */
    private final Optional<ClusterMetadataAuthorizer> authorizer;

    /**
     * Manages the standard ACLs in the cluster.
     * This must be accessed only by the event queue thread.
     */
    private final AclControlManager aclControlManager;

    /**
     * Tracks replaying the log.
     * This must be accessed only by the event queue thread.
     */
    private final LogReplayTracker logReplayTracker;

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
     * True if we need to complete the authorizer initial load.
     * This must be accessed only by the event queue thread.
     */
    private boolean needToCompleteAuthorizerLoad;

    /**
     * If we have called scheduleWrite, this is the last offset we got back from it.
     */
    private long writeOffset;

    /**
     * Timestamp for the oldest record that was committed but not included in a snapshot.
     */
    private long oldestNonSnapshottedTimestamp = Long.MAX_VALUE;

    /**
     * How long to delay partition leader balancing operations.
     */
    private final OptionalLong leaderImbalanceCheckIntervalNs;

    /**
     * How log to delay between appending NoOpRecord to the log.
     */
    private final OptionalLong maxIdleIntervalNs;

    private enum ImbalanceSchedule {
        // The leader balancing operation has been scheduled
        SCHEDULED,
        // If the leader balancing operation should be scheduled, schedule it with a delay
        DEFERRED,
        // If the leader balancing operation should be scheduled, schedule it immediately
        IMMEDIATELY
    }

    /**
     * Tracks the scheduling state for partition leader balancing operations.
     */
    private ImbalanceSchedule imbalancedScheduled = ImbalanceSchedule.DEFERRED;

    /**
     * Tracks if the write of the NoOpRecord has been scheduled.
     */
    private boolean noOpRecordScheduled = false;

    /**
     * Tracks if a snapshot generate was scheduled.
     */
    private boolean generateSnapshotScheduled = false;

    /**
     * The bootstrap metadata to use for initialization if needed.
     */
    private final BootstrapMetadata bootstrapMetadata;

    /**
     * The maximum number of records per batch to allow.
     */
    private final int maxRecordsPerBatch;

    private QuorumController(
        FaultHandler fatalFaultHandler,
        LogContext logContext,
        int nodeId,
        String clusterId,
        KafkaEventQueue queue,
        Time time,
        KafkaConfigSchema configSchema,
        RaftClient<ApiMessageAndVersion> raftClient,
        QuorumFeatures quorumFeatures,
        short defaultReplicationFactor,
        int defaultNumPartitions,
        ReplicaPlacer replicaPlacer,
        OptionalLong leaderImbalanceCheckIntervalNs,
        OptionalLong maxIdleIntervalNs,
        long sessionTimeoutNs,
        ControllerMetrics controllerMetrics,
        Optional<CreateTopicPolicy> createTopicPolicy,
        Optional<AlterConfigPolicy> alterConfigPolicy,
        ConfigurationValidator configurationValidator,
        Optional<ClusterMetadataAuthorizer> authorizer,
        Map<String, Object> staticConfig,
        BootstrapMetadata bootstrapMetadata,
        int maxRecordsPerBatch,
        boolean zkMigrationEnabled
    ) {
        this.fatalFaultHandler = fatalFaultHandler;
        this.logContext = logContext;
        this.log = logContext.logger(QuorumController.class);
        this.nodeId = nodeId;
        this.clusterId = clusterId;
        this.queue = queue;
        this.time = time;
        this.controllerMetrics = controllerMetrics;
        this.controllerMetricsManager = new ControllerMetricsManager(controllerMetrics);
        this.snapshotRegistry = new SnapshotRegistry(logContext);
        this.purgatory = new ControllerPurgatory();
        this.resourceExists = new ConfigResourceExistenceChecker();
        this.configurationControl = new ConfigurationControlManager.Builder().
            setLogContext(logContext).
            setSnapshotRegistry(snapshotRegistry).
            setKafkaConfigSchema(configSchema).
            setExistenceChecker(resourceExists).
            setAlterConfigPolicy(alterConfigPolicy).
            setValidator(configurationValidator).
            setStaticConfig(staticConfig).
            setNodeId(nodeId).
            build();
        this.clientQuotaControlManager = new ClientQuotaControlManager(snapshotRegistry);
        this.featureControl = new FeatureControlManager.Builder().
            setLogContext(logContext).
            setQuorumFeatures(quorumFeatures).
            setSnapshotRegistry(snapshotRegistry).
            // Set the default metadata version to the minimum KRaft version. This only really
            // matters if we are upgrading from a version that didn't store metadata.version in
            // the log, such as one of the pre-production 3.0, 3.1, or 3.2 versions. Those versions
            // are all treated as 3.0IV1. In newer versions the metadata.version will be specified
            // by the log.
            setMetadataVersion(MetadataVersion.MINIMUM_KRAFT_VERSION).
            build();
        this.clusterControl = new ClusterControlManager.Builder().
            setLogContext(logContext).
            setClusterId(clusterId).
            setTime(time).
            setSnapshotRegistry(snapshotRegistry).
            setSessionTimeoutNs(sessionTimeoutNs).
            setReplicaPlacer(replicaPlacer).
            setFeatureControlManager(featureControl).
            setZkMigrationEnabled(zkMigrationEnabled).
            build();
        this.producerIdControlManager = new ProducerIdControlManager(clusterControl, snapshotRegistry);
        this.leaderImbalanceCheckIntervalNs = leaderImbalanceCheckIntervalNs;
        this.maxIdleIntervalNs = maxIdleIntervalNs;
        this.replicationControl = new ReplicationControlManager.Builder().
            setSnapshotRegistry(snapshotRegistry).
            setLogContext(logContext).
            setDefaultReplicationFactor(defaultReplicationFactor).
            setDefaultNumPartitions(defaultNumPartitions).
            setMaxElectionsPerImbalance(ReplicationControlManager.MAX_ELECTIONS_PER_IMBALANCE).
            setConfigurationControl(configurationControl).
            setClusterControl(clusterControl).
            setCreateTopicPolicy(createTopicPolicy).
            setFeatureControl(featureControl).
            build();
        this.authorizer = authorizer;
        authorizer.ifPresent(a -> a.setAclMutator(this));
        this.aclControlManager = new AclControlManager(snapshotRegistry, authorizer);
        this.logReplayTracker = new LogReplayTracker.Builder().
                setLogContext(logContext).
                build();
        this.raftClient = raftClient;
        this.bootstrapMetadata = bootstrapMetadata;
        this.maxRecordsPerBatch = maxRecordsPerBatch;
        this.metaLogListener = new QuorumMetaLogListener();
        this.curClaimEpoch = -1;
        this.needToCompleteAuthorizerLoad = authorizer.isPresent();
        updateWriteOffset(-1);

        resetToEmptyState();

        log.info("Creating new QuorumController with clusterId {}, authorizer {}.", clusterId, authorizer);

        this.raftClient.register(metaLogListener);
    }

    @Override
    public CompletableFuture<AlterPartitionResponseData> alterPartition(
        ControllerRequestContext context,
        AlterPartitionRequestData request
    ) {
        if (request.topics().isEmpty()) {
            return CompletableFuture.completedFuture(new AlterPartitionResponseData());
        }
        return appendWriteEvent("alterPartition", context.deadlineNs(),
            () -> replicationControl.alterPartition(context, request));
    }

    @Override
    public CompletableFuture<CreateTopicsResponseData> createTopics(
        ControllerRequestContext context,
        CreateTopicsRequestData request, Set<String> describable
    ) {
        if (request.topics().isEmpty()) {
            return CompletableFuture.completedFuture(new CreateTopicsResponseData());
        }
        return appendWriteEvent("createTopics", context.deadlineNs(),
            () -> replicationControl.createTopics(request, describable));
    }

    @Override
    public CompletableFuture<Void> unregisterBroker(
        ControllerRequestContext context,
        int brokerId
    ) {
        return appendWriteEvent("unregisterBroker", context.deadlineNs(),
            () -> replicationControl.unregisterBroker(brokerId));
    }

    @Override
    public CompletableFuture<Map<String, ResultOrError<Uuid>>> findTopicIds(
        ControllerRequestContext context,
        Collection<String> names
    ) {
        if (names.isEmpty())
            return CompletableFuture.completedFuture(Collections.emptyMap());
        return appendReadEvent("findTopicIds", context.deadlineNs(),
            () -> replicationControl.findTopicIds(lastCommittedOffset, names));
    }

    @Override
    public CompletableFuture<Map<String, Uuid>> findAllTopicIds(
        ControllerRequestContext context
    ) {
        return appendReadEvent("findAllTopicIds", context.deadlineNs(),
            () -> replicationControl.findAllTopicIds(lastCommittedOffset));
    }

    @Override
    public CompletableFuture<Map<Uuid, ResultOrError<String>>> findTopicNames(
        ControllerRequestContext context,
        Collection<Uuid> ids
    ) {
        if (ids.isEmpty())
            return CompletableFuture.completedFuture(Collections.emptyMap());
        return appendReadEvent("findTopicNames", context.deadlineNs(),
            () -> replicationControl.findTopicNames(lastCommittedOffset, ids));
    }

    @Override
    public CompletableFuture<Map<Uuid, ApiError>> deleteTopics(
        ControllerRequestContext context,
        Collection<Uuid> ids
    ) {
        if (ids.isEmpty())
            return CompletableFuture.completedFuture(Collections.emptyMap());
        return appendWriteEvent("deleteTopics", context.deadlineNs(),
            () -> replicationControl.deleteTopics(ids));
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ResultOrError<Map<String, String>>>> describeConfigs(
        ControllerRequestContext context,
        Map<ConfigResource, Collection<String>> resources
    ) {
        return appendReadEvent("describeConfigs", context.deadlineNs(),
            () -> configurationControl.describeConfigs(lastCommittedOffset, resources));
    }

    @Override
    public CompletableFuture<ElectLeadersResponseData> electLeaders(
        ControllerRequestContext context,
        ElectLeadersRequestData request
    ) {
        // If topicPartitions is null, we will try to trigger a new leader election on
        // all partitions (!).  But if it's empty, there is nothing to do.
        if (request.topicPartitions() != null && request.topicPartitions().isEmpty()) {
            return CompletableFuture.completedFuture(new ElectLeadersResponseData());
        }
        return appendWriteEvent("electLeaders", context.deadlineNs(),
            () -> replicationControl.electLeaders(request));
    }

    @Override
    public CompletableFuture<FinalizedControllerFeatures> finalizedFeatures(
        ControllerRequestContext context
    ) {
        return appendReadEvent("getFinalizedFeatures", context.deadlineNs(),
            () -> featureControl.finalizedFeatures(lastCommittedOffset));
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ApiError>> incrementalAlterConfigs(
        ControllerRequestContext context,
        Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges,
        boolean validateOnly
    ) {
        if (configChanges.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        return appendWriteEvent("incrementalAlterConfigs", context.deadlineNs(), () -> {
            ControllerResult<Map<ConfigResource, ApiError>> result =
                configurationControl.incrementalAlterConfigs(configChanges, false);
            if (validateOnly) {
                return result.withoutRecords();
            } else {
                return result;
            }
        });
    }

    @Override
    public CompletableFuture<AlterPartitionReassignmentsResponseData> alterPartitionReassignments(
        ControllerRequestContext context,
        AlterPartitionReassignmentsRequestData request
    ) {
        if (request.topics().isEmpty()) {
            return CompletableFuture.completedFuture(new AlterPartitionReassignmentsResponseData());
        }
        return appendWriteEvent("alterPartitionReassignments", context.deadlineNs(),
            () -> replicationControl.alterPartitionReassignments(request));
    }

    @Override
    public CompletableFuture<ListPartitionReassignmentsResponseData> listPartitionReassignments(
        ControllerRequestContext context,
        ListPartitionReassignmentsRequestData request
    ) {
        if (request.topics() != null && request.topics().isEmpty()) {
            return CompletableFuture.completedFuture(
                new ListPartitionReassignmentsResponseData().setErrorMessage(null));
        }
        return appendReadEvent("listPartitionReassignments", context.deadlineNs(),
            () -> replicationControl.listPartitionReassignments(request.topics()));
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ApiError>> legacyAlterConfigs(
        ControllerRequestContext context,
        Map<ConfigResource, Map<String, String>> newConfigs, boolean validateOnly
    ) {
        if (newConfigs.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        return appendWriteEvent("legacyAlterConfigs", context.deadlineNs(), () -> {
            ControllerResult<Map<ConfigResource, ApiError>> result =
                configurationControl.legacyAlterConfigs(newConfigs, false);
            if (validateOnly) {
                return result.withoutRecords();
            } else {
                return result;
            }
        });
    }

    @Override
    public CompletableFuture<BrokerHeartbeatReply> processBrokerHeartbeat(
        ControllerRequestContext context,
        BrokerHeartbeatRequestData request
    ) {
        return appendWriteEvent("processBrokerHeartbeat", context.deadlineNs(),
            new ControllerWriteOperation<BrokerHeartbeatReply>() {
                private final int brokerId = request.brokerId();
                private boolean inControlledShutdown = false;

                @Override
                public ControllerResult<BrokerHeartbeatReply> generateRecordsAndResult() {
                    // Get the offset of the broker registration. Note: although the offset
                    // we get back here could be the offset for a previous epoch of the
                    // broker registration, we will check the broker epoch in
                    // processBrokerHeartbeat, which covers that case.
                    OptionalLong offsetForRegisterBrokerRecord =
                            clusterControl.registerBrokerRecordOffset(brokerId);
                    if (!offsetForRegisterBrokerRecord.isPresent()) {
                        throw new StaleBrokerEpochException(
                            String.format("Receive a heartbeat from broker %d before registration", brokerId));
                    }
                    ControllerResult<BrokerHeartbeatReply> result = replicationControl.
                        processBrokerHeartbeat(request, offsetForRegisterBrokerRecord.getAsLong());
                    inControlledShutdown = result.response().inControlledShutdown();
                    rescheduleMaybeFenceStaleBrokers();
                    return result;
                }

                @Override
                public void processBatchEndOffset(long offset) {
                    if (inControlledShutdown) {
                        clusterControl.heartbeatManager().
                            maybeUpdateControlledShutdownOffset(brokerId, offset);
                    }
                }
            });
    }

    @Override
    public CompletableFuture<BrokerRegistrationReply> registerBroker(
        ControllerRequestContext context,
        BrokerRegistrationRequestData request
    ) {
        return appendWriteEvent("registerBroker", context.deadlineNs(), () -> {
            ControllerResult<BrokerRegistrationReply> result = clusterControl.
                registerBroker(request, writeOffset + 1, featureControl.
                    finalizedFeatures(Long.MAX_VALUE));
            rescheduleMaybeFenceStaleBrokers();
            return result;
        });
    }

    @Override
    public CompletableFuture<Map<ClientQuotaEntity, ApiError>> alterClientQuotas(
        ControllerRequestContext context,
        Collection<ClientQuotaAlteration> quotaAlterations,
        boolean validateOnly
    ) {
        if (quotaAlterations.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        return appendWriteEvent("alterClientQuotas", context.deadlineNs(), () -> {
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
        ControllerRequestContext context,
        AllocateProducerIdsRequestData request
    ) {
        return appendWriteEvent("allocateProducerIds", context.deadlineNs(),
            () -> producerIdControlManager.generateNextProducerId(request.brokerId(), request.brokerEpoch()))
            .thenApply(result -> new AllocateProducerIdsResponseData()
                .setProducerIdStart(result.firstProducerId())
                .setProducerIdLen(result.size()));
    }

    @Override
    public CompletableFuture<UpdateFeaturesResponseData> updateFeatures(
        ControllerRequestContext context,
        UpdateFeaturesRequestData request
    ) {
        return appendWriteEvent("updateFeatures", context.deadlineNs(), () -> {
            Map<String, Short> updates = new HashMap<>();
            Map<String, FeatureUpdate.UpgradeType> upgradeTypes = new HashMap<>();
            request.featureUpdates().forEach(featureUpdate -> {
                String featureName = featureUpdate.feature();
                upgradeTypes.put(featureName, FeatureUpdate.UpgradeType.fromCode(featureUpdate.upgradeType()));
                updates.put(featureName, featureUpdate.maxVersionLevel());
            });
            return featureControl.updateFeatures(updates, upgradeTypes, clusterControl.brokerSupportedVersions(),
                request.validateOnly());
        }).thenApply(result -> {
            UpdateFeaturesResponseData responseData = new UpdateFeaturesResponseData();
            responseData.setResults(new UpdateFeaturesResponseData.UpdatableFeatureResultCollection(result.size()));
            result.forEach((featureName, error) -> responseData.results().add(
                new UpdateFeaturesResponseData.UpdatableFeatureResult()
                    .setFeature(featureName)
                    .setErrorCode(error.error().code())
                    .setErrorMessage(error.message())));
            return responseData;
        });
    }

    @Override
    public CompletableFuture<List<CreatePartitionsTopicResult>> createPartitions(
        ControllerRequestContext context,
        List<CreatePartitionsTopic> topics,
        boolean validateOnly
    ) {
        if (topics.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        return appendWriteEvent("createPartitions", context.deadlineNs(), () -> {
            final ControllerResult<List<CreatePartitionsTopicResult>> result = replicationControl.createPartitions(topics);
            if (validateOnly) {
                log.debug("Validate-only CreatePartitions result(s): {}", result.response());
                return result.withoutRecords();
            } else {
                log.debug("CreatePartitions result(s): {}", result.response());
                return result;
            }
        });
    }

    @Override
    public CompletableFuture<List<AclCreateResult>> createAcls(
        ControllerRequestContext context,
        List<AclBinding> aclBindings
    ) {
        return appendWriteEvent("createAcls", context.deadlineNs(),
            () -> aclControlManager.createAcls(aclBindings));
    }

    @Override
    public CompletableFuture<List<AclDeleteResult>> deleteAcls(
        ControllerRequestContext context,
        List<AclBindingFilter> filters
    ) {
        return appendWriteEvent("deleteAcls", context.deadlineNs(),
            () -> aclControlManager.deleteAcls(filters));
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

    public String clusterId() {
        return clusterId;
    }

    @Override
    public int curClaimEpoch() {
        return curClaimEpoch;
    }

    // Visible for testing
    MetadataVersion metadataVersion() {
        return featureControl.metadataVersion();
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
