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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.controller.ClusterControlManager.HeartbeatReply;
import org.apache.kafka.controller.ClusterControlManager.RegistrationReply;
import org.apache.kafka.metadata.FeatureManager;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.metalog.MetaLogLeader;
import org.apache.kafka.metalog.MetaLogListener;
import org.apache.kafka.metalog.MetaLogManager;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
            KafkaEventQueue queue = null;
            try {
                queue = new KafkaEventQueue(time, logContext, threadNamePrefix);
                return new QuorumController(logContext, nodeId, queue, time, configDefs,
                        logManager, supportedFeatures);
            } catch (Exception e) {
                Utils.closeQuietly(queue, "event queue");
                throw e;
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
            TimeUnit.MICROSECONDS.convert(deltaNs, TimeUnit.NANOSECONDS));
    }

    private Throwable handleEventException(String name,
                                           Optional<Long> startProcessingTimeNs,
                                           Throwable exception) {
        if (!startProcessingTimeNs.isPresent()) {
            log.debug("unable to start processing {} because of {}.", name,
                exception.getClass().getSimpleName());
            if (exception instanceof ApiException) {
                return exception;
            } else {
                return new UnknownServerException(exception);
            }
        }
        long endProcessingTime = time.nanoseconds();
        long deltaNs = endProcessingTime - startProcessingTimeNs.get();
        long deltaUs = TimeUnit.MICROSECONDS.convert(deltaNs, TimeUnit.NANOSECONDS);
        if (exception instanceof ApiException) {
            log.debug("{}: failed with {} in {} us", name,
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
        private Optional<Long> startProcessingTimeNs = Optional.empty();

        ControlEvent(String name, Runnable handler) {
            this.name = name;
            this.handler = handler;
        }

        @Override
        public void run() throws Exception {
            startProcessingTimeNs = Optional.of(time.nanoseconds());
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

    /**
     * A controller event that reads the committed internal state in order to expose it
     * to an API.
     */
    class ControllerReadEvent<T> implements EventQueue.Event {
        private final String name;
        private final CompletableFuture<T> future;
        private final Supplier<T> handler;
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
            startProcessingTimeNs = Optional.of(time.nanoseconds());
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

    private <T> CompletableFuture<T> appendReadEvent(String name, Supplier<T> handler) {
        ControllerReadEvent<T> event = new ControllerReadEvent<T>(name, handler);
        queue.append(event);
        return event.future();
    }

    /**
     * A controller event that modifies the controller state.
     */
    class ControllerWriteEvent<T> implements EventQueue.Event, DeferredEvent {
        private final String name;
        private final CompletableFuture<T> future;
        private final Supplier<ControllerResult<T>> handler;
        private Optional<Long> startProcessingTimeNs = Optional.empty();
        private ControllerResultAndOffset<T> resultAndOffset;

        ControllerWriteEvent(String name, Supplier<ControllerResult<T>> handler) {
            this.name = name;
            this.future = new CompletableFuture<T>();
            this.handler = handler;
            this.resultAndOffset = null;
        }

        CompletableFuture<T> future() {
            return future;
        }

        @Override
        public void run() throws Exception {
            long controllerEpoch = curClaimEpoch;
            if (controllerEpoch == -1) {
                throw newNotControllerException();
            }
            startProcessingTimeNs = Optional.of(time.nanoseconds());
            ControllerResult<T> result = handler.get();
            if (result.records().isEmpty()) {
                // If the handler did not return any records, then the operation was
                // actually just a read after all, and not a read + write.  However,
                // this read was done from the latest in-memory state, which might contain
                // uncommitted data.
                Optional<Long> maybeOffset = purgatory.highestPendingOffset();
                if (!maybeOffset.isPresent()) {
                    // If the purgatory is empty, there are no pending operations and no
                    // uncommitted state.  We can return immediately.
                    this.resultAndOffset = new ControllerResultAndOffset<>(-1,
                        Collections.emptyList(), result.response());
                    log.debug("Completing read-only operation {} immediately because " +
                        "the purgatory is empty.", this);
                    complete(null);
                    return;
                }
                // If there are operations in the purgatory, we want to wait for the latest
                // one to complete before returning our result to the user.
                this.resultAndOffset = new ControllerResultAndOffset<>(maybeOffset.get(),
                    result.records(), result.response());
                log.debug("Read-only operation {} will be completed when the log " +
                    "reaches offset {}", this, resultAndOffset.offset());
            } else {
                // If the handler returned a batch of records, those records need to be
                // written before we can return our result to the user.  Here, we hand off
                // the batch of records to the metadata log manager.  They will be written
                // out asynchronously.
                long offset = logManager.scheduleWrite(controllerEpoch, result.records());
                writeOffset = offset;
                this.resultAndOffset = new ControllerResultAndOffset<>(offset,
                    result.records(), result.response());
                for (ApiMessageAndVersion message : result.records()) {
                    replay(message.message());
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
                                                      Supplier<ControllerResult<T>> handler) {
        ControllerWriteEvent<T> event = new ControllerWriteEvent<>(name, handler);
        queue.append(event);
        return event.future();
    }

    class QuorumMetaLogListener implements MetaLogListener {
        @Override
        public void handleCommits(long offset, List<ApiMessage> messages) {
            appendControlEvent("handleCommits[" + offset + "]", () -> {
                if (curClaimEpoch == -1) {
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
                        replay(message);
                    }
                } else {
                    log.debug("Completing purgatory items up to offset {}.", offset);

                    // Complete any events in the purgatory that were waiting for this offset.
                    purgatory.completeUpTo(offset);

                    // Delete all snapshots older than the offset.
                    // TODO: add an exception here for when we're writing out a log snapshot
                    snapshotRegistry.deleteSnapshotsUpTo(offset);
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
                    log.info("Becoming active at controller epoch {}.", newEpoch);
                    curClaimEpoch = newEpoch;
                    writeOffset = lastCommittedOffset;
                });
            }
        }

        @Override
        public void handleRenounce(long oldEpoch) {
            appendControlEvent("handleRenounce[" + oldEpoch + "]", () -> {
                if (curClaimEpoch == oldEpoch) {
                    log.info("Renouncing the leadership at oldEpoch {} due to a metadata " +
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
        purgatory.failAll(newNotControllerException());
        snapshotRegistry.revertToSnapshot(lastCommittedOffset);
        snapshotRegistry.deleteSnapshotsUpTo(lastCommittedOffset);
        writeOffset = -1;
    }

    @SuppressWarnings("unchecked")
    private void replay(ApiMessage message) {
        MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
        switch (type) {
            case REGISTER_BROKER_RECORD:
                clusterControl.replay((RegisterBrokerRecord) message);
                break;
            case UNREGISTER_BROKER_RECORD:
                clusterControl.replay((UnregisterBrokerRecord) message);
                break;
            case FENCE_BROKER_RECORD:
                clusterControl.replay((FenceBrokerRecord) message);
                break;
            case UNFENCE_BROKER_RECORD:
                clusterControl.replay((UnfenceBrokerRecord) message);
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
            case ISR_CHANGE_RECORD:
                throw new RuntimeException("Unhandled record type " + type);
            case ACCESS_CONTROL_RECORD:
                throw new RuntimeException("Unhandled record type " + type);
        }
    }

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
                             Map<String, VersionRange> supportedFeatures) throws Exception {
        this.log = logContext.logger(QuorumController.class);
        this.nodeId = nodeId;
        this.queue = queue;
        this.time = time;
        this.snapshotRegistry = new SnapshotRegistry(logContext, -1);
        snapshotRegistry.createSnapshot(-1);
        this.purgatory = new ControllerPurgatory();
        this.configurationControl = new ConfigurationControlManager(snapshotRegistry,
            configDefs);
        this.clusterControl =
            new ClusterControlManager(logContext, time, snapshotRegistry, 18000, 9000);
        this.featureControl =
            new FeatureControlManager(supportedFeatures, snapshotRegistry);
        this.replicationControl = new ReplicationControlManager(snapshotRegistry,
            new Random(), configurationControl, clusterControl);
        this.logManager = logManager;
        this.metaLogListener = new QuorumMetaLogListener();
        this.curClaimEpoch = -1L;
        this.lastCommittedOffset = -1L;
        this.writeOffset = -1L;
        this.logManager.register(metaLogListener);
    }

    @Override
    public CompletableFuture<Map<TopicPartition, Errors>>
            alterIsr(int brokerId, long brokerEpoch,
                     Map<TopicPartition, LeaderAndIsr> changes) {
        CompletableFuture<Map<TopicPartition, Errors>> future = new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedVersionException("unimplemented"));
        return future;
    }

    @Override
    public CompletableFuture<CreateTopicsResponseData>
            createTopics(CreateTopicsRequestData request) {
        return appendWriteEvent("createTopics", () ->
            replicationControl.createTopics(request));
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ResultOrError<Map<String, String>>>>
            describeConfigs(Map<ConfigResource, Collection<String>> resources) {
        return appendReadEvent("describeConfigs", () ->
            configurationControl.describeConfigs(lastCommittedOffset, resources));
    }

    @Override
    public CompletableFuture<Map<TopicPartition, PartitionLeaderElectionResult>>
            electLeaders(int timeoutMs, Set<TopicPartition> parts, boolean unclean) {
        CompletableFuture<Map<TopicPartition, PartitionLeaderElectionResult>> future =
            new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedVersionException("unimplemented"));
        return future;
    }

    @Override
    public CompletableFuture<FeatureManager.FinalizedFeaturesAndEpoch> finalizedFeatures() {
        return appendReadEvent("getFinalizedFeatures", () ->
            featureControl.finalizedFeaturesAndEpoch(lastCommittedOffset));
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
    public CompletableFuture<Map<ConfigResource, ApiError>> legacyAlterConfigs(
        Map<ConfigResource, Map<String, String>> newConfigs, boolean validateOnly) {
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
    public CompletableFuture<HeartbeatReply>
            processBrokerHeartbeat(BrokerHeartbeatRequestData request) {
        return appendWriteEvent("processBrokerHeartbeat", () ->
            clusterControl.processBrokerHeartbeat(request, lastCommittedOffset));
    }

    @Override
    public CompletableFuture<RegistrationReply>
            registerBroker(BrokerRegistrationRequestData request) {
        return appendWriteEvent("registerBroker", () ->
            clusterControl.registerBroker(request, writeOffset + 1,
                featureControl.finalizedFeaturesAndEpoch(Long.MAX_VALUE)));
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
