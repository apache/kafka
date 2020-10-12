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
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.EventQueue;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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

        public QuorumController build() {
            if (logManager == null) {
                throw new RuntimeException("You must set a metadata log manager.");
            }
            if (threadNamePrefix == null) {
                threadNamePrefix = String.format("Node%d_", nodeId);
            }
            if (logContext == null) {
                logContext = new LogContext(threadNamePrefix);
            }
            KafkaEventQueue queue = null;
            try {
                queue = new KafkaEventQueue(time, logContext, threadNamePrefix);
                return new QuorumController(logContext, nodeId, queue, time, configDefs,
                        logManager);
            } catch (Exception e) {
                Utils.closeQuietly(queue, "event queue");
                throw e;
            }
        }
    }

    private void handleEventEnd(String name, long startProcessingTimeNs) {
        long endProcessingTime = time.nanoseconds();
        long deltaNs = endProcessingTime - startProcessingTimeNs;
        log.debug("Processed {} in {} ns", name, deltaNs);
    }

    private Throwable handleEventException(String name, long startProcessingTimeNs,
                                           Throwable exception) {
        long endProcessingTime = time.nanoseconds();
        long deltaNs = endProcessingTime - startProcessingTimeNs;
        log.info("{}: failed with {} in ns", name,
            exception.getClass().getSimpleName(), deltaNs);
        if (exception instanceof ApiException) {
            return exception;
        } else {
            log.info("Renouncing the leadership at epoch {} due to an unknown server " +
                "exception. Reverting to last committed offset {}.", curClaimEpoch,
                lastCommittedOffset);
            renounce();
            return new UnknownServerException(exception);
        }
    }

    /**
     * A controller event for handling internal state changes, such as Raft inputs.
     */
    class ControlEvent<T> implements EventQueue.Event {
        private final String name;
        private final Runnable handler;
        private long startProcessingTimeNs;

        ControlEvent(String name, Runnable handler) {
            this.name = name;
            this.handler = handler;
        }

        @Override
        public void run() throws Exception {
            startProcessingTimeNs = time.nanoseconds();
            handler.run();
            handleEventEnd(name, startProcessingTimeNs);
        }

        @Override
        public void handleException(Throwable exception) {
            handleEventException(name, startProcessingTimeNs, exception);
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
        private long startProcessingTimeNs;

        ControllerReadEvent(String name, Supplier<T> handler) {
            this.name = name;
            this.future = new CompletableFuture<T>();
            this.handler = handler;
            this.startProcessingTimeNs = -1;
        }

        CompletableFuture<T> future() {
            return future;
        }

        @Override
        public void run() throws Exception {
            startProcessingTimeNs = time.nanoseconds();
            T value = handler.get();
            handleEventEnd(name, startProcessingTimeNs);
            future.complete(value);
        }

        @Override
        public void handleException(Throwable exception) {
            future.completeExceptionally(
                handleEventException(name, startProcessingTimeNs, exception));
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
        private long startProcessingTimeNs;
        private ControllerResultAndOffset<T> resultAndOffset;

        ControllerWriteEvent(String name, Supplier<ControllerResult<T>> handler) {
            this.name = name;
            this.future = new CompletableFuture<T>();
            this.handler = handler;
            this.startProcessingTimeNs = -1;
            this.resultAndOffset = null;
        }

        CompletableFuture<T> future() {
            return future;
        }

        @Override
        public void run() throws Exception {
            long controllerEpoch = curClaimEpoch;
            if (controllerEpoch == -1) {
                throw new NotControllerException("Node " + nodeId + " is not the " +
                    "current controller.");
            }
            startProcessingTimeNs = time.nanoseconds();
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
                    complete(null);
                    return;
                }
                // If there are operations in the purgatory, we want to wait for the latest
                // one to complete before returning our result to the user.
                this.resultAndOffset = new ControllerResultAndOffset<>(maybeOffset.get(),
                    result.records(), result.response());
            } else {
                // If the handler returned a batch of records, those records need to be
                // written before we can return our result to the user.  Here, we hand off
                // the batch of records to the metadata log manager.  They will be written
                // out asynchronously.
                long offset = logManager.scheduleWrite(controllerEpoch, result.records());
                this.resultAndOffset = new ControllerResultAndOffset<>(offset,
                    result.records(), result.response());
                for (ApiMessageAndVersion message : result.records()) {
                    replay(message.message());
                }
                snapshotRegistry.createSnapshot(offset);
            }
            purgatory.add(resultAndOffset.offset(), this);
        }

        @Override
        public void handleException(Throwable exception) {
            complete(exception);
        }

        @Override
        public void complete(Throwable exception) {
            long endProcessingTime = time.nanoseconds();
            long deltaNs = endProcessingTime - startProcessingTimeNs;
            if (exception != null) {
                log.info("{}: failed with {} in ns", name,
                        exception.getClass().getSimpleName(), deltaNs);
                if (exception instanceof ApiException) {
                    future.completeExceptionally(exception);
                } else {
                    future.completeExceptionally(new UnknownServerException(exception));
                }
            } else {
                log.info("Processed {} in {} ns", name, deltaNs);
                future.complete(resultAndOffset.response());
            }
        }
    }

    private <T> CompletableFuture<T> appendWriteEvent(String name,
                                                      Supplier<ControllerResult<T>> handler) {
        ControllerWriteEvent<T> event = new ControllerWriteEvent<>(name, handler);
        queue.append(event);
        return event.future();
    }

    class MetaLogListener implements MetaLogManager.Listener {
        @Override
        public void handleCommits(long offset, List<ApiMessage> messages) {
            appendControlEvent("handleCommits", () -> {
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
        public void handleClaim(long epoch) {
            appendControlEvent("handleClaim", () -> {
                long curEpoch = curClaimEpoch;
                if (curEpoch == -1) {
                    throw new RuntimeException("Tried to claim controller epoch " +
                        curEpoch + ", but we never renounced controller epoch " +
                        curEpoch);
                }
                log.info("Becoming active at controller epoch {}.", epoch);
                curClaimEpoch = epoch;
            });
        }

        @Override
        public void handleRenounce(long epoch) {
            appendControlEvent("handleClaim", () -> {
                if (curClaimEpoch == epoch) {
                    log.info("Renouncing the leadership at epoch {} due to a metadata " +
                            "log event. Reverting to last committed offset {}.", curClaimEpoch,
                        lastCommittedOffset);
                    renounce();
                }
            });
        }

        @Override
        public void beginShutdown() {
            log.info("Shutting down the controller because the metadata log requested it.");
            QuorumController.this.beginShutdown();
        }

        @Override
        public long currentClaimEpoch() {
            return curClaimEpoch;
        }
    }

    private void renounce() {
        curClaimEpoch = -1;
        purgatory.failAll(new NotControllerException("Node " + nodeId +
            " is no longer the controller."));
        snapshotRegistry.revertToSnapshot(lastCommittedOffset);
        snapshotRegistry.deleteSnapshotsUpTo(lastCommittedOffset);
    }

    @SuppressWarnings("unchecked")
    private void replay(ApiMessage message) {
        MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
        switch (type) {
            case BROKER_RECORD:
                throw new RuntimeException("Unhandled record type " + type);
            case TOPIC_RECORD:
                throw new RuntimeException("Unhandled record type " + type);
            case PARTITION_RECORD:
                throw new RuntimeException("Unhandled record type " + type);
            case CONFIG_RECORD:
                configurationControlManager.replay((ConfigRecord) message);
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
    private final ConfigurationControlManager configurationControlManager;

    /**
     * The interface that we use to mutate the Raft log.
     */
    private final MetaLogManager logManager;

    /**
     * The interface that receives callbacks from the Raft log.  These callbacks are
     * invoked from the Raft thread(s), not from the controller thread.
     */
    private final MetaLogListener metaLogListener;

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

    private QuorumController(LogContext logContext,
                             int nodeId,
                             KafkaEventQueue queue,
                             Time time,
                             Map<ConfigResource.Type, ConfigDef> configDefs,
                             MetaLogManager logManager) {
        this.log = logContext.logger(QuorumController.class);
        this.nodeId = nodeId;
        this.queue = queue;
        this.time = time;
        this.snapshotRegistry = new SnapshotRegistry(logContext, -1);
        snapshotRegistry.createSnapshot(-1);
        this.purgatory = new ControllerPurgatory();
        this.configurationControlManager =
            new ConfigurationControlManager(snapshotRegistry, configDefs);
        this.logManager = logManager;
        this.metaLogListener = new MetaLogListener();
        this.curClaimEpoch = -1L;
        this.lastCommittedOffset = -1L;
        this.logManager.initialize(metaLogListener);
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
    public CompletableFuture<Map<TopicPartition, PartitionLeaderElectionResult>>
            electLeaders(int timeoutMs, Set<TopicPartition> parts, boolean unclean) {
        CompletableFuture<Map<TopicPartition, PartitionLeaderElectionResult>> future =
            new CompletableFuture<>();
        future.completeExceptionally(new UnsupportedVersionException("unimplemented"));
        return future;
    }

    @Override
    public CompletableFuture<Map<ConfigResource, ApiError>> incrementalAlterConfigs(
            Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges,
            boolean validateOnly) {
        return appendWriteEvent("incrementalAlterConfigs", () -> {
            ControllerResult<Map<ConfigResource, ApiError>> result =
                configurationControlManager.incrementalAlterConfigs(configChanges);
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
                configurationControlManager.legacyAlterConfigs(newConfigs);
            if (validateOnly) {
                return result.withoutRecords();
            } else {
                return result;
            }
        });
    }

    @Override
    public void beginShutdown() {
        queue.beginShutdown();
    }

    @Override
    public void close() throws InterruptedException {
        queue.close();
    }
}
