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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

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

    /**
     * A controller event that reads the committed internal state.
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
            complete(handler.get(), null);
        }

        @Override
        public void handleException(Throwable exception) {
            complete(null, exception);
        }

        private void complete(T result, Throwable exception) {
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
                future.complete(result);
            }
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
            if (activeState == null) {
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
                long offset = logManager.scheduleWrite(activeState.epoch(), result.records());
                this.resultAndOffset = new ControllerResultAndOffset<>(offset,
                    result.records(), result.response());
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

    static class ActiveState {
        private final long epoch;

        ActiveState(long epoch) {
            this.epoch = epoch;
        }

        long epoch() {
            return epoch;
        }
    }

    private final Logger log;
    private final int nodeId;
    private final KafkaEventQueue queue;
    private final Time time;
    private final SnapshotRegistry snapshotRegistry;
    private final ControllerPurgatory purgatory;
    private final ConfigurationControlManager configurationControlManager;
    private final MetaLogManager logManager;
    private ActiveState activeState;

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
        this.snapshotRegistry = new SnapshotRegistry(-1);
        snapshotRegistry.createSnapshot(-1);
        this.purgatory = new ControllerPurgatory();
        this.configurationControlManager =
            new ConfigurationControlManager(snapshotRegistry, configDefs);
        this.logManager = logManager;
        this.activeState = null;
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
