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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.KafkaEventQueue;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.timeline.SnapshotRegistry;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public final class QuorumController implements Controller {
    private final int nodeId;
    private final KafkaEventQueue queue;
    private final Time time;
    private final SnapshotRegistry snapshotRegistry;
    private final ControllerPurgatory purgatory;

    /**
     * A builder class which creates the QuorumController.
     */
    static public class Builder {
        private final int nodeId;
        private Time time = Time.SYSTEM;
        private String threadNamePrefix = null;
        private LogContext logContext = null;

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

        public QuorumController build() {
            if (threadNamePrefix == null) {
                threadNamePrefix = String.format("Node%d_", nodeId);
            }
            if (logContext == null) {
                logContext = new LogContext(threadNamePrefix);
            }
            KafkaEventQueue queue = null;
            try {
                queue = new KafkaEventQueue(time, logContext, threadNamePrefix);
                return new QuorumController(nodeId, queue, time);
            } catch (Exception e) {
                Utils.closeQuietly(queue, "event queue");
                throw e;
            }
        }
    }

    private QuorumController(int nodeId,
                             KafkaEventQueue queue,
                             Time time) {
        this.nodeId = nodeId;
        this.queue = queue;
        this.time = time;
        this.snapshotRegistry = new SnapshotRegistry(-1);
        snapshotRegistry.createSnapshot(-1);
        this.purgatory = new ControllerPurgatory();
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
    public void beginShutdown() {
    }

    @Override
    public void close() {
    }
}
