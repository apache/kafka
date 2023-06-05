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

import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;

import java.util.Collections;
import java.util.List;

/**
 * The group coordinator configurations.
 */
public class GroupCoordinatorConfig {
    public static class Builder {
        private int numThreads = 1;
        private int consumerGroupSessionTimeoutMs = 45000;
        private int consumerGroupHeartbeatIntervalMs = 5000;
        private int consumerGroupMaxSize = Integer.MAX_VALUE;
        private List<PartitionAssignor> consumerGroupAssignors = null;
        private int offsetsTopicSegmentBytes = 100 * 1024 * 1024;

        public Builder withNumThreads(int numThreads) {
            this.numThreads = numThreads;
            return this;
        }

        public Builder withConsumerGroupSessionTimeoutMs(int consumerGroupSessionTimeoutMs) {
            this.consumerGroupSessionTimeoutMs = consumerGroupSessionTimeoutMs;
            return this;
        }

        public Builder withConsumerGroupHeartbeatIntervalMs(int consumerGroupHeartbeatIntervalMs) {
            this.consumerGroupHeartbeatIntervalMs = consumerGroupHeartbeatIntervalMs;
            return this;
        }

        public Builder withConsumerGroupMaxSize(int consumerGroupMaxSize) {
            this.consumerGroupMaxSize = consumerGroupMaxSize;
            return this;
        }

        public Builder withConsumerGroupAssignors(List<PartitionAssignor> consumerGroupAssignors) {
            this.consumerGroupAssignors = consumerGroupAssignors;
            return this;
        }

        public Builder withOffsetsTopicSegmentBytes(int offsetsTopicSegmentBytes) {
            this.offsetsTopicSegmentBytes = offsetsTopicSegmentBytes;
            return this;
        }

        public GroupCoordinatorConfig build() {
            if (numThreads <= 0) {
                throw new IllegalArgumentException("The number of threads must be greater than 0.");
            }
            if (consumerGroupSessionTimeoutMs <= 0) {
                throw new IllegalArgumentException("Consumer group session timeout must be greater than 0.");
            }
            if (consumerGroupHeartbeatIntervalMs <= 0) {
                throw new IllegalArgumentException("Consumer group heartbeat interval must be greater than 0.");
            }
            if (consumerGroupMaxSize <= 0) {
                throw new IllegalArgumentException("Consumer group max size must be greater than 0.");
            }
            if (consumerGroupAssignors == null || consumerGroupAssignors.isEmpty()) {
                throw new IllegalArgumentException("At least one consumer group assignor must be specified.");
            }
            if (offsetsTopicSegmentBytes <= 0) {
                throw new IllegalArgumentException("Offsets topic segment bytes must be greater than 0.");
            }

            return new GroupCoordinatorConfig(
                numThreads,
                consumerGroupSessionTimeoutMs,
                consumerGroupHeartbeatIntervalMs,
                consumerGroupMaxSize,
                Collections.unmodifiableList(consumerGroupAssignors),
                offsetsTopicSegmentBytes
            );
        }
    }

    /**
     * The number of threads or event loops running.
     */
    public final int numThreads;

    /**
     * The consumer group session timeout in milliseconds.
     */
    public final int consumerGroupSessionTimeoutMs;

    /**
     * The consumer group heartbeat interval in milliseconds.
     */
    public final int consumerGroupHeartbeatIntervalMs;

    /**
     * The consumer group maximum size.
     */
    public final int consumerGroupMaxSize;

    /**
     * The consumer group assignors.
     */
    public final List<PartitionAssignor> consumerGroupAssignors;

    /**
     * The offsets topic segment bytes should be kept relatively small to facilitate faster
     * log compaction and faster offset loads.
     */
    public final int offsetsTopicSegmentBytes;

    GroupCoordinatorConfig(
        int numThreads,
        int consumerGroupSessionTimeoutMs,
        int consumerGroupHeartbeatIntervalMs,
        int consumerGroupMaxSize,
        List<PartitionAssignor> consumerGroupAssignors,
        int offsetsTopicSegmentBytes
    ) {
        this.numThreads = numThreads;
        this.consumerGroupSessionTimeoutMs = consumerGroupSessionTimeoutMs;
        this.consumerGroupHeartbeatIntervalMs = consumerGroupHeartbeatIntervalMs;
        this.consumerGroupMaxSize = consumerGroupMaxSize;
        this.consumerGroupAssignors = consumerGroupAssignors;
        this.offsetsTopicSegmentBytes = offsetsTopicSegmentBytes;
    }
}
