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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.test.TestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.BROKER_ID;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.LOG_DIR;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_RETENTION_MS_PROP;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class RemoteLogMetadataManagerTestUtils {
    static final int METADATA_TOPIC_PARTITIONS_COUNT = 3;
    private static final short METADATA_TOPIC_REPLICATION_FACTOR = 2;
    private static final long METADATA_TOPIC_RETENTION_MS = 24 * 60 * 60 * 1000L;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String bootstrapServers;
        private Map<String, Object> overrideRemoteLogMetadataManagerProps = Map.of();
        private Supplier<RemotePartitionMetadataStore> remotePartitionMetadataStore = RemotePartitionMetadataStore::new;
        private Function<Integer, RemoteLogMetadataTopicPartitioner> remoteLogMetadataTopicPartitioner = RemoteLogMetadataTopicPartitioner::new;

        private Builder() {
        }

        public Builder bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = Objects.requireNonNull(bootstrapServers);
            return this;
        }

        public Builder remotePartitionMetadataStore(Supplier<RemotePartitionMetadataStore> remotePartitionMetadataStore) {
            this.remotePartitionMetadataStore = remotePartitionMetadataStore;
            return this;
        }

        public Builder remoteLogMetadataTopicPartitioner(Function<Integer, RemoteLogMetadataTopicPartitioner> remoteLogMetadataTopicPartitioner) {
            this.remoteLogMetadataTopicPartitioner = Objects.requireNonNull(remoteLogMetadataTopicPartitioner);
            return this;
        }

        public Builder overrideRemoteLogMetadataManagerProps(Map<String, Object> overrideRemoteLogMetadataManagerProps) {
            this.overrideRemoteLogMetadataManagerProps = Objects.requireNonNull(overrideRemoteLogMetadataManagerProps);
            return this;
        }

        public TopicBasedRemoteLogMetadataManager build() {
            Objects.requireNonNull(bootstrapServers);
            String logDir = TestUtils.tempDirectory("rlmm_segs_").getAbsolutePath();
            TopicBasedRemoteLogMetadataManager topicBasedRemoteLogMetadataManager =
                new TopicBasedRemoteLogMetadataManager(remoteLogMetadataTopicPartitioner, remotePartitionMetadataStore);

            // Initialize TopicBasedRemoteLogMetadataManager.
            Map<String, Object> configs = new HashMap<>();
            configs.put(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            configs.put(BROKER_ID, 0);
            configs.put(LOG_DIR, logDir);
            configs.put(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP, METADATA_TOPIC_PARTITIONS_COUNT);
            configs.put(REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, METADATA_TOPIC_REPLICATION_FACTOR);
            configs.put(REMOTE_LOG_METADATA_TOPIC_RETENTION_MS_PROP, METADATA_TOPIC_RETENTION_MS);
            // Add override properties.
            configs.putAll(overrideRemoteLogMetadataManagerProps);

            topicBasedRemoteLogMetadataManager.configure(configs);
            topicBasedRemoteLogMetadataManager.onBrokerReady();
            assertDoesNotThrow(() -> TestUtils.waitForCondition(topicBasedRemoteLogMetadataManager::isInitialized, 60_000L,
                    "Time out reached before it is initialized successfully"));
            return topicBasedRemoteLogMetadataManager;
        }
    }
}
