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

import kafka.api.IntegrationTestHarness;
import kafka.utils.EmptyTestInfo;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.BROKER_ID;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.LOG_DIR;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_RETENTION_MS_PROP;

/**
 * A test harness class that brings up 3 brokers and registers {@link TopicBasedRemoteLogMetadataManager} on broker with id as 0.
 */
public class TopicBasedRemoteLogMetadataManagerHarness extends IntegrationTestHarness {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRemoteLogMetadataManagerHarness.class);

    protected static final int METADATA_TOPIC_PARTITIONS_COUNT = 3;
    protected static final short METADATA_TOPIC_REPLICATION_FACTOR = 2;
    protected static final long METADATA_TOPIC_RETENTION_MS = 24 * 60 * 60 * 1000L;

    private TopicBasedRemoteLogMetadataManager topicBasedRemoteLogMetadataManager;

    protected Map<String, Object> overrideRemoteLogMetadataManagerProps() {
        return Collections.emptyMap();
    }

    public void initialize(Set<TopicIdPartition> topicIdPartitions,
                           boolean startConsumerThread) {
        // Call setup to start the cluster.
        super.setUp(new EmptyTestInfo());

        initializeRemoteLogMetadataManager(topicIdPartitions, startConsumerThread, null);
    }

    public void initializeRemoteLogMetadataManager(Set<TopicIdPartition> topicIdPartitions,
                                                   boolean startConsumerThread,
                                                   RemoteLogMetadataTopicPartitioner remoteLogMetadataTopicPartitioner) {
        String logDir = TestUtils.tempDirectory("rlmm_segs_").getAbsolutePath();
        topicBasedRemoteLogMetadataManager = new TopicBasedRemoteLogMetadataManager(startConsumerThread) {
            @Override
            public void onPartitionLeadershipChanges(Set<TopicIdPartition> leaderPartitions,
                                                     Set<TopicIdPartition> followerPartitions) {
                Set<TopicIdPartition> allReplicas = new HashSet<>(leaderPartitions);
                allReplicas.addAll(followerPartitions);
                // Make sure the topic partition dirs exist as the topics might not have been created on this broker.
                for (TopicIdPartition topicIdPartition : allReplicas) {
                    // Create partition directory in the log directory created by topicBasedRemoteLogMetadataManager.
                    File partitionDir = new File(new File(config().logDir()), topicIdPartition.topicPartition().topic() + "-" + topicIdPartition.topicPartition().partition());
                    partitionDir.mkdirs();
                    if (!partitionDir.exists()) {
                        throw new KafkaException("Partition directory:[" + partitionDir + "] could not be created successfully.");
                    }
                }

                super.onPartitionLeadershipChanges(leaderPartitions, followerPartitions);
            }
        };

        // Initialize TopicBasedRemoteLogMetadataManager.
        Map<String, Object> configs = new HashMap<>();
        configs.put(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(listenerName()));
        configs.put(BROKER_ID, 0);
        configs.put(LOG_DIR, logDir);
        configs.put(REMOTE_LOG_METADATA_TOPIC_PARTITIONS_PROP, METADATA_TOPIC_PARTITIONS_COUNT);
        configs.put(REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, METADATA_TOPIC_REPLICATION_FACTOR);
        configs.put(REMOTE_LOG_METADATA_TOPIC_RETENTION_MS_PROP, METADATA_TOPIC_RETENTION_MS);

        log.debug("TopicBasedRemoteLogMetadataManager configs before adding overridden properties: {}", configs);
        // Add override properties.
        configs.putAll(overrideRemoteLogMetadataManagerProps());
        log.debug("TopicBasedRemoteLogMetadataManager configs after adding overridden properties: {}", configs);

        topicBasedRemoteLogMetadataManager.configure(configs);
        if (remoteLogMetadataTopicPartitioner != null) {
            topicBasedRemoteLogMetadataManager.setRlmTopicPartitioner(remoteLogMetadataTopicPartitioner);
        }
        try {
            waitUntilInitialized(60_000);
        } catch (TimeoutException e) {
            throw new KafkaException(e);
        }

        topicBasedRemoteLogMetadataManager.onPartitionLeadershipChanges(topicIdPartitions, Collections.emptySet());
    }

    // Visible for testing.
    public void waitUntilInitialized(long waitTimeMs) throws TimeoutException {
        long startMs = System.currentTimeMillis();
        while (!topicBasedRemoteLogMetadataManager.isInitialized()) {
            long currentTimeMs = System.currentTimeMillis();
            if (currentTimeMs > startMs + waitTimeMs) {
                throw new TimeoutException("Time out reached before it is initialized successfully");
            }

            Utils.sleep(100);
        }
    }

    @Override
    public int brokerCount() {
        return 3;
    }

    protected TopicBasedRemoteLogMetadataManager remoteLogMetadataManager() {
        return topicBasedRemoteLogMetadataManager;
    }

    public void close() throws IOException {
        closeRemoteLogMetadataManager();

        // Stop the servers and zookeeper.
        tearDown();
    }

    public void closeRemoteLogMetadataManager() {
        Utils.closeQuietly(topicBasedRemoteLogMetadataManager, "TopicBasedRemoteLogMetadataManager");
    }
}
