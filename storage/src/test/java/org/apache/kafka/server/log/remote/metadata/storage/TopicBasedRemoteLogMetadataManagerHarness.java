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
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A test harness class that brings up 3 brokers and registers {@link TopicBasedRemoteLogMetadataManager} on broker with id as 0.
 */
public class TopicBasedRemoteLogMetadataManagerHarness extends IntegrationTestHarness {

    private TopicBasedRemoteLogMetadataManager topicBasedRemoteLogMetadataManager;

    protected Map<String, Object> overrideRemoteLogMetadataManagerProps() {
        return Collections.emptyMap();
    }

    public void initialize(Set<TopicIdPartition> topicIdPartitions,
                           boolean startConsumerThread) {
        initialize(topicIdPartitions, startConsumerThread, RemotePartitionMetadataStore::new);
    }

    public void initialize(Set<TopicIdPartition> topicIdPartitions,
                           boolean startConsumerThread,
                           Supplier<RemotePartitionMetadataStore> remotePartitionMetadataStoreSupplier) {
        // Call setup to start the cluster.
        super.setUp(new EmptyTestInfo());

        initializeRemoteLogMetadataManager(topicIdPartitions, startConsumerThread, RemoteLogMetadataTopicPartitioner::new, remotePartitionMetadataStoreSupplier);
    }

    public void initializeRemoteLogMetadataManager(Set<TopicIdPartition> topicIdPartitions,
                                                   boolean startConsumerThread,
                                                   Function<Integer, RemoteLogMetadataTopicPartitioner> remoteLogMetadataTopicPartitioner) {
        initializeRemoteLogMetadataManager(topicIdPartitions, startConsumerThread, remoteLogMetadataTopicPartitioner, RemotePartitionMetadataStore::new);
    }

    public void initializeRemoteLogMetadataManager(Set<TopicIdPartition> topicIdPartitions,
                                                   boolean startConsumerThread,
                                                   Function<Integer, RemoteLogMetadataTopicPartitioner> remoteLogMetadataTopicPartitioner,
                                                   Supplier<RemotePartitionMetadataStore> remotePartitionMetadataStoreSupplier) {

        topicBasedRemoteLogMetadataManager = RemoteLogMetadataManagerTestUtils.builder()
          .topicIdPartitions(topicIdPartitions)
          .bootstrapServers(bootstrapServers(listenerName()))
          .startConsumerThread(startConsumerThread)
          .remoteLogMetadataTopicPartitioner(remoteLogMetadataTopicPartitioner)
          .remotePartitionMetadataStore(remotePartitionMetadataStoreSupplier)
          .build();
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
