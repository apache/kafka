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
package org.apache.kafka.tiered.storage.integration;

import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.tiered.storage.TieredStorageTestBuilder;
import org.apache.kafka.tiered.storage.specs.KeyValueSpec;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createServerPropsForRemoteStorage;

/**
 * Test to verify that the active segment is rolled and uploaded to remote storage when the segment breaches the
 * local log retention policy.
 */
public final class RollAndOffloadActiveSegmentTest {
    private static final int BROKER_COUNT = 3;
    private static final int NUM_REMOTE_LOG_METADATA_PARTITIONS = 5;

    @SuppressWarnings("unused")
    private static List<ClusterConfig> clusterConfig() {
        return List.of(ClusterConfig.defaultBuilder()
                .setTypes(Set.of(Type.KRAFT))
                .setBrokers(BROKER_COUNT)
                .setServerProperties(createServerPropsForRemoteStorage(
                        RollAndOffloadActiveSegmentTest.class.getSimpleName().toLowerCase(Locale.ROOT),
                        BROKER_COUNT,
                        NUM_REMOTE_LOG_METADATA_PARTITIONS))
                .build());
    }

    @ClusterTemplate("clusterConfig")
    public void testRollAndOffloadActiveSegmentWithClassicGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeRollAndOffloadActiveSegmentTest(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTemplate("clusterConfig")
    public void testRollAndOffloadActiveSegmentWithConsumerGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeRollAndOffloadActiveSegmentTest(clusterInstance, GroupProtocol.CONSUMER);
    }

    private void executeRollAndOffloadActiveSegmentTest(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws Exception {
        final int broker0 = 0;
        final String topicA = "topicA";
        final int p0 = 0;
        final int partitionCount = 1;
        final int replicationFactor = 1;
        final int maxBatchCountPerSegment = 1;
        // Pin the partition to broker 0 so the broker0-based expectations are deterministic
        // regardless of how many brokers the cluster has.
        final Map<Integer, List<Integer>> replicaAssignment = Map.of(p0, List.of(broker0));
        final boolean enableRemoteLogStorage = true;

        final TieredStorageTestBuilder builder = new TieredStorageTestBuilder()
                // Create topicA with 1 partition, 1 RF and enabled with remote storage.
                .createTopic(topicA, partitionCount, replicationFactor, maxBatchCountPerSegment, replicaAssignment,
                        enableRemoteLogStorage)
                // update the topic config such that it triggers the rolling of the active segment
                .updateTopicConfig(topicA, configsToBeAdded(), List.of())
                // produce events to partition 0 and expect all the 4 segments to be offloaded
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 2, new KeyValueSpec("k2", "v2"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 3, new KeyValueSpec("k3", "v3"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 4L)
                .produce(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"), new KeyValueSpec("k3", "v3"))
                // consume from the beginning of the topic to read data from local and remote storage
                .expectFetchFromTieredStorage(broker0, topicA, p0, 4)
                .consume(topicA, p0, 0L, 4, 4);

        builder.build().execute(clusterInstance, groupProtocol);
    }

    private static Map<String, String> configsToBeAdded() {
        // Update localLog retentionMs to 1 ms and segment roll-time to 10 ms
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "1");
        return topicConfigs;
    }
}
