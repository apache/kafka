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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.DELETE_SEGMENT;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createServerPropsForRemoteStorage;

public final class DeleteSegmentsTest {

    private static final int BROKER_COUNT = 3;
    private static final int NUM_REMOTE_LOG_METADATA_PARTITIONS = 5;

    private static final Map<String, String> RETENTION_SIZE_CONFIG =
            Map.of(TopicConfig.RETENTION_BYTES_CONFIG, "1");
    private static final Map<String, String> RETENTION_TIME_CONFIG =
            Map.of(TopicConfig.RETENTION_MS_CONFIG, "1");

    private static List<ClusterConfig> clusterConfig() {
        return List.of(ClusterConfig.defaultBuilder()
                .setTypes(Set.of(Type.KRAFT))
                .setBrokers(BROKER_COUNT)
                .setServerProperties(createServerPropsForRemoteStorage(
                        DeleteSegmentsTest.class.getSimpleName().toLowerCase(Locale.ROOT),
                        BROKER_COUNT,
                        NUM_REMOTE_LOG_METADATA_PARTITIONS))
                .build());
    }

    @ClusterTemplate("clusterConfig")
    public void testDeleteSegmentsByRetentionSizeWithClassicGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeDeleteSegmentsTest(clusterInstance, GroupProtocol.CLASSIC, RETENTION_SIZE_CONFIG);
    }

    @ClusterTemplate("clusterConfig")
    public void testDeleteSegmentsByRetentionSizeWithConsumerGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeDeleteSegmentsTest(clusterInstance, GroupProtocol.CONSUMER, RETENTION_SIZE_CONFIG);
    }

    @ClusterTemplate("clusterConfig")
    public void testDeleteSegmentsByRetentionTimeWithClassicGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeDeleteSegmentsTest(clusterInstance, GroupProtocol.CLASSIC, RETENTION_TIME_CONFIG);
    }

    @ClusterTemplate("clusterConfig")
    public void testDeleteSegmentsByRetentionTimeWithConsumerGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeDeleteSegmentsTest(clusterInstance, GroupProtocol.CONSUMER, RETENTION_TIME_CONFIG);
    }

    private static void executeDeleteSegmentsTest(ClusterInstance clusterInstance,
                                                  GroupProtocol groupProtocol,
                                                  Map<String, String> configsToBeAdded) throws Exception {
        final int broker0 = 0;
        final String topicA = "topicA";
        final int p0 = 0;
        final int partitionCount = 1;
        final int replicationFactor = 1;
        final int maxBatchCountPerSegment = 1;
        // Pin the partition to broker 0 so the broker0-based expectations are deterministic
        // regardless of how many brokers the cluster has.
        final Map<Integer, List<Integer>> replicaAssignment = mkMap(mkEntry(p0, List.of(broker0)));
        final boolean enableRemoteLogStorage = true;
        final int beginEpoch = 0;
        final long startOffset = 3;

        TieredStorageTestBuilder builder = new TieredStorageTestBuilder();

        // Create topicA with 1 partition, 1 RF and enabled with remote storage.
        builder.createTopic(topicA, partitionCount, replicationFactor, maxBatchCountPerSegment, replicaAssignment,
                        enableRemoteLogStorage)
                // produce events to partition 0 and expect 3 segments to be offloaded
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 2, new KeyValueSpec("k2", "v2"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 3L)
                .produceWithTimestamp(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        // testDeleteSegmentsByRetentionTime* uses a tiny retention time, which could cause the active
                        // segment to be rolled and deleted. We use a future timestamp to prevent that from happening.
                        new KeyValueSpec("k2", "v2"), new KeyValueSpec("k3", "v3", System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1)))
                // update the topic config such that it triggers the deletion of segments
                .updateTopicConfig(topicA, configsToBeAdded, List.of())
                // expect that the three offloaded remote log segments are deleted
                .expectDeletionInRemoteStorage(broker0, topicA, p0, DELETE_SEGMENT, 3)
                .waitForRemoteLogSegmentDeletion(topicA)
                // expect that the leader epoch checkpoint is updated
                .expectLeaderEpochCheckpoint(broker0, topicA, p0, beginEpoch, startOffset)
                // consume from the beginning of the topic to read data from local and remote storage
                .expectFetchFromTieredStorage(broker0, topicA, p0, 0)
                .consume(topicA, p0, 0L, 1, 0);

        builder.build().execute(clusterInstance, groupProtocol);
    }
}
