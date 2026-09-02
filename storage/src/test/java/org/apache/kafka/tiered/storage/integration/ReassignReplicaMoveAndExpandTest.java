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
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.tiered.storage.TieredStorageTestBuilder;
import org.apache.kafka.tiered.storage.specs.KeyValueSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createServerPropsForRemoteStorage;

public final class ReassignReplicaMoveAndExpandTest {
    private static final int BROKER_COUNT = 3;
    private static final int NUM_REMOTE_LOG_METADATA_PARTITIONS = 2;

    private static final Integer BROKER_0 = 0;
    private static final Integer BROKER_1 = 1;

    private static List<ClusterConfig> clusterConfig() {
        return List.of(ClusterConfig.defaultBuilder()
            .setTypes(Set.of(Type.KRAFT))
            .setBrokers(BROKER_COUNT)
            .setServerProperties(createServerPropsForRemoteStorage(
                ReassignReplicaMoveAndExpandTest.class.getSimpleName().toLowerCase(Locale.ROOT),
                BROKER_COUNT,
                NUM_REMOTE_LOG_METADATA_PARTITIONS))
            .build());
    }

    @ClusterTemplate("clusterConfig")
    public void testReassignReplicaExpandWithClassicGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeReassignReplicaTest(clusterInstance, GroupProtocol.CLASSIC, List.of(BROKER_0, BROKER_1));
    }

    @ClusterTemplate("clusterConfig")
    public void testReassignReplicaExpandWithConsumerGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeReassignReplicaTest(clusterInstance, GroupProtocol.CONSUMER, List.of(BROKER_0, BROKER_1));
    }

    @ClusterTemplate("clusterConfig")
    public void testReassignReplicaMoveWithClassicGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeReassignReplicaTest(clusterInstance, GroupProtocol.CLASSIC, List.of(BROKER_1));
    }

    @ClusterTemplate("clusterConfig")
    public void testReassignReplicaMoveWithConsumerGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeReassignReplicaTest(clusterInstance, GroupProtocol.CONSUMER, List.of(BROKER_1));
    }

    private void executeReassignReplicaTest(ClusterInstance clusterInstance, GroupProtocol groupProtocol, List<Integer> replicaIds) throws Exception {
        final String topicA = "topicA";
        final String topicB = "topicB";
        final int p0 = 0;
        final int partitionCount = 1;
        final int replicationFactor = 1;
        final int maxBatchCountPerSegment = 1;
        final boolean enableRemoteLogStorage = true;
        final List<Integer> metadataPartitions = new ArrayList<>();
        for (int i = 0; i < NUM_REMOTE_LOG_METADATA_PARTITIONS; i++) {
            metadataPartitions.add(i);
        }

        TieredStorageTestBuilder builder = new TieredStorageTestBuilder();
        builder
                // create topicA with 50 partitions and 2 RF. Using 50 partitions to ensure that the user-partitions
                // are mapped to all the __remote_log_metadata partitions. This is required to ensure that
                // TBRLMM able to handle the assignment of the newly created replica to one of the already assigned
                // metadata partition
                .createTopic(topicA, 50, 2, maxBatchCountPerSegment,
                        null, enableRemoteLogStorage)
                .expectUserTopicMappedToMetadataPartitions(topicA, metadataPartitions)
                // create topicB with 1 partition and 1 RF
                .createTopic(topicB, partitionCount, replicationFactor, maxBatchCountPerSegment,
                        mkMap(mkEntry(p0, List.of(BROKER_0))), enableRemoteLogStorage)
                // send records to partition 0
                .expectSegmentToBeOffloaded(BROKER_0, topicB, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(BROKER_0, topicB, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicB, p0, 2L)
                .produce(topicB, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                // The newly created replica gets mapped to one of the metadata partition which is being actively
                // consumed by both the brokers
                .reassignReplica(topicB, p0, replicaIds)
                .expectLeader(topicB, p0, BROKER_1, true)
                // produce some more events and verify the earliest local offset
                .expectEarliestLocalOffsetInLogDirectory(topicB, p0, 3L)
                .produce(topicB, p0, new KeyValueSpec("k3", "v3"))
                // consume from the beginning of the topic to read data from local and remote storage
                .expectFetchFromTieredStorage(BROKER_1, topicB, p0, 3)
                .consume(topicB, p0, 0L, 4, 3);

        builder.build().execute(clusterInstance, groupProtocol);
    }
}
