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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createServerPropsForRemoteStorage;

/**
 * Test Cases:
 *    Elementary offloads and fetches from tiered storage.
 */
public final class OffloadAndConsumeFromLeaderTest {

    private static final int BROKER_COUNT = 3;
    private static final int NUM_REMOTE_LOG_METADATA_PARTITIONS = 5;

    @SuppressWarnings("unused")
    private static List<ClusterConfig> clusterConfig() {
        return List.of(ClusterConfig.defaultBuilder()
                .setTypes(Set.of(Type.KRAFT))
                .setBrokers(BROKER_COUNT)
                .setServerProperties(createServerPropsForRemoteStorage(
                        OffloadAndConsumeFromLeaderTest.class.getSimpleName().toLowerCase(Locale.ROOT),
                        BROKER_COUNT,
                        NUM_REMOTE_LOG_METADATA_PARTITIONS))
                .build());
    }

    @ClusterTemplate("clusterConfig")
    public void testOffloadAndConsumeFromLeaderWithClassicGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeOffloadAndConsumeFromLeaderTest(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTemplate("clusterConfig")
    public void testOffloadAndConsumeFromLeaderWithConsumerGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeOffloadAndConsumeFromLeaderTest(clusterInstance, GroupProtocol.CONSUMER);
    }

    private void executeOffloadAndConsumeFromLeaderTest(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws Exception {
        final int broker0 = 0;
        final String topicA = "topicA";
        final String topicB = "topicB";
        final int p0 = 0;
        final int partitionCount = 1;
        final int replicationFactor = 1;
        final int oneBatchPerSegment = 1;
        final int twoBatchPerSegment = 2;
        // Pin the partition to broker 0 so that the broker0-based expectations are deterministic
        // regardless of how many brokers the cluster has.
        final Map<Integer, List<Integer>> replicaAssignment = Map.of(p0, List.of(broker0));
        final boolean enableRemoteLogStorage = true;

        final TieredStorageTestBuilder builder = new TieredStorageTestBuilder()
                /*
                 * (1) Create a topic which segments contain only one batch and produce three records
                 *       with a batch size of 1.
                 *
                 *       The topic and broker are configured so that the two rolled segments are picked from
                 *       the offloaded to the tiered storage and not present in the first-tier broker storage.
                 *
                 *       Acceptance:
                 *       -----------
                 *       State of the storages after production of the records and propagation of the log
                 *       segment lifecycles to peer subsystems (log cleaner, remote log manager).
                 *
                 *         - First-tier storage -            - Second-tier storage -
                 *           Log tA-p0                         Log tA-p0
                 *          *-------------------*             *-------------------*
                 *          | base offset = 2   |             |  base offset = 0  |
                 *          | (k2, v2)          |             |  (k0, v0)         |
                 *          *-------------------*             *-------------------*
                 *                                            *-------------------*
                 *                                            |  base offset = 1  |
                 *                                            |  (k1, v1)         |
                 *                                            *-------------------*
                 */
                .createTopic(topicA, partitionCount, replicationFactor, oneBatchPerSegment, replicaAssignment,
                        enableRemoteLogStorage)
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 2L)
                .produce(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))

                /*
                 * (2) Similar scenario as above, but with segments of two records.
                 *
                 *       Acceptance:
                 *       -----------
                 *       State of the storages after production of the records and propagation of the log
                 *       segment lifecycles to peer subsystems (log cleaner, remote log manager).
                 *
                 *         - First-tier storage -            - Second-tier storage -
                 *           Log tB-p0                         Log tB-p0
                 *          *-------------------*             *-------------------*
                 *          | base offset = 4   |             |  base offset = 0  |
                 *          | (k4, v4)          |             |  (k0, v0)         |
                 *          *-------------------*             |  (k1, v1)         |
                 *                                            *-------------------*
                 *                                            *-------------------*
                 *                                            |  base offset = 2  |
                 *                                            |  (k2, v2)         |
                 *                                            |  (k3, v3)         |
                 *                                            *-------------------*
                 */
                .createTopic(topicB, partitionCount, replicationFactor, twoBatchPerSegment, replicaAssignment,
                        enableRemoteLogStorage)
                .expectEarliestLocalOffsetInLogDirectory(topicB, p0, 4L)
                .expectSegmentToBeOffloaded(broker0, topicB, p0, 0,
                        new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"))
                .expectSegmentToBeOffloaded(broker0, topicB, p0, 2,
                        new KeyValueSpec("k2", "v2"), new KeyValueSpec("k3", "v3"))
                .produce(topicB, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"), new KeyValueSpec("k3", "v3"), new KeyValueSpec("k4", "v4"))

                /*
                 * (3) Stops and restarts the broker. The purpose of this test is to
                 *      a) exercise consumption from a given offset and
                 *      b) verify that upon broker start, existing remote log segments metadata are loaded by Kafka and these log segments available.
                 *
                 *       Acceptance:
                 *       -----------
                 *       - For topic A, this offset is defined such that only the second segment is fetched from
                 *         the tiered storage.
                 *       - For topic B, two segments are present in the tiered storage, as asserted by the
                 *         previous sub-test-case.
                 */
                .bounce(broker0)
                .expectFetchFromTieredStorage(broker0, topicA, p0, 1)
                .consume(topicA, p0, 1L, 2, 1)
                .expectFetchFromTieredStorage(broker0, topicB, p0, 2)
                .consume(topicB, p0, 1L, 4, 3);

        builder.build().execute(clusterInstance, groupProtocol);
    }
}
