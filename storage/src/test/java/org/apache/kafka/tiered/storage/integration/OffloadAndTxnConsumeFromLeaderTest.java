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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.tiered.storage.TieredStorageTestBuilder;
import org.apache.kafka.tiered.storage.specs.FetchCountAndOp;
import org.apache.kafka.tiered.storage.specs.KeyValueSpec;
import org.apache.kafka.tiered.storage.specs.RemoteFetchCount;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.tiered.storage.specs.RemoteFetchCount.OperationType.LESS_THAN_OR_EQUALS_TO;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createServerPropsForRemoteStorage;

/**
 * Test Cases:
 *    Elementary offloads and fetches from tiered storage using consumer with read_committed isolation level.
 */
public final class OffloadAndTxnConsumeFromLeaderTest {

    private static final int BROKER_COUNT = 3;
    private static final int NUM_REMOTE_LOG_METADATA_PARTITIONS = 5;

    @SuppressWarnings("unused")
    private static List<ClusterConfig> clusterConfig() {
        Map<String, String> serverProps = createServerPropsForRemoteStorage(
                OffloadAndTxnConsumeFromLeaderTest.class.getSimpleName().toLowerCase(Locale.ROOT),
                BROKER_COUNT,
                NUM_REMOTE_LOG_METADATA_PARTITIONS);
        // Configure the remote-log index cache size to hold one entry to simulate eviction of cached index entries.
        serverProps.put(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP, "1");
        return List.of(ClusterConfig.defaultBuilder()
                .setTypes(Set.of(Type.KRAFT))
                .setBrokers(BROKER_COUNT)
                .setServerProperties(serverProps)
                .build());
    }

    @ClusterTemplate("clusterConfig")
    public void testOffloadAndTxnConsumeFromLeaderWithClassicGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeOffloadAndTxnConsumeFromLeaderTest(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTemplate("clusterConfig")
    public void testOffloadAndTxnConsumeFromLeaderWithConsumerGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeOffloadAndTxnConsumeFromLeaderTest(clusterInstance, GroupProtocol.CONSUMER);
    }

    private void executeOffloadAndTxnConsumeFromLeaderTest(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws Exception {
        final int broker0 = 0;
        final String topicA = "topicA";
        final int p0 = 0;
        final int partitionCount = 1;
        final int replicationFactor = 1;
        final int oneBatchPerSegment = 1;
        // Pin the partition to broker 0 so that the broker0-based expectations are deterministic
        // regardless of how many brokers the cluster has.
        final Map<Integer, List<Integer>> replicaAssignment = Map.of(p0, List.of(broker0));
        final boolean enableRemoteLogStorage = true;

        final TieredStorageTestBuilder builder = new TieredStorageTestBuilder()
                .createTopic(topicA, partitionCount, replicationFactor, oneBatchPerSegment, replicaAssignment,
                        enableRemoteLogStorage)
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 2, new KeyValueSpec("k2", "v2"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 3, new KeyValueSpec("k3", "v3"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 4, new KeyValueSpec("k4", "v4"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 5, new KeyValueSpec("k5", "v5"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 6L)
                .produce(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"), new KeyValueSpec("k3", "v3"), new KeyValueSpec("k4", "v4"),
                        new KeyValueSpec("k5", "v5"), new KeyValueSpec("k6", "v6"))
                // When reading with transactional consumer, the consecutive remote fetch indexes are fetched until the
                // LSO found is higher than the fetch-offset.
                // summation(n) = (n * (n + 1)) / 2
                // Total number of uploaded remote segments = 6. Total number of index fetches = (6 * (6 + 1)) / 2 = 21
                // Note that we skip the index fetch when the txn-index is empty, so the effective index fetch count
                // should be same as the segment count.
                .expectFetchFromTieredStorage(broker0, topicA, p0, getRemoteFetchCount())
                .consume(topicA, p0, 0L, 7, 6);

        final Map<String, Object> extraConsumerProps = Map.of(
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name().toLowerCase(Locale.ROOT),
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString()
        );

        builder.build().execute(clusterInstance, extraConsumerProps);
    }

    private static RemoteFetchCount getRemoteFetchCount() {
        // Ideally, each remote-log segment should be fetched only once. For 6 segments, we would have 6 fetch-counts.
        // But, the client can retry the FETCH request, to make the test deterministic, increasing the fetch-count
        // to be at-max of 12 (2 times of fetch-count).
        FetchCountAndOp segmentFetchCountAndOp = new FetchCountAndOp(12, LESS_THAN_OR_EQUALS_TO);
        // RemoteIndexCache might evict the entries much before reaching the maximum size.
        // To make the test deterministic, we are using the operation type as LESS_THAN_OR_EQUALS_TO which equals to the
        // number of times the RemoteIndexCache gets accessed. The RemoteIndexCache gets accessed twice for each read.
        FetchCountAndOp indexFetchCountAndOp = new FetchCountAndOp(12, LESS_THAN_OR_EQUALS_TO);
        return new RemoteFetchCount(segmentFetchCountAndOp, indexFetchCountAndOp,
                indexFetchCountAndOp, indexFetchCountAndOp);
    }
}
