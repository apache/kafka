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
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.tiered.storage.TieredStorageTestBuilder;
import org.apache.kafka.tiered.storage.TieredStorageTestHarness;
import org.apache.kafka.tiered.storage.specs.KeyValueSpec;
import org.apache.kafka.tiered.storage.specs.RemoteFetchCount;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.tiered.storage.specs.RemoteFetchCount.FetchCountAndOp;
import static org.apache.kafka.tiered.storage.specs.RemoteFetchCount.OperationType.LESS_THAN_OR_EQUALS_TO;

/**
 * Test Cases:
 *    Elementary offloads and fetches from tiered storage using consumer with read_committed isolation level.
 */
public final class OffloadAndTxnConsumeFromLeaderTest extends TieredStorageTestHarness {

    /**
     * Cluster of one broker
     * @return number of brokers in the cluster
     */
    @Override
    public int brokerCount() {
        return 1;
    }

    @Override
    public Properties overridingProps() {
        Properties props = super.overridingProps();
        // Configure the remote-log index cache size to hold one entry to simulate eviction of cached index entries.
        props.put(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP, "1");
        return props;
    }

    @Override
    protected void overrideConsumerConfig(Properties consumerConfig) {
        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString());
    }

    @Override
    protected void writeTestSpecifications(TieredStorageTestBuilder builder) {
        final Integer broker = 0;
        final String topicA = "topicA";
        final Integer p0 = 0;
        final Integer partitionCount = 1;
        final Integer replicationFactor = 1;
        final Integer oneBatchPerSegment = 1;
        final Map<Integer, List<Integer>> replicaAssignment = null;
        final boolean enableRemoteLogStorage = true;

        builder
                .createTopic(topicA, partitionCount, replicationFactor, oneBatchPerSegment, replicaAssignment,
                        enableRemoteLogStorage)
                .expectSegmentToBeOffloaded(broker, topicA, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker, topicA, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectSegmentToBeOffloaded(broker, topicA, p0, 2, new KeyValueSpec("k2", "v2"))
                .expectSegmentToBeOffloaded(broker, topicA, p0, 3, new KeyValueSpec("k3", "v3"))
                .expectSegmentToBeOffloaded(broker, topicA, p0, 4, new KeyValueSpec("k4", "v4"))
                .expectSegmentToBeOffloaded(broker, topicA, p0, 5, new KeyValueSpec("k5", "v5"))
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
                .expectFetchFromTieredStorage(broker, topicA, p0, getRemoteFetchCount())
                .consume(topicA, p0, 0L, 7, 6);
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
