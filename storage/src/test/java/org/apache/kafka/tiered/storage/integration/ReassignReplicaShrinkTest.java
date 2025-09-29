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

import org.apache.kafka.tiered.storage.TieredStorageTestBuilder;
import org.apache.kafka.tiered.storage.TieredStorageTestHarness;
import org.apache.kafka.tiered.storage.specs.KeyValueSpec;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public final class ReassignReplicaShrinkTest extends TieredStorageTestHarness {

    /**
     * Cluster of two brokers
     * @return number of brokers in the cluster
     */
    @Override
    public int brokerCount() {
        return 2;
    }

    /**
     * Number of partitions in the '__remote_log_metadata' topic
     * @return number of partitions in the '__remote_log_metadata' topic
     */
    @Override
    public int numRemoteLogMetadataPartitions() {
        return 2;
    }

    @Override
    protected void writeTestSpecifications(TieredStorageTestBuilder builder) {
        final Integer broker0 = 0;
        final Integer broker1 = 1;
        final String topicA = "topicA";
        final Integer p0 = 0;
        final Integer p1 = 1;
        final Integer partitionCount = 2;
        final Integer replicationFactor = 2;
        final Integer maxBatchCountPerSegment = 1;
        final boolean enableRemoteLogStorage = true;
        final Map<Integer, List<Integer>> replicaAssignment = mkMap(
                mkEntry(p0, Arrays.asList(broker0, broker1)),
                mkEntry(p1, Arrays.asList(broker1, broker0))
        );

        builder
                // create topicA with 2 partitions and 2 RF
                .createTopic(topicA, partitionCount, replicationFactor, maxBatchCountPerSegment,
                        replicaAssignment, enableRemoteLogStorage)
                // send records to partition 0, expect that the segments are uploaded and removed from local log dir
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 2L)
                .produce(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                // send records to partition 1, expect that the segments are uploaded and removed from local log dir
                .expectSegmentToBeOffloaded(broker1, topicA, p1, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker1, topicA, p1, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p1, 2L)
                .produce(topicA, p1, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                // shrink the replication factor to 1
                .shrinkReplica(topicA, p0, Collections.singletonList(broker1))
                .shrinkReplica(topicA, p1, Collections.singletonList(broker0))
                .expectLeader(topicA, p0, broker1, false)
                .expectLeader(topicA, p1, broker0, false)
                // produce some more events to partition 0
                // KAFKA-15431: Support needs to be added to capture the offloaded segment event for already sent
                // message (k2, v2)
                // .expectSegmentToBeOffloaded(broker1, topicA, p0, 2, new KeyValueSpec("k2", "v2"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 3L)
                .produce(topicA, p0, new KeyValueSpec("k3", "v3"))
                // produce some more events to partition 1
                // KAFKA-15431: Support needs to be added to capture the offloaded segment event for already sent
                // message (k2, v2)
                // .expectSegmentToBeOffloaded(broker0, topicA, p1, 2, new KeyValueSpec("k2", "v2"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p1, 3L)
                .produce(topicA, p1, new KeyValueSpec("k3", "v3"))
                // consume from the beginning of the topic to read data from local and remote storage
                .expectFetchFromTieredStorage(broker1, topicA, p0, 3)
                .consume(topicA, p0, 0L, 4, 3)
                .expectFetchFromTieredStorage(broker0, topicA, p1, 3)
                .consume(topicA, p1, 0L, 4, 3);
    }
}
