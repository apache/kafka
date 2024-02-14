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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public abstract class BaseReassignReplicaTest extends TieredStorageTestHarness {
    protected final Integer broker0 = 0;
    protected final Integer broker1 = 1;

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
        final String topicA = "topicA";
        final String topicB = "topicB";
        final Integer p0 = 0;
        final Integer partitionCount = 1;
        final Integer replicationFactor = 1;
        final Integer maxBatchCountPerSegment = 1;
        final Map<Integer, List<Integer>> replicaAssignment = null;
        final boolean enableRemoteLogStorage = true;
        final List<Integer> metadataPartitions = new ArrayList<>();
        for (int i = 0; i < numRemoteLogMetadataPartitions(); i++) {
            metadataPartitions.add(i);
        }

        builder
                // create topicA with 50 partitions and 2 RF. Using 50 partitions to ensure that the user-partitions
                // are mapped to all the __remote_log_metadata partitions. This is required to ensure that
                // TBRLMM able to handle the assignment of the newly created replica to one of the already assigned
                // metadata partition
                .createTopic(topicA, 50, 2, maxBatchCountPerSegment,
                        replicaAssignment, enableRemoteLogStorage)
                .expectUserTopicMappedToMetadataPartitions(topicA, metadataPartitions)
                // create topicB with 1 partition and 1 RF
                .createTopic(topicB, partitionCount, replicationFactor, maxBatchCountPerSegment,
                        mkMap(mkEntry(p0, Collections.singletonList(broker0))), enableRemoteLogStorage)
                // send records to partition 0
                .expectSegmentToBeOffloaded(broker0, topicB, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicB, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicB, p0, 2L)
                .produce(topicB, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                // The newly created replica gets mapped to one of the metadata partition which is being actively
                // consumed by both the brokers
                .reassignReplica(topicB, p0, replicaIds())
                .expectLeader(topicB, p0, broker1, true)
                // produce some more events and verify the earliest local offset
                .expectEarliestLocalOffsetInLogDirectory(topicB, p0, 3L)
                .produce(topicB, p0, new KeyValueSpec("k3", "v3"))
                // consume from the beginning of the topic to read data from local and remote storage
                .expectFetchFromTieredStorage(broker1, topicB, p0, 3)
                .consume(topicB, p0, 0L, 4, 3);
    }

    /**
     * Replicas of the topic
     * @return the replica-ids of the topic
     */
    protected abstract List<Integer> replicaIds();
}
