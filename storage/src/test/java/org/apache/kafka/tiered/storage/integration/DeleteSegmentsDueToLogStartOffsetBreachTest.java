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
import java.util.List;
import java.util.Map;

import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.DELETE_SEGMENT;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public final class DeleteSegmentsDueToLogStartOffsetBreachTest extends TieredStorageTestHarness {

    @Override
    public int brokerCount() {
        return 2;
    }

    @Override
    protected void writeTestSpecifications(TieredStorageTestBuilder builder) {
        final Integer broker0 = 0;
        final Integer broker1 = 1;
        final String topicA = "topicA";
        final Integer p0 = 0;
        final Integer partitionCount = 1;
        final Integer replicationFactor = 2;
        final Integer maxBatchCountPerSegment = 2;
        final Map<Integer, List<Integer>> replicaAssignment = mkMap(mkEntry(p0, Arrays.asList(broker0, broker1)));
        final boolean enableRemoteLogStorage = true;
        final int beginEpoch = 0;
        final long startOffset = 3;
        final long beforeOffset = 3L;
        final long beforeOffset1 = 7L;

        // Create topicA with 1 partition and 2 RF
        builder.createTopic(topicA, partitionCount, replicationFactor, maxBatchCountPerSegment, replicaAssignment,
                        enableRemoteLogStorage)
                // produce events to partition 0 and expect 2 segments to be offloaded
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k0", "v0"),
                        new KeyValueSpec("k1", "v1"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 2, new KeyValueSpec("k2", "v2"),
                        new KeyValueSpec("k3", "v3"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 4L)
                .produce(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"), new KeyValueSpec("k3", "v3"), new KeyValueSpec("k4", "v4"))
                // Use DELETE_RECORDS API to delete the records upto offset 3 and expect one remote segment to be deleted
                .expectDeletionInRemoteStorage(broker0, topicA, p0, DELETE_SEGMENT, 1)
                .deleteRecords(topicA, p0, beforeOffset)
                // expect that the leader epoch checkpoint is updated
                // Comment out this line if it's FLAKY since the leader-epoch is not deterministic in ZK mode.
                .expectLeaderEpochCheckpoint(broker0, topicA, p0, beginEpoch, startOffset)
                // consume from the offset-3 of the topic to read data from local and remote storage
                .expectFetchFromTieredStorage(broker0, topicA, p0, 1)
                .consume(topicA, p0, 3L, 2, 1)

                // switch leader to change the leader-epoch from 0 to 1
                .expectLeader(topicA, p0, broker1, true)
                // produce some more messages and move the log-start-offset such that earliest-epoch changes from 0 to 1
                .expectSegmentToBeOffloaded(broker1, topicA, p0, 4, new KeyValueSpec("k4", "v4"),
                        new KeyValueSpec("k5", "v5"))
                .expectSegmentToBeOffloaded(broker1, topicA, p0, 6, new KeyValueSpec("k6", "v6"),
                        new KeyValueSpec("k7", "v7"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 8L)
                .produce(topicA, p0, new KeyValueSpec("k5", "v5"), new KeyValueSpec("k6", "v6"),
                        new KeyValueSpec("k7", "v7"), new KeyValueSpec("k8", "v8"), new KeyValueSpec("k9", "v9"))
                // Use DELETE_RECORDS API to delete the records upto offset 7 and expect 2 remote segments to be deleted
                .expectDeletionInRemoteStorage(broker1, topicA, p0, DELETE_SEGMENT, 2)
                .deleteRecords(topicA, p0, beforeOffset1)
                // consume from the topic with fetch-offset 7 to read data from local and remote storage
                .expectFetchFromTieredStorage(broker1, topicA, p0, 1)
                .consume(topicA, p0, 7L, 3, 1);
    }
}
