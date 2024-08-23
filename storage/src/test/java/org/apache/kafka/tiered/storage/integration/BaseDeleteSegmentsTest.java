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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.DELETE_SEGMENT;

public abstract class BaseDeleteSegmentsTest extends TieredStorageTestHarness {

    @Override
    public int brokerCount() {
        return 1;
    }

    @Override
    protected void writeTestSpecifications(TieredStorageTestBuilder builder) {
        final Integer broker0 = 0;
        final String topicA = "topicA";
        final Integer p0 = 0;
        final Integer partitionCount = 1;
        final Integer replicationFactor = 1;
        final Integer maxBatchCountPerSegment = 1;
        final Map<Integer, List<Integer>> replicaAssignment = null;
        final boolean enableRemoteLogStorage = true;
        final int beginEpoch = 0;
        final long startOffset = 3;

        // Create topicA with 1 partition, 1 RF and enabled with remote storage.
        builder.createTopic(topicA, partitionCount, replicationFactor, maxBatchCountPerSegment, replicaAssignment,
                        enableRemoteLogStorage)
                // produce events to partition 0 and expect 3 segments to be offloaded
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 2, new KeyValueSpec("k2", "v2"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 3L)
                .produceWithTimestamp(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"), new KeyValueSpec("k3", "v3", System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1)))
                // update the topic config such that it triggers the deletion of segments
                .updateTopicConfig(topicA, configsToBeAdded(), Collections.emptyList())
                // expect that the three offloaded remote log segments are deleted
                .expectDeletionInRemoteStorage(broker0, topicA, p0, DELETE_SEGMENT, 3)
                .waitForRemoteLogSegmentDeletion(topicA)
                // expect that the leader epoch checkpoint is updated
                .expectLeaderEpochCheckpoint(broker0, topicA, p0, beginEpoch, startOffset)
                // consume from the beginning of the topic to read data from local and remote storage
                .expectFetchFromTieredStorage(broker0, topicA, p0, 0)
                .consume(topicA, p0, 0L, 1, 0);
    }

    protected abstract Map<String, String> configsToBeAdded();
}
