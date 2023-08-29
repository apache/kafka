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

import java.util.List;
import java.util.Map;

import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.DELETE_SEGMENT;

public final class DeleteSegmentsDueToLogStartOffsetBreachTest extends TieredStorageTestHarness {

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
        final Integer batchSize = 2;
        final Map<Integer, List<Integer>> replicaAssignment = null;
        final boolean enableRemoteLogStorage = true;
        final int beginEpoch = 0;
        final long startOffset = 3;
        final long beforeOffset = 3L;

        builder.createTopic(topicA, partitionCount, replicationFactor, maxBatchCountPerSegment, replicaAssignment,
                        enableRemoteLogStorage)
                .withBatchSize(topicA, p0, batchSize)
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k0", "v0"),
                        new KeyValueSpec("k1", "v1"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 2, new KeyValueSpec("k2", "v2"),
                        new KeyValueSpec("k3", "v3"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 4L)
                .produce(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"), new KeyValueSpec("k3", "v3"), new KeyValueSpec("k4", "v4"))
                .expectDeletionInRemoteStorage(broker0, topicA, p0, DELETE_SEGMENT, 1)
                // FIXME: `deleteRecords` action should wait for the segment deletion implicitly similar to `deleteTopic`
                // so that test will be non-flaky.
                .deleteRecords(topicA, p0, beforeOffset)
                .waitForRemoteLogSegmentDeletion(topicA)
                .expectLeaderEpochCheckpoint(broker0, topicA, p0, beginEpoch, startOffset)
                .expectFetchFromTieredStorage(broker0, topicA, p0, 1)
                .consume(topicA, p0, 3L, 2, 1);
    }
}
