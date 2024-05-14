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
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.DELETE_SEGMENT;

public final class DeleteTopicTest extends TieredStorageTestHarness {

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
        final Integer p1 = 1;
        final Integer partitionCount = 2;
        final Integer replicationFactor = 2;
        final Integer maxBatchCountPerSegment = 1;
        final boolean enableRemoteLogStorage = true;
        final Map<Integer, List<Integer>> assignment = mkMap(
                mkEntry(p0, Arrays.asList(broker0, broker1)),
                mkEntry(p1, Arrays.asList(broker1, broker0))
        );

        builder
                .createTopic(topicA, partitionCount, replicationFactor, maxBatchCountPerSegment,
                        assignment, enableRemoteLogStorage)
                // send records to partition 0
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 2L)
                .produce(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                // send records to partition 1
                .expectSegmentToBeOffloaded(broker1, topicA, p1, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker1, topicA, p1, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p1, 2L)
                .produce(topicA, p1, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                // delete the topic
                .expectDeletionInRemoteStorage(broker0, topicA, p0, DELETE_SEGMENT, 2)
                .expectDeletionInRemoteStorage(broker1, topicA, p1, DELETE_SEGMENT, 2)
                .deleteTopic(Collections.singletonList(topicA))
                .expectEmptyRemoteStorage(topicA, p0)
                .expectEmptyRemoteStorage(topicA, p1);
    }
}
