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

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public final class PartitionsExpandTest extends TieredStorageTestHarness {

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
        final Integer p2 = 2;
        final Integer partitionCount = 1;
        final Integer replicationFactor = 2;
        final Integer maxBatchCountPerSegment = 1;
        final boolean enableRemoteLogStorage = true;
        final List<Integer> p0Assignment = Arrays.asList(broker0, broker1);
        final List<Integer> p1Assignment = Arrays.asList(broker0, broker1);
        final List<Integer> p2Assignment = Arrays.asList(broker1, broker0);

        builder
                .createTopic(topicA, partitionCount, replicationFactor, maxBatchCountPerSegment,
                        Collections.singletonMap(p0, p0Assignment), enableRemoteLogStorage)
                // produce events to partition 0
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 2L)
                .produce(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                // expand the topicA partition-count to 3
                .createPartitions(topicA, 3, mkMap(mkEntry(p1, p1Assignment), mkEntry(p2, p2Assignment)))
                // consume from the beginning of the topic to read data from local and remote storage for partition 0
                .expectFetchFromTieredStorage(broker0, topicA, p0, 2)
                .consume(topicA, p0, 0L, 3, 2)

                .expectLeader(topicA, p1, broker0, false)
                .expectLeader(topicA, p2, broker1, false)

                // produce events to partition 1
                .expectSegmentToBeOffloaded(broker0, topicA, p1, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicA, p1, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p1, 2L)
                .produce(topicA, p1, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))

                // produce events to partition 2
                .expectSegmentToBeOffloaded(broker1, topicA, p2, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker1, topicA, p2, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p2, 2L)
                .produce(topicA, p2, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))

                // produce some more events to partition 0 and expect the segments to be offloaded
                // KAFKA-15431: Support needs to be added to capture the offloaded segment event for already sent
                // message (k2, v2)
                // .expectSegmentToBeOffloaded(broker0, topicA, p0, 2, new KeyValueSpec("k2", "v2"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 3, new KeyValueSpec("k3", "v3"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 4, new KeyValueSpec("k4", "v4"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 5L)
                .produce(topicA, p0, new KeyValueSpec("k3", "v3"), new KeyValueSpec("k4", "v4"),
                        new KeyValueSpec("k5", "v5"))

                // consume from the beginning of the topic to read data from local and remote storage for partition 0
                .expectFetchFromTieredStorage(broker0, topicA, p0, 5)
                .consume(topicA, p0, 0L, 6, 5)

                // consume from the beginning of the topic to read data from local and remote storage for partition 1
                .expectFetchFromTieredStorage(broker0, topicA, p1, 2)
                .consume(topicA, p1, 0L, 3, 2)

                // consume from the middle of the topic for partition 2
                .expectFetchFromTieredStorage(broker1, topicA, p2, 1)
                .consume(topicA, p2, 1L, 2, 1);
    }
}
