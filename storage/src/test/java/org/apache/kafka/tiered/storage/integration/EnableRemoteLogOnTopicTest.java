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

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.tiered.storage.TieredStorageTestBuilder;
import org.apache.kafka.tiered.storage.TieredStorageTestHarness;
import org.apache.kafka.tiered.storage.specs.KeyValueSpec;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public final class EnableRemoteLogOnTopicTest extends TieredStorageTestHarness {

    @Override
    public int brokerCount() {
        return 2;
    }

    @Override
    protected void writeTestSpecifications(TieredStorageTestBuilder builder) {
        final int broker0 = 0;
        final int broker1 = 1;
        final String topicA = "topicA";
        final int p0 = 0;
        final int p1 = 1;
        final int partitionCount = 2;
        final int replicationFactor = 2;
        final int maxBatchCountPerSegment = 1;
        final boolean enableRemoteLogStorage = false;
        final Map<Integer, List<Integer>> assignment = mkMap(
                mkEntry(p0, List.of(broker0, broker1)),
                mkEntry(p1, List.of(broker1, broker0))
        );

        builder
                .createTopic(topicA, partitionCount, replicationFactor, maxBatchCountPerSegment, assignment,
                        enableRemoteLogStorage)
                // send records to partition 0
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 0L)
                .produce(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                // send records to partition 1
                .expectEarliestLocalOffsetInLogDirectory(topicA, p1, 0L)
                .produce(topicA, p1, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                // enable remote log storage
                .updateTopicConfig(topicA,
                        Map.of(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                        List.of())
                // produce some more records to partition 0
                // Note that the segment 0-2 gets offloaded for p0, but we cannot expect those events deterministically
                // because the rlm-task-thread runs in background and this framework doesn't support it.
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 3, new KeyValueSpec("k3", "v3"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 4L)
                .produce(topicA, p0, new KeyValueSpec("k3", "v3"), new KeyValueSpec("k4", "v4"))
                // produce some more records to partition 1
                // Note that the segment 0-2 gets offloaded for p1, but we cannot expect those events deterministically
                // because the rlm-task-thread runs in background and this framework doesn't support it.
                .expectSegmentToBeOffloaded(broker1, topicA, p1, 3, new KeyValueSpec("k3", "v3"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p1, 4L)
                .produce(topicA, p1, new KeyValueSpec("k3", "v3"), new KeyValueSpec("k4", "v4"))
                // consume from the beginning of the topic to read data from local and remote storage for partition 0
                .expectFetchFromTieredStorage(broker0, topicA, p0, 4)
                .consume(topicA, p0, 0L, 5, 4)
                // consume from the beginning of the topic to read data from local and remote storage for partition 1
                .expectFetchFromTieredStorage(broker1, topicA, p1, 4)
                .consume(topicA, p1, 0L, 5, 4);
    }
}
