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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.DELETE_SEGMENT;

public final class DisableRemoteLogOnTopicTest extends TieredStorageTestHarness {

    @Override
    public int brokerCount() {
        return 2;
    }

    @ParameterizedTest(name = "{displayName}.quorum={0}")
    @ValueSource(strings = {"kraft"})
    public void executeTieredStorageTest(String quorum) {
        super.executeTieredStorageTest(quorum);
    }

    @Override
    protected void writeTestSpecifications(TieredStorageTestBuilder builder) {
        final Integer broker0 = 0;
        final Integer broker1 = 1;
        final String topicA = "topicA";
        final Integer p0 = 0;
        final Integer partitionCount = 1;
        final Integer replicationFactor = 2;
        final Integer maxBatchCountPerSegment = 1;
        final boolean enableRemoteLogStorage = true;
        final Map<Integer, List<Integer>> assignment = mkMap(
                mkEntry(p0, Arrays.asList(broker0, broker1))
        );
        final Map<String, String> explictRetain = new HashMap<>();
        explictRetain.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false");
        explictRetain.put(TopicConfig.REMOTE_LOG_DISABLE_POLICY_CONFIG, "retain");

        final Map<String, String> deletePolicy = new HashMap<>();
        deletePolicy.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false");
        deletePolicy.put(TopicConfig.REMOTE_LOG_DISABLE_POLICY_CONFIG, "delete");

        builder
                .createTopic(topicA, partitionCount, replicationFactor, maxBatchCountPerSegment, assignment,
                        enableRemoteLogStorage)
                // send records to partition 0
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 2L)
                .produce(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                // disable remote log storage
                .updateTopicConfig(topicA,
                        Collections.singletonMap(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"),
                        Collections.emptyList())

                // make sure we can still consume from the beginning of the topic to read data from local and remote storage
                .expectFetchFromTieredStorage(broker0, topicA, p0, 2)
                .consume(topicA, p0, 0L, 3, 2)

                // enable remote log storage
                .updateTopicConfig(topicA,
                        Collections.singletonMap(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                        Collections.emptyList())

                // make sure the logs can be offloaded
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 3L)
                .produce(topicA, p0, new KeyValueSpec("k3", "v3"))

                // explicitly set delete policy as "retain" and disable remote storage
                .updateTopicConfig(topicA,
                        explictRetain,
                        Collections.emptyList())
                // make sure we can still consume from the beginning of the topic to read data from local and remote storage
                .expectFetchFromTieredStorage(broker0, topicA, p0, 3)
                .consume(topicA, p0, 0L, 4, 3)

                // verify the remote retention policy is working.
                // update retention size to 2, and we have 3 inactive segments with each size 1,
                // so there will be 1 remote log segment get deleted
                .updateTopicConfig(topicA,
                        Collections.singletonMap(TopicConfig.RETENTION_BYTES_CONFIG, "2"),
                        Collections.emptyList())
                .expectDeletionInRemoteStorage(broker0, topicA, p0, DELETE_SEGMENT, 1)
                .waitForRemoteLogSegmentDeletion(topicA)

                // setting delete policy to "delete" and disable remote storage
                .updateTopicConfig(topicA,
                        deletePolicy,
                        Collections.emptyList())
                // make sure all remote data is deleted
                .expectEmptyRemoteStorage(topicA, p0);
    }
}
