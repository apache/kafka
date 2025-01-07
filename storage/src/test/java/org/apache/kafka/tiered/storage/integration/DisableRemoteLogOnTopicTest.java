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
import org.junit.jupiter.params.provider.MethodSource;

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

    @ParameterizedTest(name = "{displayName}.quorum={0}.groupProtocol={1}")
    @MethodSource("getTestQuorumAndGroupProtocolParametersAll")
    @Override
    public void executeTieredStorageTest(String quorum, String groupProtocol) {
        super.executeTieredStorageTest(quorum, groupProtocol);
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
        // local.retention.ms/bytes need to set to the same value as retention.ms/bytes when disabling remote log copy
        final Map<String, String> disableRemoteCopy = new HashMap<>();
        disableRemoteCopy.put(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "true");
        disableRemoteCopy.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "-2");
        disableRemoteCopy.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "-2");

        // revert the change to local.retention.bytes
        final Map<String, String> enableRemoteCopy = new HashMap<>();
        enableRemoteCopy.put(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "false");
        enableRemoteCopy.put(TopicConfig.LOCAL_LOG_RETENTION_BYTES_CONFIG, "1");

        final Map<String, String> deleteOnDisable = new HashMap<>();
        deleteOnDisable.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false");
        deleteOnDisable.put(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, "true");

        builder
                .createTopic(topicA, partitionCount, replicationFactor, maxBatchCountPerSegment, assignment,
                        enableRemoteLogStorage)
                // send records to partition 0
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 2L)
                .produce(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                // disable remote log copy
                .updateTopicConfig(topicA,
                        disableRemoteCopy,
                        Collections.emptyList())

                // make sure we can still consume from the beginning of the topic to read data from local and remote storage
                .expectFetchFromTieredStorage(broker0, topicA, p0, 2)
                .consume(topicA, p0, 0L, 3, 2)

                // re-enable remote log copy
                .updateTopicConfig(topicA,
                        enableRemoteCopy,
                        Collections.emptyList())

                // make sure the logs can be offloaded
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 3L)
                .produce(topicA, p0, new KeyValueSpec("k3", "v3"))

                // disable remote log copy again
                .updateTopicConfig(topicA,
                        disableRemoteCopy,
                        Collections.emptyList())
                // make sure we can still consume from the beginning of the topic to read data from local and remote storage
                .expectFetchFromTieredStorage(broker0, topicA, p0, 3)
                .consume(topicA, p0, 0L, 4, 3)

                // verify the remote retention policy is working.
                // Use DELETE_RECORDS API to delete the records upto offset 1 and expect 1 remote segment to be deleted
                .expectDeletionInRemoteStorage(broker0, topicA, p0, DELETE_SEGMENT, 1)
                .deleteRecords(topicA, p0, 1L)
                .waitForRemoteLogSegmentDeletion(topicA)

                // disabling remote log on topicA and enabling deleteOnDisable
                .updateTopicConfig(topicA,
                        deleteOnDisable,
                        Collections.emptyList())
                // make sure all remote data is deleted
                .expectEmptyRemoteStorage(topicA, p0)
                // verify the local log is still consumable
                .consume(topicA, p0, 3L, 1, 0);
    }
}
