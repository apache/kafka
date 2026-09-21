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

import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.tiered.storage.TieredStorageTestBuilder;
import org.apache.kafka.tiered.storage.specs.KeyValueSpec;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.DELETE_SEGMENT;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createServerPropsForRemoteStorage;

public final class DisableRemoteLogOnTopicTest {

    private static final int BROKER_COUNT = 3;
    private static final int NUM_REMOTE_LOG_METADATA_PARTITIONS = 5;

    @SuppressWarnings("unused")
    private static List<ClusterConfig> clusterConfig() {
        return List.of(ClusterConfig.defaultBuilder()
                .setTypes(Set.of(Type.KRAFT))
                .setBrokers(BROKER_COUNT)
                .setServerProperties(createServerPropsForRemoteStorage(
                        DisableRemoteLogOnTopicTest.class.getSimpleName().toLowerCase(Locale.ROOT),
                        BROKER_COUNT,
                        NUM_REMOTE_LOG_METADATA_PARTITIONS))
                .build());
    }

    @ClusterTemplate("clusterConfig")
    public void testDisableRemoteLogOnTopicWithClassicGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeDisableRemoteLogOnTopicTest(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTemplate("clusterConfig")
    public void testDisableRemoteLogOnTopicWithConsumerGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeDisableRemoteLogOnTopicTest(clusterInstance, GroupProtocol.CONSUMER);
    }

    private static void executeDisableRemoteLogOnTopicTest(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws Exception {
        final int broker0 = 0;
        final int broker1 = 1;
        final String topicA = "topicA";
        final int p0 = 0;
        final int partitionCount = 1;
        final int replicationFactor = 2;
        final int maxBatchCountPerSegment = 1;
        final boolean enableRemoteLogStorage = true;
        final Map<Integer, List<Integer>> assignment = mkMap(
                mkEntry(p0, List.of(broker0, broker1))
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

        TieredStorageTestBuilder builder = new TieredStorageTestBuilder();
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
                    List.of())

                // make sure we can still consume from the beginning of the topic to read data from local and remote storage
                .expectFetchFromTieredStorage(broker0, topicA, p0, 2)
                .consume(topicA, p0, 0L, 3, 2)

                // re-enable remote log copy
                .updateTopicConfig(topicA,
                    enableRemoteCopy,
                    List.of())

                // make sure the logs can be offloaded
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 3L)
                .produce(topicA, p0, new KeyValueSpec("k3", "v3"))

                // disable remote log copy again
                .updateTopicConfig(topicA,
                    disableRemoteCopy,
                    List.of())
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
                    List.of())
                // make sure all remote data is deleted
                .expectEmptyRemoteStorage(topicA, p0)
                // verify the local log is still consumable
                .consume(topicA, p0, 3L, 1, 0);

        builder.build().execute(clusterInstance, groupProtocol);
    }
}
