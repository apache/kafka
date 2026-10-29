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

import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.tiered.storage.TieredStorageTestBuilder;
import org.apache.kafka.tiered.storage.specs.KeyValueSpec;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.record.internal.RecordBatch.NO_PARTITION_LEADER_EPOCH;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent.EventType.DELETE_SEGMENT;
import static org.apache.kafka.tiered.storage.utils.TieredStorageTestUtils.createServerPropsForRemoteStorage;

public final class ListOffsetsTest {

    private static final int BROKER_COUNT = 3;
    private static final int NUM_REMOTE_LOG_METADATA_PARTITIONS = 5;

    private static List<ClusterConfig> clusterConfig() {
        return List.of(ClusterConfig.defaultBuilder()
                .setTypes(Set.of(Type.KRAFT))
                .setBrokers(BROKER_COUNT)
                .setServerProperties(createServerPropsForRemoteStorage(
                        ListOffsetsTest.class.getSimpleName().toLowerCase(Locale.ROOT),
                        BROKER_COUNT,
                        NUM_REMOTE_LOG_METADATA_PARTITIONS))
                .build());
    }

    @ClusterTemplate("clusterConfig")
    public void testListOffsetsWithClassicGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeListOffsetsTest(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTemplate("clusterConfig")
    public void testListOffsetsWithConsumerGroupProtocol(ClusterInstance clusterInstance) throws Exception {
        executeListOffsetsTest(clusterInstance, GroupProtocol.CONSUMER);
    }

    private void executeListOffsetsTest(ClusterInstance clusterInstance, GroupProtocol groupProtocol) throws Exception {
        final int broker0 = 0;
        final int broker1 = 1;
        final String topicA = "topicA";
        final int p0 = 0;
        final Time time = new MockTime();
        final long timestamp = time.milliseconds();
        final Map<Integer, List<Integer>> assignment = mkMap(mkEntry(p0, List.of(broker0, broker1)));

        TieredStorageTestBuilder builder = new TieredStorageTestBuilder();
        builder
                .createTopic(topicA, 1, 2, 2, assignment, true)
                // send records to partition 0 and expect the first segment to be offloaded
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 2L)
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0,
                        new KeyValueSpec("k0", "v0", timestamp),
                        new KeyValueSpec("k1", "v1", timestamp + 1))
                .produceWithTimestamp(topicA, p0,
                        new KeyValueSpec("k0", "v0", timestamp),
                        new KeyValueSpec("k1", "v1", timestamp + 1),
                        new KeyValueSpec("k2", "v2", timestamp + 2))

                // switch leader and send more records to partition 0 and expect the second segment to be offloaded.
                .reassignReplica(topicA, p0, List.of(broker1, broker0))
                // After leader election, the partition's leader-epoch gets bumped from 0 -> 1
                .expectLeader(topicA, p0, broker1, true)
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 4L)

                // NOTE that the (k2, v2) message was sent in the previous producer so we cannot expect that event in
                // the segment to be offloaded. We can only expect the new messages.
                .expectSegmentToBeOffloaded(broker1, topicA, p0, 2,
                        new KeyValueSpec("k3", "v3", timestamp + 3))
                .produceWithTimestamp(topicA, p0,
                        new KeyValueSpec("k3", "v3", timestamp + 3),
                        new KeyValueSpec("k4", "v4", timestamp + 4),
                        new KeyValueSpec("k5", "v5", timestamp + 5))

                // LIST_OFFSETS requests can list the offsets from least-loaded (any) node.
                // List offset for special timestamps
                .expectListOffsets(topicA, p0, OffsetSpec.earliest(), new EpochEntry(0, 0))
                .expectListOffsets(topicA, p0, OffsetSpec.earliestLocal(), new EpochEntry(1, 4))
                .expectListOffsets(topicA, p0, OffsetSpec.latestTiered(), new EpochEntry(1, 3))
                .expectListOffsets(topicA, p0, OffsetSpec.latest(), new EpochEntry(1, 6))

                // fetch offset using timestamp from the local disk
                .expectListOffsets(topicA, p0, OffsetSpec.forTimestamp(timestamp + 6), new EpochEntry(NO_PARTITION_LEADER_EPOCH, -1))
                .expectListOffsets(topicA, p0, OffsetSpec.forTimestamp(timestamp + 5), new EpochEntry(1, 5))
                .expectListOffsets(topicA, p0, OffsetSpec.forTimestamp(timestamp + 4), new EpochEntry(1, 4))

                // fetch offset using timestamp from the remote disk
                .expectListOffsets(topicA, p0, OffsetSpec.forTimestamp(timestamp - 1), new EpochEntry(0, 0))
                .expectListOffsets(topicA, p0, OffsetSpec.forTimestamp(timestamp), new EpochEntry(0, 0))
                .expectListOffsets(topicA, p0, OffsetSpec.forTimestamp(timestamp + 1), new EpochEntry(0, 1))
                .expectListOffsets(topicA, p0, OffsetSpec.forTimestamp(timestamp + 3), new EpochEntry(1, 3))

                // delete some records and check whether the earliest_offset gets updated.
                .expectDeletionInRemoteStorage(broker1, topicA, p0, DELETE_SEGMENT, 1)
                .deleteRecords(topicA, p0, 3L)
                .expectListOffsets(topicA, p0, OffsetSpec.earliest(), new EpochEntry(1, 3))
                .expectListOffsets(topicA, p0, OffsetSpec.earliestLocal(), new EpochEntry(1, 4))

                // delete all the records in remote layer and expect that earliest and earliest_local offsets are same
                .expectDeletionInRemoteStorage(broker1, topicA, p0, DELETE_SEGMENT, 1)
                .deleteRecords(topicA, p0, 4L)
                .expectListOffsets(topicA, p0, OffsetSpec.earliest(), new EpochEntry(1, 4))
                .expectListOffsets(topicA, p0, OffsetSpec.earliestLocal(), new EpochEntry(1, 4))
                .expectListOffsets(topicA, p0, OffsetSpec.latestTiered(), new EpochEntry(NO_PARTITION_LEADER_EPOCH, 3))
                .expectListOffsets(topicA, p0, OffsetSpec.latest(), new EpochEntry(1, 6));

        builder.build().execute(clusterInstance, groupProtocol);
    }
}
