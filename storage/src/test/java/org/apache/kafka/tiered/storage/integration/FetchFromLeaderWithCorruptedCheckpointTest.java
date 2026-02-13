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

import kafka.server.ReplicaManager;

import org.apache.kafka.storage.internals.checkpoint.CleanShutdownFileHandler;
import org.apache.kafka.storage.internals.log.LogManager;
import org.apache.kafka.tiered.storage.TieredStorageTestBuilder;
import org.apache.kafka.tiered.storage.TieredStorageTestHarness;
import org.apache.kafka.tiered.storage.specs.KeyValueSpec;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public class FetchFromLeaderWithCorruptedCheckpointTest extends TieredStorageTestHarness {

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
        final int partitionCount = 1;
        final int replicationFactor = 2;
        final int maxBatchCountPerSegment = 1;
        final boolean enableRemoteLogStorage = true;
        final Map<Integer, List<Integer>> assignment = mkMap(mkEntry(p0, List.of(broker0, broker1)));
        final List<String> checkpointFiles = List.of(
                ReplicaManager.HighWatermarkFilename(),
                LogManager.RECOVERY_POINT_CHECKPOINT_FILE,
                CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME);

        builder.createTopic(topicA, partitionCount, replicationFactor, maxBatchCountPerSegment, assignment,
                        enableRemoteLogStorage)
                // send records to partition 0
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 2L)
                .produce(topicA, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                .expectFetchFromTieredStorage(broker0, topicA, p0, 2)
                .consume(topicA, p0, 0L, 3, 2)
                // shutdown the brokers
                .stop(broker1)
                .stop(broker0)
                // delete the checkpoint files
                .eraseBrokerStorage(broker0, (dir, name) -> checkpointFiles.contains(name), true)
                // start the broker first whose checkpoint files were deleted.
                .start(broker0)
                .start(broker1)
                // send some records to partition 0
                // Note that the segment 2 gets offloaded for p0, but we cannot expect those events deterministically
                // because the rlm-task-thread runs in background and this framework doesn't support it.
                .expectSegmentToBeOffloaded(broker0, topicA, p0, 3, new KeyValueSpec("k3", "v3"))
                .expectEarliestLocalOffsetInLogDirectory(topicA, p0, 4L)
                .produce(topicA, p0, new KeyValueSpec("k3", "v3"), new KeyValueSpec("k4", "v4"))
                .expectFetchFromTieredStorage(broker0, topicA, p0, 4)
                .consume(topicA, p0, 0L, 5, 4);

    }
}
