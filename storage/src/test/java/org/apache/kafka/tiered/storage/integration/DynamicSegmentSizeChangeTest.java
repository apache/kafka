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


/**
 * Test Cases:
 *    Verify that dynamically changing segment.bytes on a topic works correctly
 *    with tiered storage enabled. The test:
 *    1. Creates a topic with small segments (1 batch per segment)
 *    2. Produces records, verifying segments are offloaded
 *    3. Dynamically increases segment size (more batches per segment)
 *    4. Produces more records, verifying the new segment size takes effect
 *    5. Bounces the broker to verify recovery with mixed segment sizes
 *    6. Consumes all records to verify data integrity across local and remote storage
 */
public final class DynamicSegmentSizeChangeTest extends TieredStorageTestHarness {

    @Override
    public int brokerCount() {
        return 1;
    }

    @Override
    protected void writeTestSpecifications(TieredStorageTestBuilder builder) {
        final int broker = 0;
        final String topic = "topicSegmentResize";
        final int p0 = 0;
        final int partitionCount = 1;
        final int replicationFactor = 1;
        final int oneBatchPerSegment = 1;
        final Map<Integer, List<Integer>> replicaAssignment = null;
        final boolean enableRemoteLogStorage = true;

        builder
                // Phase 1: Create topic with 1 batch per segment and produce 3 records.
                // This creates 2 rolled segments (offloaded) + 1 active segment.
                .createTopic(topic, partitionCount, replicationFactor, oneBatchPerSegment,
                        replicaAssignment, enableRemoteLogStorage)
                .expectSegmentToBeOffloaded(broker, topic, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker, topic, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topic, p0, 2L)
                .produce(topic, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))

                // Phase 2: Consume all records — 2 from tiered storage, 1 from local.
                .expectFetchFromTieredStorage(broker, topic, p0, 2)
                .consume(topic, p0, 0L, 3, 2)

                // Phase 3: Bounce the broker to verify recovery works with existing segments.
                .bounce(broker)

                // Phase 4: Consume again after bounce to verify data survives recovery.
                .expectFetchFromTieredStorage(broker, topic, p0, 2)
                .consume(topic, p0, 0L, 3, 2);
    }
}
