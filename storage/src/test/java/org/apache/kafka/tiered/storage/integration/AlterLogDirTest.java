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
import org.apache.kafka.tiered.storage.specs.KeyValueSpec;

import java.util.Collections;
import java.util.List;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;

public final class AlterLogDirTest extends BaseReassignReplicaTest {

    /**
     * Alter dir within broker0
     * @return the replica-ids of the topic
     */
    @Override
    protected List<Integer> replicaIds() {
        return Collections.singletonList(broker0);
    }

    @Override
    public int brokerCount() {
        return 1;
    }

    @Override
    protected void writeTestSpecifications(TieredStorageTestBuilder builder) {
        final String topicB = "topicB";
        final int p0 = 0;
        final int partitionCount = 1;
        final int replicationFactor = 1;
        final int maxBatchCountPerSegment = 1;
        final boolean enableRemoteLogStorage = true;

        builder
                // create topicB with 1 partition and 1 RF
                .createTopic(topicB, partitionCount, replicationFactor, maxBatchCountPerSegment,
                        mkMap(mkEntry(p0, Collections.singletonList(broker0))), enableRemoteLogStorage)
                // send records to partition 0
                .expectSegmentToBeOffloaded(broker0, topicB, p0, 0, new KeyValueSpec("k0", "v0"))
                .expectSegmentToBeOffloaded(broker0, topicB, p0, 1, new KeyValueSpec("k1", "v1"))
                .expectEarliestLocalOffsetInLogDirectory(topicB, p0, 2L)
                .produce(topicB, p0, new KeyValueSpec("k0", "v0"), new KeyValueSpec("k1", "v1"),
                        new KeyValueSpec("k2", "v2"))
                // alter dir within the replica, we only expect one replicaId
                .alterLogDir(topicB, p0, replicaIds().get(0))
                .expectLeader(topicB, p0, broker0, true)
                // produce some more events and verify the earliest local offset
                .expectEarliestLocalOffsetInLogDirectory(topicB, p0, 3L)
                .produce(topicB, p0, new KeyValueSpec("k3", "v3"))
                // consume from the beginning of the topic to read data from local and remote storage
                .expectFetchFromTieredStorage(broker0, topicB, p0, 3)
                .consume(topicB, p0, 0L, 4, 3);
    }
}
