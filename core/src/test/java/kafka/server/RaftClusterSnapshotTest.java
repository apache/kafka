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

package kafka.server;

import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.raft.MetadataLogConfig;
import org.apache.kafka.snapshot.RecordsSnapshotReader;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
public class RaftClusterSnapshotTest {

    @Test
    public void testSnapshotsGenerated() throws Exception {
        int numberOfBrokers = 3;
        int numberOfControllers = 3;

        try (var cluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(numberOfBrokers)
                        .setNumControllerNodes(numberOfControllers)
                        .build())
                .setConfigProp(MetadataLogConfig.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG, "10")
                .setConfigProp(MetadataLogConfig.METADATA_MAX_IDLE_INTERVAL_MS_CONFIG, "0")
                .build()) {

            cluster.format();
            cluster.startup();

            // Check that every controller and broker has a snapshot
            TestUtils.waitForCondition(
                    () -> cluster.raftManagers().values().stream()
                            .allMatch(raftManager -> raftManager.raftLog().latestSnapshotId().isPresent()),
                    () -> "Expected for every controller and broker to generate a snapshot: " +
                            cluster.raftManagers().entrySet().stream()
                                    .collect(Collectors.toMap(
                                            Map.Entry::getKey,
                                            e -> e.getValue().raftLog().latestSnapshotId()
                                    ))
            );

            assertEquals(numberOfControllers + numberOfBrokers, cluster.raftManagers().size());

            // For every controller and broker perform some sanity checks against the latest snapshot
            for (var raftManager : cluster.raftManagers().values()) {
                try (var snapshot = RecordsSnapshotReader.of(
                        raftManager.raftLog().latestSnapshot().get(),
                        new MetadataRecordSerde(),
                        BufferSupplier.create(),
                        1,
                        true,
                        new LogContext()
                )) {
                    // Check that the snapshot is non-empty
                    assertTrue(snapshot.hasNext());

                    // Check that we can read the entire snapshot
                    while (snapshot.hasNext()) {
                        var batch = snapshot.next();
                        assertTrue(batch.sizeInBytes() > 0);
                        // A batch must have at least one control records or at least one data records, but not both
                        assertNotEquals(
                                batch.records().isEmpty(),
                                batch.controlRecords().isEmpty(),
                                "data records = " + batch.records() + "; control records = " + batch.controlRecords()
                        );
                    }
                }
            }
        }
    }
}
