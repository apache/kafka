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

package org.apache.kafka.server;

import kafka.server.BrokerServer;

import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.raft.RaftManager;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.RecordsSnapshotReader;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.raft.MetadataLogConfig.METADATA_MAX_IDLE_INTERVAL_MS_CONFIG;
import static org.apache.kafka.raft.MetadataLogConfig.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
@ClusterTestDefaults(
        types = Type.KRAFT,
        brokers = RaftClusterSnapshotTest.BROKER_COUNT,
        controllers = RaftClusterSnapshotTest.CONTROLLER_COUNT,
        serverProperties = {
            @ClusterConfigProperty(key = METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG, value = "10"),
            @ClusterConfigProperty(key = METADATA_MAX_IDLE_INTERVAL_MS_CONFIG, value = "0"),
        })
public class RaftClusterSnapshotTest {

    public static final int BROKER_COUNT = 3;
    public static final int CONTROLLER_COUNT = 3;

    private final ClusterInstance clusterInstance;

    public RaftClusterSnapshotTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    private Map<Integer, RaftManager<ApiMessageAndVersion>> raftManagers() {
        Map<Integer, RaftManager<ApiMessageAndVersion>> results = new HashMap<>();
        clusterInstance.brokers().values().forEach(broker -> {
            if (broker instanceof BrokerServer brokerServer) {
                results.put(brokerServer.config().brokerId(), brokerServer.sharedServer().raftManager());
            }
        });

        clusterInstance.controllers().values().forEach(controller ->
                results.putIfAbsent(controller.config().nodeId(), controller.sharedServer().raftManager())
        );
        return results;
    }

    @ClusterTest
    public void testSnapshotsGenerated() throws Exception {

        var raftManagers = raftManagers();
        // Check that every controller and broker has a snapshot
        TestUtils.waitForCondition(
                () -> raftManagers.values().stream()
                        .allMatch(raftManager -> raftManager.raftLog().latestSnapshotId().isPresent()),
                () -> "Expected for every controller and broker to generate a snapshot: " +
                        raftManagers.entrySet().stream()
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> e.getValue().raftLog().latestSnapshotId()
                                ))
        );

        assertEquals(BROKER_COUNT + CONTROLLER_COUNT, raftManagers.size());

        // For every controller and broker perform some sanity checks against the latest snapshot
        for (var raftManager : raftManagers.values()) {
            try (var snapshot = RecordsSnapshotReader.of(
                    raftManager.raftLog().latestSnapshot().get(),
                    MetadataRecordSerde.INSTANCE,
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
