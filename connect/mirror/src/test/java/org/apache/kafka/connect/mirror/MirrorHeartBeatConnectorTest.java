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
package org.apache.kafka.connect.mirror;

import static org.apache.kafka.connect.mirror.Heartbeat.SOURCE_CLUSTER_ALIAS_KEY;
import static org.apache.kafka.connect.mirror.Heartbeat.TARGET_CLUSTER_ALIAS_KEY;
import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

public class MirrorHeartBeatConnectorTest {

    private static final Map<String, ?> SOURCE_OFFSET = MirrorUtils.wrapOffset(0);

    @Test
    public void testMirrorHeartbeatConnectorDisabled() {
        // disable the heartbeat emission
        MirrorHeartbeatConfig config = new MirrorHeartbeatConfig(
            makeProps("emit.heartbeats.enabled", "false"));

        // MirrorHeartbeatConnector as minimum to run taskConfig()
        MirrorHeartbeatConnector connector = new MirrorHeartbeatConnector(config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect no task will be created
        assertEquals(0, output.size(), "Expected task to not be created");
    }

    @Test
    public void testReplicationDisabled() {
        // disable the replication
        MirrorHeartbeatConfig config = new MirrorHeartbeatConfig(
            makeProps("enabled", "false"));

        // MirrorHeartbeatConnector as minimum to run taskConfig()
        MirrorHeartbeatConnector connector = new MirrorHeartbeatConnector(config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect one task will be created, even the replication is disabled
        assertEquals(1, output.size(), "Task should have been created even with replication disabled");
    }

    @Test
    public void testAlterOffsetsIncorrectPartitionKey() {
        MirrorHeartbeatConnector connector = new MirrorHeartbeatConnector();
        assertThrows(ConnectException.class, () -> connector.alterOffsets(null, Collections.singletonMap(
                Collections.singletonMap("unused_partition_key", "unused_partition_value"),
                SOURCE_OFFSET
        )));

        // null partitions are invalid
        assertThrows(ConnectException.class, () -> connector.alterOffsets(null, Collections.singletonMap(
                null,
                SOURCE_OFFSET
        )));
    }

    @Test
    public void testAlterOffsetsMissingPartitionKey() {
        MirrorHeartbeatConnector connector = new MirrorHeartbeatConnector();

        Function<Map<String, ?>, Boolean> alterOffsets = partition -> connector.alterOffsets(null, Collections.singletonMap(
                partition,
                SOURCE_OFFSET
        ));

        Map<String, ?> validPartition = sourcePartition("primary", "backup");
        // Sanity check to make sure our valid partition is actually valid
        assertTrue(alterOffsets.apply(validPartition));

        for (String key : Arrays.asList(SOURCE_CLUSTER_ALIAS_KEY, TARGET_CLUSTER_ALIAS_KEY)) {
            Map<String, ?> invalidPartition = new HashMap<>(validPartition);
            invalidPartition.remove(key);
            assertThrows(ConnectException.class, () -> alterOffsets.apply(invalidPartition));
        }
    }

    @Test
    public void testAlterOffsetsMultiplePartitions() {
        MirrorHeartbeatConnector connector = new MirrorHeartbeatConnector();

        Map<String, ?> partition1 = sourcePartition("primary", "backup");
        Map<String, ?> partition2 = sourcePartition("backup", "primary");

        Map<Map<String, ?>, Map<String, ?>> offsets = new HashMap<>();
        offsets.put(partition1, SOURCE_OFFSET);
        offsets.put(partition2, SOURCE_OFFSET);

        assertTrue(connector.alterOffsets(null, offsets));
    }

    @Test
    public void testAlterOffsetsIncorrectOffsetKey() {
        MirrorHeartbeatConnector connector = new MirrorHeartbeatConnector();

        Map<Map<String, ?>, Map<String, ?>> offsets = Collections.singletonMap(
                sourcePartition("primary", "backup"),
                Collections.singletonMap("unused_offset_key", 0)
        );
        assertThrows(ConnectException.class, () -> connector.alterOffsets(null, offsets));
    }

    @Test
    public void testAlterOffsetsOffsetValues() {
        MirrorHeartbeatConnector connector = new MirrorHeartbeatConnector();

        Function<Object, Boolean> alterOffsets = offset -> connector.alterOffsets(null, Collections.singletonMap(
                sourcePartition("primary", "backup"),
                Collections.singletonMap(MirrorUtils.OFFSET_KEY, offset)
        ));

        assertThrows(ConnectException.class, () -> alterOffsets.apply("nan"));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(null));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(new Object()));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(3.14));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(-420));
        assertThrows(ConnectException.class, () -> alterOffsets.apply("-420"));
        assertThrows(ConnectException.class, () -> alterOffsets.apply("10"));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(10));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(((long) Integer.MAX_VALUE) + 1));
        assertTrue(() -> alterOffsets.apply(0));
    }

    @Test
    public void testSuccessfulAlterOffsets() {
        MirrorHeartbeatConnector connector = new MirrorHeartbeatConnector();

        Map<Map<String, ?>, Map<String, ?>> offsets = Collections.singletonMap(
                sourcePartition("primary", "backup"),
                SOURCE_OFFSET
        );

        // Expect no exception to be thrown when a valid offsets map is passed. An empty offsets map is treated as valid
        // since it could indicate that the offsets were reset previously or that no offsets have been committed yet
        // (for a reset operation)
        assertTrue(connector.alterOffsets(null, offsets));
        assertTrue(connector.alterOffsets(null, Collections.emptyMap()));
    }

    @Test
    public void testAlterOffsetsTombstones() {
        MirrorHeartbeatConnector connector = new MirrorHeartbeatConnector();

        Function<Map<String, ?>, Boolean> alterOffsets = partition -> connector.alterOffsets(
                null,
                Collections.singletonMap(partition, null)
        );

        Map<String, Object> partition = sourcePartition("src", "bak");
        assertTrue(() -> alterOffsets.apply(partition));
        partition.put(SOURCE_CLUSTER_ALIAS_KEY, 618);
        assertTrue(() -> alterOffsets.apply(partition));
        partition.remove(SOURCE_CLUSTER_ALIAS_KEY);
        assertTrue(() -> alterOffsets.apply(partition));

        assertTrue(() -> alterOffsets.apply(null));
        assertTrue(() -> alterOffsets.apply(Collections.emptyMap()));
        assertTrue(() -> alterOffsets.apply(Collections.singletonMap("unused_partition_key", "unused_partition_value")));
    }

    private static Map<String, Object> sourcePartition(String sourceClusterAlias, String targetClusterAlias) {
        Map<String, Object> result = new HashMap<>();
        result.put(SOURCE_CLUSTER_ALIAS_KEY, sourceClusterAlias);
        result.put(TARGET_CLUSTER_ALIAS_KEY, targetClusterAlias);
        return result;
    }
}
