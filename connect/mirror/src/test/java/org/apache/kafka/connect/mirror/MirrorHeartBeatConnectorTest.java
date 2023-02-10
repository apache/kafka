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

import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class MirrorHeartBeatConnectorTest {

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
}
