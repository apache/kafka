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

import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


public class MirrorCheckpointConnectorTest {

    @Test
    public void testMirrorCheckpointConnectorDisabled() {
        // disable the checkpoint emission
        MirrorConnectorConfig config = new MirrorConnectorConfig(
            makeProps("emit.checkpoints.enabled", "false"));

        List<String> knownConsumerGroups = new ArrayList<>();
        knownConsumerGroups.add("consumer-group-1");
        // MirrorCheckpointConnector as minimum to run taskConfig()
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(knownConsumerGroups,
                                                                            config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect no task will be created
        assertEquals(0, output.size());
    }

    @Test
    public void testMirrorCheckpointConnectorEnabled() {
        // enable the checkpoint emission
        MirrorConnectorConfig config = new MirrorConnectorConfig(
                makeProps("emit.checkpoints.enabled", "true"));

        List<String> knownConsumerGroups = new ArrayList<>();
        knownConsumerGroups.add("consumer-group-1");
        // MirrorCheckpointConnector as minimum to run taskConfig()
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(knownConsumerGroups,
                config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect 1 task will be created
        assertEquals(1, output.size());
        assertEquals("consumer-group-1", output.get(0).get(MirrorConnectorConfig.TASK_CONSUMER_GROUPS));
    }

    @Test
    public void testNoConsumerGroup() {
        MirrorConnectorConfig config = new MirrorConnectorConfig(makeProps());
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(new ArrayList<>(), config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect no task will be created
        assertEquals(0, output.size());
    }

    @Test
    public void testReplicationDisabled() {
        // disable the replication
        MirrorConnectorConfig config = new MirrorConnectorConfig(makeProps("enabled", "false"));

        List<String> knownConsumerGroups = new ArrayList<>();
        knownConsumerGroups.add("consumer-group-1");
        // MirrorCheckpointConnector as minimum to run taskConfig()
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(knownConsumerGroups, config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect no task will be created
        assertEquals(0, output.size());
    }

    @Test
    public void testReplicationEnabled() {
        // enable the replication
        MirrorConnectorConfig config = new MirrorConnectorConfig(makeProps("enabled", "true"));

        List<String> knownConsumerGroups = new ArrayList<>();
        knownConsumerGroups.add("consumer-group-1");
        // MirrorCheckpointConnector as minimum to run taskConfig()
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(knownConsumerGroups, config);
        List<Map<String, String>> output = connector.taskConfigs(1);
        // expect 1 task will be created
        assertEquals(1, output.size());
        assertEquals("consumer-group-1", output.get(0).get(MirrorConnectorConfig.TASK_CONSUMER_GROUPS));
    }

    @Test
    public void testFindConsumerGroups() throws Exception {
        MirrorConnectorConfig config = new MirrorConnectorConfig(makeProps());
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector(Collections.emptyList(), config);
        connector = spy(connector);

        Collection<ConsumerGroupListing> groups = Arrays.asList(
                new ConsumerGroupListing("g1", true),
                new ConsumerGroupListing("g2", false));
        doReturn(groups).when(connector).listConsumerGroups();
        doReturn(true).when(connector).shouldReplicate(anyString());
        List<String> groupFound = connector.findConsumerGroups();

        Set<String> expectedGroups = groups.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toSet());
        assertEquals(expectedGroups, new HashSet<>(groupFound));
    }

}
