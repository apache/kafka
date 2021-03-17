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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;

import org.junit.jupiter.api.Test;

import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.TASK_TOPIC_PARTITIONS;
import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MirrorSourceConnectorTest {

    @Test
    public void testReplicatesHeartbeatsByDefault() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"), 
            new DefaultReplicationPolicy(), new DefaultTopicFilter(), new DefaultConfigPropertyFilter());
        assertTrue(connector.shouldReplicateTopic("heartbeats"), "should replicate heartbeats");
        assertTrue(connector.shouldReplicateTopic("us-west.heartbeats"), "should replicate upstream heartbeats");
    }

    @Test
    public void testReplicatesHeartbeatsDespiteFilter() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> false, new DefaultConfigPropertyFilter());
        assertTrue(connector.shouldReplicateTopic("heartbeats"), "should replicate heartbeats");
        assertTrue(connector.shouldReplicateTopic("us-west.heartbeats"), "should replicate upstream heartbeats");
    }

    @Test
    public void testNoCycles() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> true, x -> true);
        assertFalse(connector.shouldReplicateTopic("target.topic1"), "should not allow cycles");
        assertFalse(connector.shouldReplicateTopic("target.source.topic1"), "should not allow cycles");
        assertFalse(connector.shouldReplicateTopic("source.target.topic1"), "should not allow cycles");
        assertTrue(connector.shouldReplicateTopic("topic1"), "should allow anything else");
        assertTrue(connector.shouldReplicateTopic("source.topic1"), "should allow anything else");
    }

    @Test
    public void testAclFiltering() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> true, x -> true);
        assertFalse(connector.shouldReplicateAcl(
            new AclBinding(new ResourcePattern(ResourceType.TOPIC, "test_topic", PatternType.LITERAL),
            new AccessControlEntry("kafka", "", AclOperation.WRITE, AclPermissionType.ALLOW))), "should not replicate ALLOW WRITE");
        assertTrue(connector.shouldReplicateAcl(
            new AclBinding(new ResourcePattern(ResourceType.TOPIC, "test_topic", PatternType.LITERAL),
            new AccessControlEntry("kafka", "", AclOperation.ALL, AclPermissionType.ALLOW))), "should replicate ALLOW ALL");
    }

    @Test
    public void testAclTransformation() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> true, x -> true);
        AclBinding allowAllAclBinding = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "test_topic", PatternType.LITERAL),
            new AccessControlEntry("kafka", "", AclOperation.ALL, AclPermissionType.ALLOW));
        AclBinding processedAllowAllAclBinding = connector.targetAclBinding(allowAllAclBinding);
        String expectedRemoteTopicName = "source" + DefaultReplicationPolicy.SEPARATOR_DEFAULT
            + allowAllAclBinding.pattern().name();
        assertEquals(expectedRemoteTopicName, processedAllowAllAclBinding.pattern().name(), "should change topic name");
        assertEquals(processedAllowAllAclBinding.entry().operation(), AclOperation.READ, "should change ALL to READ");
        assertEquals(processedAllowAllAclBinding.entry().permissionType(), AclPermissionType.ALLOW, "should not change ALLOW");

        AclBinding denyAllAclBinding = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "test_topic", PatternType.LITERAL),
            new AccessControlEntry("kafka", "", AclOperation.ALL, AclPermissionType.DENY));
        AclBinding processedDenyAllAclBinding = connector.targetAclBinding(denyAllAclBinding);
        assertEquals(processedDenyAllAclBinding.entry().operation(), AclOperation.ALL, "should not change ALL");
        assertEquals(processedDenyAllAclBinding.entry().permissionType(), AclPermissionType.DENY, "should not change DENY");
    }
    
    @Test
    public void testConfigPropertyFiltering() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> true, new DefaultConfigPropertyFilter());
        ArrayList<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry("name-1", "value-1"));
        entries.add(new ConfigEntry("min.insync.replicas", "2"));
        Config config = new Config(entries);
        Config targetConfig = connector.targetConfig(config);
        assertTrue(targetConfig.entries().stream()
            .anyMatch(x -> x.name().equals("name-1")), "should replicate properties");
        assertFalse(targetConfig.entries().stream()
            .anyMatch(x -> x.name().equals("min.insync.replicas")), "should not replicate excluded properties");
    }

    @Test
    public void testMirrorSourceConnectorTaskConfig() {
        List<TopicPartition> knownSourceTopicPartitions = new ArrayList<>();

        // topic `t0` has 8 partitions
        knownSourceTopicPartitions.add(new TopicPartition("t0", 0));
        knownSourceTopicPartitions.add(new TopicPartition("t0", 1));
        knownSourceTopicPartitions.add(new TopicPartition("t0", 2));
        knownSourceTopicPartitions.add(new TopicPartition("t0", 3));
        knownSourceTopicPartitions.add(new TopicPartition("t0", 4));
        knownSourceTopicPartitions.add(new TopicPartition("t0", 5));
        knownSourceTopicPartitions.add(new TopicPartition("t0", 6));
        knownSourceTopicPartitions.add(new TopicPartition("t0", 7));

        // topic `t1` has 2 partitions
        knownSourceTopicPartitions.add(new TopicPartition("t1", 0));
        knownSourceTopicPartitions.add(new TopicPartition("t1", 1));

        // topic `t2` has 2 partitions
        knownSourceTopicPartitions.add(new TopicPartition("t2", 0));
        knownSourceTopicPartitions.add(new TopicPartition("t2", 1));

        // MirrorConnectorConfig example for test
        MirrorConnectorConfig config = new MirrorConnectorConfig(makeProps());

        // MirrorSourceConnector as minimum to run taskConfig()
        MirrorSourceConnector connector = new MirrorSourceConnector(knownSourceTopicPartitions, config);

        // distribute the topic-partition to 3 tasks by round-robin
        List<Map<String, String>> output = connector.taskConfigs(3);

        // the expected assignments over 3 tasks:
        // t1 -> [t0p0, t0p3, t0p6, t1p1]
        // t2 -> [t0p1, t0p4, t0p7, t2p0]
        // t3 -> [t0p2, t0p5, t1p0, t2p1]

        Map<String, String> t1 = output.get(0);
        assertEquals("t0-0,t0-3,t0-6,t1-1", t1.get(TASK_TOPIC_PARTITIONS));

        Map<String, String> t2 = output.get(1);
        assertEquals("t0-1,t0-4,t0-7,t2-0", t2.get(TASK_TOPIC_PARTITIONS));

        Map<String, String> t3 = output.get(2);
        assertEquals("t0-2,t0-5,t1-0,t2-1", t3.get(TASK_TOPIC_PARTITIONS));
    }

    @Test
    public void testRefreshTopicPartitions() throws Exception {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
                new DefaultReplicationPolicy(), new DefaultTopicFilter(), new DefaultConfigPropertyFilter());
        connector.initialize(mock(ConnectorContext.class));
        connector = spy(connector);

        Config topicConfig = new Config(Arrays.asList(
                new ConfigEntry("cleanup.policy", "compact"),
                new ConfigEntry("segment.bytes", "100")));
        Map<String, Config> configs = Collections.singletonMap("topic", topicConfig);

        List<TopicPartition> sourceTopicPartitions = Collections.singletonList(new TopicPartition("topic", 0));
        doReturn(sourceTopicPartitions).when(connector).findSourceTopicPartitions();
        doReturn(Collections.emptyList()).when(connector).findTargetTopicPartitions();
        doReturn(configs).when(connector).describeTopicConfigs(Collections.singleton("topic"));
        doNothing().when(connector).createNewTopics(any());

        connector.refreshTopicPartitions();
        // if target topic is not created, refreshTopicPartitions() will call createTopicPartitions() again
        connector.refreshTopicPartitions();

        Map<String, Long> expectedPartitionCounts = new HashMap<>();
        expectedPartitionCounts.put("source.topic", 1L);
        Map<String, String> configMap = MirrorSourceConnector.configToMap(topicConfig);
        assertEquals(2, configMap.size());

        Map<String, NewTopic> expectedNewTopics = new HashMap<>();
        expectedNewTopics.put("source.topic", new NewTopic("source.topic", 1, (short) 0).configs(configMap));

        verify(connector, times(2)).computeAndCreateTopicPartitions();
        verify(connector, times(2)).createNewTopics(eq(expectedNewTopics));
        verify(connector, times(0)).createNewPartitions(any());

        List<TopicPartition> targetTopicPartitions = Collections.singletonList(new TopicPartition("source.topic", 0));
        doReturn(targetTopicPartitions).when(connector).findTargetTopicPartitions();
        connector.refreshTopicPartitions();

        // once target topic is created, refreshTopicPartitions() will NOT call computeAndCreateTopicPartitions() again
        verify(connector, times(2)).computeAndCreateTopicPartitions();
    }

    @Test
    public void testRefreshTopicPartitionsTopicOnTargetFirst() throws Exception {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
                new DefaultReplicationPolicy(), new DefaultTopicFilter(), new DefaultConfigPropertyFilter());
        connector.initialize(mock(ConnectorContext.class));
        connector = spy(connector);

        Config topicConfig = new Config(Arrays.asList(
                new ConfigEntry("cleanup.policy", "compact"),
                new ConfigEntry("segment.bytes", "100")));
        Map<String, Config> configs = Collections.singletonMap("source.topic", topicConfig);

        List<TopicPartition> sourceTopicPartitions = Collections.emptyList();
        List<TopicPartition> targetTopicPartitions = Collections.singletonList(new TopicPartition("source.topic", 0));
        doReturn(sourceTopicPartitions).when(connector).findSourceTopicPartitions();
        doReturn(targetTopicPartitions).when(connector).findTargetTopicPartitions();
        doReturn(configs).when(connector).describeTopicConfigs(Collections.singleton("source.topic"));
        doReturn(Collections.emptyMap()).when(connector).describeTopicConfigs(Collections.emptySet());
        doNothing().when(connector).createNewTopics(any());
        doNothing().when(connector).createNewPartitions(any());

        // partitions appearing on the target cluster should not cause reconfiguration
        connector.refreshTopicPartitions();
        connector.refreshTopicPartitions();
        verify(connector, times(0)).computeAndCreateTopicPartitions();

        sourceTopicPartitions = Collections.singletonList(new TopicPartition("topic", 0));
        doReturn(sourceTopicPartitions).when(connector).findSourceTopicPartitions();

        // when partitions are added to the source cluster, reconfiguration is triggered
        connector.refreshTopicPartitions();
        verify(connector, times(1)).computeAndCreateTopicPartitions();
    }
}
