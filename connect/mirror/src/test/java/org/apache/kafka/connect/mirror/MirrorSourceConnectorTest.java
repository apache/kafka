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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.ExactlyOnceSupport;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.CONSUMER_CLIENT_PREFIX;
import static org.apache.kafka.connect.mirror.MirrorConnectorConfig.SOURCE_PREFIX;
import static org.apache.kafka.connect.mirror.MirrorSourceConfig.OFFSET_LAG_MAX;
import static org.apache.kafka.connect.mirror.MirrorSourceConfig.TASK_TOPIC_PARTITIONS;
import static org.apache.kafka.connect.mirror.MirrorUtils.PARTITION_KEY;
import static org.apache.kafka.connect.mirror.MirrorUtils.SOURCE_CLUSTER_KEY;
import static org.apache.kafka.connect.mirror.MirrorUtils.TOPIC_KEY;
import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class MirrorSourceConnectorTest {
    private ConfigPropertyFilter getConfigPropertyFilter() {
        return prop -> true;
    }

    @Test
    public void testReplicatesHeartbeatsByDefault() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), new DefaultTopicFilter(), new DefaultConfigPropertyFilter());
        assertTrue(connector.shouldReplicateTopic("heartbeats"), "should replicate heartbeats");
        assertTrue(connector.shouldReplicateTopic("us-west.heartbeats"), "should replicate upstream heartbeats");
    }

    @Test
    public void testReplicatesHeartbeatsDespiteFilter() {
        DefaultReplicationPolicy defaultReplicationPolicy = new DefaultReplicationPolicy();
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            defaultReplicationPolicy, x -> false, new DefaultConfigPropertyFilter());
        assertTrue(connector.shouldReplicateTopic("heartbeats"), "should replicate heartbeats");
        assertTrue(connector.shouldReplicateTopic("us-west.heartbeats"), "should replicate upstream heartbeats");

        Map<String, ?> configs = Collections.singletonMap(DefaultReplicationPolicy.SEPARATOR_CONFIG, "_");
        defaultReplicationPolicy.configure(configs);
        assertTrue(connector.shouldReplicateTopic("heartbeats"), "should replicate heartbeats");
        assertFalse(connector.shouldReplicateTopic("us-west.heartbeats"), "should not consider this topic as a heartbeats topic");
    }

    @Test
    public void testDoesNotReplicateHeartbeatsWhenDisabled() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
                new DefaultReplicationPolicy(), new DefaultTopicFilter(), new DefaultConfigPropertyFilter(), false);
        assertFalse(connector.shouldReplicateTopic("heartbeats"), "should not replicate heartbeats");
        assertFalse(connector.shouldReplicateTopic("us-west.heartbeats"), "should not replicate upstream heartbeats");
    }

    @Test
    public void testReplicatesHeartbeatsWhenDisabledButFilterAllows() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
                new DefaultReplicationPolicy(), x -> true, new DefaultConfigPropertyFilter(), false);
        assertTrue(connector.shouldReplicateTopic("heartbeats"), "should replicate heartbeats");
        assertTrue(connector.shouldReplicateTopic("us-west.heartbeats"), "should replicate upstream heartbeats");
    }

    @Test
    public void testNoCycles() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> true, getConfigPropertyFilter());
        assertFalse(connector.shouldReplicateTopic("source.topic1"), "should not allow cycles");
        assertFalse(connector.shouldReplicateTopic("target.topic1"), "should not allow cycles");
        assertFalse(connector.shouldReplicateTopic("target.source.topic1"), "should not allow cycles");
        assertFalse(connector.shouldReplicateTopic("source.target.topic1"), "should not allow cycles");
        assertFalse(connector.shouldReplicateTopic("target.source.target.topic1"), "should not allow cycles");
        assertFalse(connector.shouldReplicateTopic("source.target.source.topic1"), "should not allow cycles");
        assertTrue(connector.shouldReplicateTopic("topic1"), "should allow anything else");
        assertTrue(connector.shouldReplicateTopic("othersource.topic1"), "should allow anything else");
        assertTrue(connector.shouldReplicateTopic("othertarget.topic1"), "should allow anything else");
        assertTrue(connector.shouldReplicateTopic("other.another.topic1"), "should allow anything else");

        final IdentityReplicationPolicy identityReplicationPolicy = new IdentityReplicationPolicy();
        final HashMap<String, String> props = new HashMap<>();
        props.put("source.cluster.alias", "source");
        identityReplicationPolicy.configure(props);
        connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            identityReplicationPolicy, x -> true, x -> true);
        assertTrue(connector.shouldReplicateTopic("source.topic1"), "should not consider this a cycle");
        assertTrue(connector.shouldReplicateTopic("target.topic1"), "should not consider this a cycle");
        assertTrue(connector.shouldReplicateTopic("target.source.topic1"), "should not consider this a cycle");
        assertTrue(connector.shouldReplicateTopic("source.target.topic1"), "should not consider this a cycle");
        assertTrue(connector.shouldReplicateTopic("topic1"), "should not consider this a cycle");
        assertTrue(connector.shouldReplicateTopic("othersource.topic1"), "should not consider this a cycle");
        assertTrue(connector.shouldReplicateTopic("othertarget.topic1"), "should not consider this a cycle");
        assertTrue(connector.shouldReplicateTopic("other.another.topic1"), "should not consider this a cycle");
    }

    @Test
    public void testIdentityReplication() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new IdentityReplicationPolicy(), x -> true, getConfigPropertyFilter());
        assertTrue(connector.shouldReplicateTopic("target.topic1"), "should allow cycles");
        assertTrue(connector.shouldReplicateTopic("target.source.topic1"), "should allow cycles");
        assertTrue(connector.shouldReplicateTopic("source.target.topic1"), "should allow cycles");
        assertTrue(connector.shouldReplicateTopic("target.source.target.topic1"), "should allow cycles");
        assertTrue(connector.shouldReplicateTopic("source.target.source.topic1"), "should allow cycles");
        assertTrue(connector.shouldReplicateTopic("topic1"), "should allow normal topics");
        assertTrue(connector.shouldReplicateTopic("othersource.topic1"), "should allow normal topics");
        assertFalse(connector.shouldReplicateTopic("target.heartbeats"), "should not allow heartbeat cycles");
        assertFalse(connector.shouldReplicateTopic("target.source.heartbeats"), "should not allow heartbeat cycles");
        assertFalse(connector.shouldReplicateTopic("source.target.heartbeats"), "should not allow heartbeat cycles");
        assertFalse(connector.shouldReplicateTopic("target.source.target.heartbeats"), "should not allow heartbeat cycles");
        assertFalse(connector.shouldReplicateTopic("source.target.source.heartbeats"), "should not allow heartbeat cycles");
        assertTrue(connector.shouldReplicateTopic("heartbeats"), "should allow heartbeat topics");
        assertTrue(connector.shouldReplicateTopic("othersource.heartbeats"), "should allow heartbeat topics");
    }

    @Test
    public void testAclFiltering() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> true, getConfigPropertyFilter());
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
            new DefaultReplicationPolicy(), x -> true, getConfigPropertyFilter());
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
    public void testNoBrokerAclAuthorizer() throws Exception {
        Admin sourceAdmin = mock(Admin.class);
        Admin targetAdmin = mock(Admin.class);
        MirrorSourceConnector connector = new MirrorSourceConnector(sourceAdmin, targetAdmin,
                new MirrorSourceConfig(makeProps()));

        ExecutionException describeAclsFailure = new ExecutionException(
                "Failed to describe ACLs",
                new SecurityDisabledException("No ACL authorizer configured on this broker")
        );
        @SuppressWarnings("unchecked")
        KafkaFuture<Collection<AclBinding>> describeAclsFuture = mock(KafkaFuture.class);
        when(describeAclsFuture.get()).thenThrow(describeAclsFailure);
        DescribeAclsResult describeAclsResult = mock(DescribeAclsResult.class);
        when(describeAclsResult.values()).thenReturn(describeAclsFuture);
        when(sourceAdmin.describeAcls(any())).thenReturn(describeAclsResult);

        try (LogCaptureAppender connectorLogs = LogCaptureAppender.createAndRegister(MirrorSourceConnector.class)) {
            connectorLogs.setClassLogger(MirrorSourceConnector.class, Level.TRACE);
            connector.syncTopicAcls();
            long aclSyncDisableMessages = connectorLogs.getMessages().stream()
                    .filter(m -> m.contains("Consider disabling topic ACL syncing"))
                    .count();
            assertEquals(1, aclSyncDisableMessages, "Should have recommended that user disable ACL syncing");
            long aclSyncSkippingMessages = connectorLogs.getMessages().stream()
                    .filter(m -> m.contains("skipping topic ACL sync"))
                    .count();
            assertEquals(0, aclSyncSkippingMessages, "Should not have logged ACL sync skip at same time as suggesting ACL sync be disabled");

            connector.syncTopicAcls();
            connector.syncTopicAcls();
            aclSyncDisableMessages = connectorLogs.getMessages().stream()
                    .filter(m -> m.contains("Consider disabling topic ACL syncing"))
                    .count();
            assertEquals(1, aclSyncDisableMessages, "Should not have recommended that user disable ACL syncing more than once");
            aclSyncSkippingMessages = connectorLogs.getMessages().stream()
                    .filter(m -> m.contains("skipping topic ACL sync"))
                    .count();
            assertEquals(2, aclSyncSkippingMessages, "Should have logged ACL sync skip instead of suggesting disabling ACL syncing");
        }

        // We should never have tried to perform an ACL sync on the target cluster
        verifyNoInteractions(targetAdmin);
    }

    @Test
    public void testMissingDescribeConfigsAcl() throws Exception {
        Admin sourceAdmin = mock(Admin.class);
        MirrorSourceConnector connector = new MirrorSourceConnector(
                sourceAdmin,
                mock(Admin.class),
                new MirrorSourceConfig(makeProps())
        );

        ExecutionException describeConfigsFailure = new ExecutionException(
                "Failed to describe topic configs",
                new TopicAuthorizationException("Topic authorization failed")
        );
        @SuppressWarnings("unchecked")
        KafkaFuture<Map<ConfigResource, Config>> describeConfigsFuture = mock(KafkaFuture.class);
        when(describeConfigsFuture.get()).thenThrow(describeConfigsFailure);
        DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
        when(describeConfigsResult.all()).thenReturn(describeConfigsFuture);
        when(sourceAdmin.describeConfigs(any())).thenReturn(describeConfigsResult);

        try (LogCaptureAppender connectorLogs = LogCaptureAppender.createAndRegister(MirrorUtils.class)) {
            connectorLogs.setClassLogger(MirrorUtils.class, Level.TRACE);
            Set<String> topics = new HashSet<>();
            topics.add("topic1");
            topics.add("topic2");
            ExecutionException exception = assertThrows(ExecutionException.class, () -> connector.describeTopicConfigs(topics));
            assertEquals(
                    exception.getCause().getClass().getSimpleName() + " occurred while trying to describe configs for topics [topic1, topic2] on source1 cluster",
                    connectorLogs.getMessages().get(0)
            );
        }
    }

    @Test
    public void testConfigPropertyFiltering() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> true, new DefaultConfigPropertyFilter());
        ArrayList<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry("name-1", "value-1"));
        entries.add(new ConfigEntry("name-2", "value-2", ConfigEntry.ConfigSource.DEFAULT_CONFIG, false, false, Collections.emptyList(), ConfigEntry.ConfigType.STRING, ""));
        entries.add(new ConfigEntry("min.insync.replicas", "2"));
        Config config = new Config(entries);
        Config targetConfig = connector.targetConfig(config, true);
        assertTrue(targetConfig.entries().stream()
            .anyMatch(x -> x.name().equals("name-1")), "should replicate properties");
        assertTrue(targetConfig.entries().stream()
            .anyMatch(x -> x.name().equals("name-2")), "should include default properties");
        assertFalse(targetConfig.entries().stream()
            .anyMatch(x -> x.name().equals("min.insync.replicas")), "should not replicate excluded properties");
    }

    @Test
    @Deprecated
    public void testConfigPropertyFilteringWithAlterConfigs() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
                new DefaultReplicationPolicy(), x -> true, new DefaultConfigPropertyFilter());
        List<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry("name-1", "value-1"));
        // When "use.defaults.from" set to "target" by default, the config with default value should be excluded
        entries.add(new ConfigEntry("name-2", "value-2", ConfigEntry.ConfigSource.DEFAULT_CONFIG, false, false, Collections.emptyList(), ConfigEntry.ConfigType.STRING, ""));
        entries.add(new ConfigEntry("min.insync.replicas", "2"));
        Config config = new Config(entries);
        Config targetConfig = connector.targetConfig(config, false);
        assertTrue(targetConfig.entries().stream()
            .anyMatch(x -> x.name().equals("name-1")), "should replicate properties");
        assertFalse(targetConfig.entries().stream()
            .anyMatch(x -> x.name().equals("name-2")), "should not replicate default properties");
        assertFalse(targetConfig.entries().stream()
            .anyMatch(x -> x.name().equals("min.insync.replicas")), "should not replicate excluded properties");
    }

    @Test
    @Deprecated
    public void testConfigPropertyFilteringWithAlterConfigsAndSourceDefault() {
        Map<String, Object> filterConfig = Collections.singletonMap(DefaultConfigPropertyFilter.USE_DEFAULTS_FROM, "source");
        DefaultConfigPropertyFilter filter = new DefaultConfigPropertyFilter();
        filter.configure(filterConfig);

        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
                new DefaultReplicationPolicy(),  x -> true, filter);
        List<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry("name-1", "value-1"));
        // When "use.defaults.from" explicitly set to "source", the config with default value should be replicated
        entries.add(new ConfigEntry("name-2", "value-2", ConfigEntry.ConfigSource.DEFAULT_CONFIG, false, false, Collections.emptyList(), ConfigEntry.ConfigType.STRING, ""));
        entries.add(new ConfigEntry("min.insync.replicas", "2"));
        Config config = new Config(entries);
        Config targetConfig = connector.targetConfig(config, false);
        assertTrue(targetConfig.entries().stream()
                .anyMatch(x -> x.name().equals("name-1")), "should replicate properties");
        assertTrue(targetConfig.entries().stream()
                .anyMatch(x -> x.name().equals("name-2")), "should include default properties");
        assertFalse(targetConfig.entries().stream()
                .anyMatch(x -> x.name().equals("min.insync.replicas")), "should not replicate excluded properties");
    }

    @Test
    public void testNewTopicConfigs() throws Exception {
        Map<String, Object> filterConfig = new HashMap<>();
        filterConfig.put(DefaultConfigPropertyFilter.CONFIG_PROPERTIES_EXCLUDE_CONFIG, "follower\\.replication\\.throttled\\.replicas, "
                + "leader\\.replication\\.throttled\\.replicas, "
                + "message\\.timestamp\\.difference\\.max\\.ms, "
                + "message\\.timestamp\\.type, "
                + "unclean\\.leader\\.election\\.enable, "
                + "min\\.insync\\.replicas,"
                + "exclude_param.*");
        DefaultConfigPropertyFilter filter = new DefaultConfigPropertyFilter();
        filter.configure(filterConfig);

        MirrorSourceConnector connector = spy(new MirrorSourceConnector(new SourceAndTarget("source", "target"),
                new DefaultReplicationPolicy(), x -> true, filter));

        final String topic = "testtopic";
        List<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry("name-1", "value-1"));
        entries.add(new ConfigEntry("exclude_param.param1", "value-param1"));
        entries.add(new ConfigEntry("min.insync.replicas", "2"));
        Config config = new Config(entries);
        doReturn(Collections.singletonMap(topic, config)).when(connector).describeTopicConfigs(any());
        doAnswer(invocation -> {
            Map<String, NewTopic> newTopics = invocation.getArgument(0);
            assertNotNull(newTopics.get("source." + topic));
            Map<String, String> targetConfig = newTopics.get("source." + topic).configs();

            // property 'name-1' isn't defined in the exclude filter -> should be replicated
            assertNotNull(targetConfig.get("name-1"), "should replicate properties");

            // this property is in default list, just double check it:
            String prop1 = "min.insync.replicas";
            assertNull(targetConfig.get(prop1), "should not replicate excluded properties " + prop1);
            // this property is only in exclude filter custom parameter, also tests regex on the way:
            String prop2 = "exclude_param.param1";
            assertNull(targetConfig.get(prop2), "should not replicate excluded properties " + prop2);
            return null;
        }).when(connector).createNewTopics(any());
        connector.createNewTopics(Collections.singleton(topic), Collections.singletonMap(topic, 1L));
        verify(connector).createNewTopics(any(), any());
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
        MirrorSourceConfig config = new MirrorSourceConfig(makeProps());

        // MirrorSourceConnector as minimum to run taskConfig()
        MirrorSourceConnector connector = new MirrorSourceConnector(knownSourceTopicPartitions, config);

        // distribute the topic-partition to 3 tasks by round-robin
        List<Map<String, String>> output = connector.taskConfigs(3);

        // the expected assignments over 3 tasks:
        // t1 -> [t0p0, t0p3, t0p6, t1p1]
        // t2 -> [t0p1, t0p4, t0p7, t2p0]
        // t3 -> [t0p2, t0p5, t1p0, t2p1]

        Map<String, String> t1 = output.get(0);
        assertEquals("t0-0,t0-3,t0-6,t1-1", t1.get(TASK_TOPIC_PARTITIONS), "Config for t1 is incorrect");

        Map<String, String> t2 = output.get(1);
        assertEquals("t0-1,t0-4,t0-7,t2-0", t2.get(TASK_TOPIC_PARTITIONS), "Config for t2 is incorrect");

        Map<String, String> t3 = output.get(2);
        assertEquals("t0-2,t0-5,t1-0,t2-1", t3.get(TASK_TOPIC_PARTITIONS), "Config for t3 is incorrect");
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
        assertEquals(2, configMap.size(), "configMap has incorrect size");

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

    @Test
    public void testIsCycleWithNullUpstreamTopic() {
        class CustomReplicationPolicy extends DefaultReplicationPolicy {
            @Override
            public String upstreamTopic(String topic) {
                return null;
            }
        }
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
                new CustomReplicationPolicy(), new DefaultTopicFilter(), new DefaultConfigPropertyFilter());
        assertDoesNotThrow(() -> connector.isCycle(".b"));
    }

    @Test
    public void testExactlyOnceSupport() {
        String readCommitted = "read_committed";
        String readUncommitted = "read_uncommitted";
        String readGarbage = "read_garbage";

        // Connector is configured correctly, but exactly-once can't be supported
        assertExactlyOnceSupport(null, null, false);
        assertExactlyOnceSupport(readUncommitted, null, false);
        assertExactlyOnceSupport(null, readUncommitted, false);
        assertExactlyOnceSupport(readUncommitted, readUncommitted, false);

        // Connector is configured correctly, and exactly-once can be supported
        assertExactlyOnceSupport(readCommitted, null, true);
        assertExactlyOnceSupport(null, readCommitted, true);
        assertExactlyOnceSupport(readUncommitted, readCommitted, true);
        assertExactlyOnceSupport(readCommitted, readCommitted, true);

        // Connector is configured incorrectly, but is able to react gracefully
        assertExactlyOnceSupport(readGarbage, null, false);
        assertExactlyOnceSupport(null, readGarbage, false);
        assertExactlyOnceSupport(readGarbage, readGarbage, false);
        assertExactlyOnceSupport(readCommitted, readGarbage, false);
        assertExactlyOnceSupport(readUncommitted, readGarbage, false);
        assertExactlyOnceSupport(readGarbage, readUncommitted, false);
        assertExactlyOnceSupport(readGarbage, readCommitted, true);
    }

    private void assertExactlyOnceSupport(String defaultIsolationLevel, String sourceIsolationLevel, boolean expected) {
        Map<String, String> props = makeProps();
        if (defaultIsolationLevel != null) {
            props.put(CONSUMER_CLIENT_PREFIX + ISOLATION_LEVEL_CONFIG, defaultIsolationLevel);
        }
        if (sourceIsolationLevel != null) {
            props.put(SOURCE_PREFIX + CONSUMER_CLIENT_PREFIX + ISOLATION_LEVEL_CONFIG, sourceIsolationLevel);
        }
        ExactlyOnceSupport expectedSupport = expected ? ExactlyOnceSupport.SUPPORTED : ExactlyOnceSupport.UNSUPPORTED;
        ExactlyOnceSupport actualSupport = new MirrorSourceConnector().exactlyOnceSupport(props);
        assertEquals(expectedSupport, actualSupport);
    }

    @Test
    public void testExactlyOnceSupportValidation() {
        String exactlyOnceSupport = "exactly.once.support";

        Map<String, String> props = makeProps();
        Optional<ConfigValue> configValue = validateProperty(exactlyOnceSupport, props);
        assertEquals(Optional.empty(), configValue);

        props.put(exactlyOnceSupport, "requested");
        configValue = validateProperty(exactlyOnceSupport, props);
        assertEquals(Optional.empty(), configValue);

        props.put(exactlyOnceSupport, "garbage");
        configValue = validateProperty(exactlyOnceSupport, props);
        assertEquals(Optional.empty(), configValue);

        props.put(exactlyOnceSupport, "required");
        configValue = validateProperty(exactlyOnceSupport, props);
        assertTrue(configValue.isPresent());
        List<String> errorMessages = configValue.get().errorMessages();
        assertEquals(1, errorMessages.size());
        String errorMessage = errorMessages.get(0);
        assertTrue(
                errorMessages.get(0).contains(ISOLATION_LEVEL_CONFIG),
                "Error message \"" + errorMessage + "\" should have mentioned the 'isolation.level' consumer property"
        );

        props.put(CONSUMER_CLIENT_PREFIX + ISOLATION_LEVEL_CONFIG, "read_committed");
        configValue = validateProperty(exactlyOnceSupport, props);
        assertEquals(Optional.empty(), configValue);

        // Make sure that an unrelated invalid property doesn't cause an exception to be thrown and is instead handled and reported gracefully
        props.put(OFFSET_LAG_MAX, "bad");
        // Ensure that the issue with the invalid property is reported...
        configValue = validateProperty(OFFSET_LAG_MAX, props);
        assertTrue(configValue.isPresent());
        errorMessages = configValue.get().errorMessages();
        assertEquals(1, errorMessages.size());
        errorMessage = errorMessages.get(0);
        assertTrue(
                errorMessages.get(0).contains(OFFSET_LAG_MAX),
                "Error message \"" + errorMessage + "\" should have mentioned the 'offset.lag.max' property"
        );
        // ... and that it does not cause any issues with validation for exactly-once support...
        configValue = validateProperty(exactlyOnceSupport, props);
        assertEquals(Optional.empty(), configValue);

        // ... regardless of whether validation for exactly-once support does or does not find an error
        props.remove(CONSUMER_CLIENT_PREFIX + ISOLATION_LEVEL_CONFIG);
        configValue = validateProperty(exactlyOnceSupport, props);
        assertTrue(configValue.isPresent());
        errorMessages = configValue.get().errorMessages();
        assertEquals(1, errorMessages.size());
        errorMessage = errorMessages.get(0);
        assertTrue(
                errorMessages.get(0).contains(ISOLATION_LEVEL_CONFIG),
                "Error message \"" + errorMessage + "\" should have mentioned the 'isolation.level' consumer property"
        );
    }

    private Optional<ConfigValue> validateProperty(String name, Map<String, String> props) {
        List<ConfigValue> results = new MirrorSourceConnector().validate(props)
                .configValues().stream()
                .filter(cv -> name.equals(cv.name()))
                .collect(Collectors.toList());

        assertTrue(results.size() <= 1, "Connector produced multiple config values for '" + name + "' property");

        if (results.isEmpty())
            return Optional.empty();

        ConfigValue result = results.get(0);
        assertNotNull(result, "Connector should not have record null config value for '" + name + "' property");
        return Optional.of(result);
    }

    @Test
    public void testAlterOffsetsIncorrectPartitionKey() {
        MirrorSourceConnector connector = new MirrorSourceConnector();
        assertThrows(ConnectException.class, () -> connector.alterOffsets(null, Collections.singletonMap(
                Collections.singletonMap("unused_partition_key", "unused_partition_value"),
                MirrorUtils.wrapOffset(10)
        )));

        // null partitions are invalid
        assertThrows(ConnectException.class, () -> connector.alterOffsets(null, Collections.singletonMap(
                null,
                MirrorUtils.wrapOffset(10)
        )));
    }

    @Test
    public void testAlterOffsetsMissingPartitionKey() {
        MirrorSourceConnector connector = new MirrorSourceConnector();

        Function<Map<String, ?>, Boolean> alterOffsets = partition -> connector.alterOffsets(null, Collections.singletonMap(
                partition,
                MirrorUtils.wrapOffset(64)
        ));

        Map<String, ?> validPartition = sourcePartition("t", 3, "us-east-2");
        // Sanity check to make sure our valid partition is actually valid
        assertTrue(alterOffsets.apply(validPartition));

        for (String key : Arrays.asList(SOURCE_CLUSTER_KEY, TOPIC_KEY, PARTITION_KEY)) {
            Map<String, ?> invalidPartition = new HashMap<>(validPartition);
            invalidPartition.remove(key);
            assertThrows(ConnectException.class, () -> alterOffsets.apply(invalidPartition));
        }
    }

    @Test
    public void testAlterOffsetsInvalidPartitionPartition() {
        MirrorSourceConnector connector = new MirrorSourceConnector();
        Map<String, Object> partition = sourcePartition("t", 3, "us-west-2");
        partition.put(PARTITION_KEY, "a string");
        assertThrows(ConnectException.class, () -> connector.alterOffsets(null, Collections.singletonMap(
                partition,
                MirrorUtils.wrapOffset(49)
        )));
    }

    @Test
    public void testAlterOffsetsMultiplePartitions() {
        MirrorSourceConnector connector = new MirrorSourceConnector();

        Map<String, ?> partition1 = sourcePartition("t1", 0, "primary");
        Map<String, ?> partition2 = sourcePartition("t1", 1, "primary");

        Map<Map<String, ?>, Map<String, ?>> offsets = new HashMap<>();
        offsets.put(partition1, MirrorUtils.wrapOffset(50));
        offsets.put(partition2, MirrorUtils.wrapOffset(100));

        assertTrue(connector.alterOffsets(null, offsets));
    }

    @Test
    public void testAlterOffsetsIncorrectOffsetKey() {
        MirrorSourceConnector connector = new MirrorSourceConnector();

        Map<Map<String, ?>, Map<String, ?>> offsets = Collections.singletonMap(
                sourcePartition("t1", 2, "backup"),
                Collections.singletonMap("unused_offset_key", 0)
        );
        assertThrows(ConnectException.class, () -> connector.alterOffsets(null, offsets));
    }

    @Test
    public void testAlterOffsetsOffsetValues() {
        MirrorSourceConnector connector = new MirrorSourceConnector();

        Function<Object, Boolean> alterOffsets = offset -> connector.alterOffsets(null, Collections.singletonMap(
                sourcePartition("t", 5, "backup"),
                Collections.singletonMap(MirrorUtils.OFFSET_KEY, offset)
        ));

        assertThrows(ConnectException.class, () -> alterOffsets.apply("nan"));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(null));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(new Object()));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(3.14));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(-420));
        assertThrows(ConnectException.class, () -> alterOffsets.apply("-420"));
        assertThrows(ConnectException.class, () -> alterOffsets.apply("10"));
        assertTrue(() -> alterOffsets.apply(0));
        assertTrue(() -> alterOffsets.apply(10));
        assertTrue(() -> alterOffsets.apply(((long) Integer.MAX_VALUE) + 1));
    }

    @Test
    public void testSuccessfulAlterOffsets() {
        MirrorSourceConnector connector = new MirrorSourceConnector();

        Map<Map<String, ?>, Map<String, ?>> offsets = Collections.singletonMap(
                sourcePartition("t2", 0, "backup"),
                MirrorUtils.wrapOffset(5)
        );

        // Expect no exception to be thrown when a valid offsets map is passed. An empty offsets map is treated as valid
        // since it could indicate that the offsets were reset previously or that no offsets have been committed yet
        // (for a reset operation)
        assertTrue(connector.alterOffsets(null, offsets));
        assertTrue(connector.alterOffsets(null, Collections.emptyMap()));
    }

    @Test
    public void testAlterOffsetsTombstones() {
        MirrorCheckpointConnector connector = new MirrorCheckpointConnector();

        Function<Map<String, ?>, Boolean> alterOffsets = partition -> connector.alterOffsets(
                null,
                Collections.singletonMap(partition, null)
        );

        Map<String, Object> partition = sourcePartition("kips", 875, "apache.kafka");
        assertTrue(() -> alterOffsets.apply(partition));
        partition.put(PARTITION_KEY, "a string");
        assertTrue(() -> alterOffsets.apply(partition));
        partition.remove(PARTITION_KEY);
        assertTrue(() -> alterOffsets.apply(partition));

        assertTrue(() -> alterOffsets.apply(null));
        assertTrue(() -> alterOffsets.apply(Collections.emptyMap()));
        assertTrue(() -> alterOffsets.apply(Collections.singletonMap("unused_partition_key", "unused_partition_value")));
    }

    private static Map<String, Object> sourcePartition(String topic, int partition, String sourceClusterAlias) {
        return MirrorUtils.wrapPartition(
                new TopicPartition(topic, partition),
                sourceClusterAlias
        );
    }
}
