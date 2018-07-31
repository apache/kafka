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
package org.apache.kafka.connect.kafka;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectorContext;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.runner.RunWith;

import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.api.easymock.PowerMock;
import org.easymock.EasyMockSupport;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaSourceConnector.class, PartitionMonitor.class})
@PowerMockIgnore("javax.management.*")
public class KafkaSourceConnectorTest extends EasyMockSupport {

    private PartitionMonitor partitionMonitorMock;
    private ConnectorContext connectorContextMock;

    private Set<LeaderTopicPartition> stubLeaderTopicPartitions;

    private KafkaSourceConnector connector;

    private Map<String, String> sourceProperties;

    private static final String SOURCE_TOPICS_VALUE = "test.topic";
    private static final String SOURCE_BOOTSTRAP_SERVERS_CONFIG = "localhost:6000";
    private static final String POLL_LOOP_TIMEOUT_MS_VALUE = "2000";
    private static final String TOPIC_LIST_TIMEOUT_MS_VALUE = "5000";
    private static final String CONSUMER_GROUP_ID_VALUE = "test-consumer-group";

    @Before
    public void setUp() throws Exception {
        connector = new KafkaSourceConnector();
        connectorContextMock = PowerMock.createMock(ConnectorContext.class);
        partitionMonitorMock = PowerMock.createMock(PartitionMonitor.class);
        connector.initialize(connectorContextMock);

        // Default test settings
        sourceProperties = new HashMap<>();
        sourceProperties.put(KafkaSourceConnectorConfig.SOURCE_TOPIC_WHITELIST_CONFIG, SOURCE_TOPICS_VALUE);
        sourceProperties.put(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG, SOURCE_BOOTSTRAP_SERVERS_CONFIG);
        sourceProperties.put(KafkaSourceConnectorConfig.POLL_LOOP_TIMEOUT_MS_CONFIG, POLL_LOOP_TIMEOUT_MS_VALUE);
        sourceProperties.put(KafkaSourceConnectorConfig.TOPIC_LIST_TIMEOUT_MS_CONFIG, TOPIC_LIST_TIMEOUT_MS_VALUE);
        sourceProperties.put(KafkaSourceConnectorConfig.CONSUMER_GROUP_ID_CONFIG, CONSUMER_GROUP_ID_VALUE);

        // Default leader topic partitions to return (just one)
        stubLeaderTopicPartitions = new HashSet<>();
        LeaderTopicPartition leaderTopicPartition = new LeaderTopicPartition(0, SOURCE_TOPICS_VALUE, 0);
        stubLeaderTopicPartitions.add(leaderTopicPartition);
    }

    @After
    public void tearDown() {
    }


    @Test(expected = ConfigException.class)
    public void testStartMissingBootstrapServers() {
        suppress(method(PartitionMonitor.class, "start"));
        PowerMock.replayAll();

        sourceProperties.remove(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG);
        connector.start(sourceProperties);

        PowerMock.verifyAll();
    }

    @Test(expected = ConfigException.class)
    public void testStartBlankBootstrapServers() {
        suppress(method(PartitionMonitor.class, "start"));
        PowerMock.replayAll();

        sourceProperties.put(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG, "");
        connector.start(sourceProperties);

        PowerMock.verifyAll();
    }

    @Test(expected = ConfigException.class)
    public void testStartTopicWhitelistMissing() {
        suppress(method(PartitionMonitor.class, "start"));
        replayAll();

        sourceProperties.remove(KafkaSourceConnectorConfig.SOURCE_TOPIC_WHITELIST_CONFIG);
        connector.start(sourceProperties);

        PowerMock.verifyAll();
    }

    @Test
    public void testStartCorrectConfig() throws Exception {
        PowerMock.expectNew(
                PartitionMonitor.class,
                new Class<?>[] {ConnectorContext.class, KafkaSourceConnectorConfig.class},
                EasyMock.anyObject(ConnectorContext.class),
                EasyMock.anyObject(KafkaSourceConnectorConfig.class)
        ).andStubReturn(partitionMonitorMock);
        partitionMonitorMock.start();
        PowerMock.expectLastCall().andVoid();
        PowerMock.replayAll();

        connector.start(sourceProperties);

        verifyAll();
    }

    @Test
    public void testTaskConfigsReturns1TaskOnOneTopicPartition() throws Exception {
        PowerMock.expectNew(
                PartitionMonitor.class,
                new Class<?>[] {ConnectorContext.class, KafkaSourceConnectorConfig.class},
                EasyMock.anyObject(ConnectorContext.class),
                EasyMock.anyObject(KafkaSourceConnectorConfig.class)
        ).andStubReturn(partitionMonitorMock);
        partitionMonitorMock.start();
        PowerMock.expectLastCall().andVoid();
        EasyMock.expect(partitionMonitorMock.getCurrentLeaderTopicPartitions()).andReturn(stubLeaderTopicPartitions);
        PowerMock.replayAll();

        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);

        assertEquals(1, taskConfigs.size());
        assertEquals("0:test.topic:0",
                taskConfigs.get(0).get("task.leader.topic.partitions"));
        assertEquals(SOURCE_TOPICS_VALUE,
                taskConfigs.get(0).get(KafkaSourceConnectorConfig.SOURCE_TOPIC_WHITELIST_CONFIG));
        assertEquals(SOURCE_BOOTSTRAP_SERVERS_CONFIG,
                taskConfigs.get(0).get(KafkaSourceConnectorConfig.SOURCE_BOOTSTRAP_SERVERS_CONFIG));

        verifyAll();
    }

    @Test
    public void testTaskConfigsReturns1TaskOnTwoTopicPartitions() throws Exception {
        // Default leader topic partitions to return (just one)
        stubLeaderTopicPartitions = new HashSet<>();
        LeaderTopicPartition leaderTopicPartition1 = new LeaderTopicPartition(0, SOURCE_TOPICS_VALUE, 0);
        stubLeaderTopicPartitions.add(leaderTopicPartition1);
        LeaderTopicPartition leaderTopicPartition2 = new LeaderTopicPartition(0, SOURCE_TOPICS_VALUE, 1);
        stubLeaderTopicPartitions.add(leaderTopicPartition2);

        PowerMock.expectNew(
                PartitionMonitor.class,
                new Class<?>[] {ConnectorContext.class, KafkaSourceConnectorConfig.class},
                EasyMock.anyObject(ConnectorContext.class),
                EasyMock.anyObject(KafkaSourceConnectorConfig.class)
        ).andStubReturn(partitionMonitorMock);

        partitionMonitorMock.start();
        PowerMock.expectLastCall().andVoid();
        EasyMock.expect(partitionMonitorMock.getCurrentLeaderTopicPartitions()).andReturn(stubLeaderTopicPartitions);
        PowerMock.replayAll();

        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

        assertEquals(1, taskConfigs.size());

        PowerMock.verifyAll();
    }

    @Test
    public void testTaskConfigsReturns2TasksOnTwoTopicPartitions() throws Exception {
        // Default leader topic partitions to return (just one)
        stubLeaderTopicPartitions = new HashSet<>();
        LeaderTopicPartition leaderTopicPartition1 = new LeaderTopicPartition(0, SOURCE_TOPICS_VALUE, 0);
        stubLeaderTopicPartitions.add(leaderTopicPartition1);
        LeaderTopicPartition leaderTopicPartition2 = new LeaderTopicPartition(0, SOURCE_TOPICS_VALUE, 1);
        stubLeaderTopicPartitions.add(leaderTopicPartition2);

        PowerMock.expectNew(
                PartitionMonitor.class,
                new Class<?>[] {ConnectorContext.class, KafkaSourceConnectorConfig.class},
                EasyMock.anyObject(ConnectorContext.class),
                EasyMock.anyObject(KafkaSourceConnectorConfig.class)
        ).andStubReturn(partitionMonitorMock);

        partitionMonitorMock.start();
        PowerMock.expectLastCall().andVoid();
        EasyMock.expect(partitionMonitorMock.getCurrentLeaderTopicPartitions()).andReturn(stubLeaderTopicPartitions);
        PowerMock.replayAll();

        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);

        assertEquals(2, taskConfigs.size());

        PowerMock.verifyAll();
    }


    @Test
    public void testTaskClass() {
        replayAll();

        assertEquals(KafkaSourceTask.class, connector.taskClass());

        verifyAll();
    }


}
