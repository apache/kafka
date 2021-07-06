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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;

import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MirrorConnectorConfigTest {

    @Test
    public void testTaskConfigTopicPartitions() {
        List<TopicPartition> topicPartitions = Arrays.asList(new TopicPartition("topic-1", 2),
            new TopicPartition("topic-3", 4), new TopicPartition("topic-5", 6));
        MirrorConnectorConfig config = new MirrorConnectorConfig(makeProps());
        Map<String, String> props = config.taskConfigForTopicPartitions(topicPartitions);
        MirrorTaskConfig taskConfig = new MirrorTaskConfig(props);
        assertEquals(taskConfig.taskTopicPartitions(), new HashSet<>(topicPartitions),
                "Setting topic property configuration failed");
    }

    @Test
    public void testTaskConfigConsumerGroups() {
        List<String> groups = Arrays.asList("consumer-1", "consumer-2", "consumer-3");
        MirrorConnectorConfig config = new MirrorConnectorConfig(makeProps());
        Map<String, String> props = config.taskConfigForConsumerGroups(groups);
        MirrorTaskConfig taskConfig = new MirrorTaskConfig(props);
        assertEquals(taskConfig.taskConsumerGroups(), new HashSet<>(groups),
                "Setting consumer groups property configuration failed");
    }

    @Test
    public void testTopicMatching() {
        MirrorConnectorConfig config = new MirrorConnectorConfig(makeProps("topics", "topic1"));
        assertTrue(config.topicFilter().shouldReplicateTopic("topic1"),
                "topic1 replication property configuration failed");
        assertFalse(config.topicFilter().shouldReplicateTopic("topic2"),
                "topic2 replication property configuration failed");
    }

    @Test
    public void testGroupMatching() {
        MirrorConnectorConfig config = new MirrorConnectorConfig(makeProps("groups", "group1"));
        assertTrue(config.groupFilter().shouldReplicateGroup("group1"),
                "topic1 group matching property configuration failed");
        assertFalse(config.groupFilter().shouldReplicateGroup("group2"),
                "topic2 group matching property configuration failed");
    }

    @Test
    public void testConfigPropertyMatching() {
        MirrorConnectorConfig config = new MirrorConnectorConfig(
            makeProps("config.properties.exclude", "prop2"));
        assertTrue(config.configPropertyFilter().shouldReplicateConfigProperty("prop1"),
                "config.properties.exclude incorrectly excluded prop1");
        assertFalse(config.configPropertyFilter().shouldReplicateConfigProperty("prop2"),
                "config.properties.exclude incorrectly included prop2");
    }

    @Test
    public void testConfigBackwardsCompatibility() {
        MirrorConnectorConfig config = new MirrorConnectorConfig(
            makeProps("config.properties.blacklist", "prop1",
                      "groups.blacklist", "group-1",
                      "topics.blacklist", "topic-1"));
        assertFalse(config.configPropertyFilter().shouldReplicateConfigProperty("prop1"));
        assertTrue(config.configPropertyFilter().shouldReplicateConfigProperty("prop2"));
        assertFalse(config.topicFilter().shouldReplicateTopic("topic-1"));
        assertTrue(config.topicFilter().shouldReplicateTopic("topic-2"));
        assertFalse(config.groupFilter().shouldReplicateGroup("group-1"));
        assertTrue(config.groupFilter().shouldReplicateGroup("group-2"));
    }

    @Test
    public void testNoTopics() {
        MirrorConnectorConfig config = new MirrorConnectorConfig(makeProps("topics", ""));
        assertFalse(config.topicFilter().shouldReplicateTopic("topic1"), "topic1 shouldn't exist");
        assertFalse(config.topicFilter().shouldReplicateTopic("topic2"), "topic2 shouldn't exist");
        assertFalse(config.topicFilter().shouldReplicateTopic(""), "Empty topic shouldn't exist");
    }

    @Test
    public void testAllTopics() {
        MirrorConnectorConfig config = new MirrorConnectorConfig(makeProps("topics", ".*"));
        assertTrue(config.topicFilter().shouldReplicateTopic("topic1"),
                "topic1 created from wildcard should exist");
        assertTrue(config.topicFilter().shouldReplicateTopic("topic2"),
                "topic2 created from wildcard should exist");
    }

    @Test
    public void testListOfTopics() {
        MirrorConnectorConfig config = new MirrorConnectorConfig(makeProps("topics", "topic1, topic2"));
        assertTrue(config.topicFilter().shouldReplicateTopic("topic1"), "topic1 created from list should exist");
        assertTrue(config.topicFilter().shouldReplicateTopic("topic2"), "topic2 created from list should exist");
        assertFalse(config.topicFilter().shouldReplicateTopic("topic3"), "topic3 created from list should exist");
    }

    @Test
    public void testNonMutationOfConfigDef() {
        Collection<String> taskSpecificProperties = Arrays.asList(
            MirrorConnectorConfig.TASK_TOPIC_PARTITIONS,
            MirrorConnectorConfig.TASK_CONSUMER_GROUPS
        );

        // Sanity check to make sure that these properties are actually defined for the task config,
        // and that the task config class has been loaded and statically initialized by the JVM
        ConfigDef taskConfigDef = MirrorTaskConfig.TASK_CONFIG_DEF;
        taskSpecificProperties.forEach(taskSpecificProperty -> assertTrue(
            taskConfigDef.names().contains(taskSpecificProperty),
            taskSpecificProperty + " should be defined for task ConfigDef"
        ));

        // Ensure that the task config class hasn't accidentally modified the connector config
        ConfigDef connectorConfigDef = MirrorConnectorConfig.CONNECTOR_CONFIG_DEF;
        taskSpecificProperties.forEach(taskSpecificProperty -> assertFalse(
            connectorConfigDef.names().contains(taskSpecificProperty),
            taskSpecificProperty + " should not be defined for connector ConfigDef"
        ));
    }

    @Test
    public void testSourceConsumerConfig() {
        Map<String, String> connectorProps = makeProps(
                MirrorConnectorConfig.CONSUMER_CLIENT_PREFIX + "max.poll.interval.ms", "120000"
        );
        MirrorConnectorConfig config = new MirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorConsumerProps = config.sourceConsumerConfig();
        Map<String, Object> expectedConsumerProps = new HashMap<>();
        expectedConsumerProps.put("enable.auto.commit", "false");
        expectedConsumerProps.put("auto.offset.reset", "earliest");
        expectedConsumerProps.put("max.poll.interval.ms", "120000");
        assertEquals(expectedConsumerProps, connectorConsumerProps);

        // checking auto.offset.reset override works
        connectorProps = makeProps(
                MirrorConnectorConfig.CONSUMER_CLIENT_PREFIX + "auto.offset.reset", "latest"
        );
        config = new MirrorConnectorConfig(connectorProps);
        connectorConsumerProps = config.sourceConsumerConfig();
        expectedConsumerProps.put("auto.offset.reset", "latest");
        expectedConsumerProps.remove("max.poll.interval.ms");
        assertEquals(expectedConsumerProps, connectorConsumerProps,
                MirrorConnectorConfig.CONSUMER_CLIENT_PREFIX + " source consumer config not matching");
    }

    @Test
    public void testSourceConsumerConfigWithSourcePrefix() {
        String prefix = MirrorConnectorConfig.SOURCE_PREFIX + MirrorConnectorConfig.CONSUMER_CLIENT_PREFIX;
        Map<String, String> connectorProps = makeProps(
                prefix + "auto.offset.reset", "latest",
                prefix + "max.poll.interval.ms", "100"
        );
        MirrorConnectorConfig config = new MirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorConsumerProps = config.sourceConsumerConfig();
        Map<String, Object> expectedConsumerProps = new HashMap<>();
        expectedConsumerProps.put("enable.auto.commit", "false");
        expectedConsumerProps.put("auto.offset.reset", "latest");
        expectedConsumerProps.put("max.poll.interval.ms", "100");
        assertEquals(expectedConsumerProps, connectorConsumerProps,
                prefix + " source consumer config not matching");
    }

    @Test
    public void testSourceProducerConfig() {
        Map<String, String> connectorProps = makeProps(
                MirrorConnectorConfig.PRODUCER_CLIENT_PREFIX + "acks", "1"
        );
        MirrorConnectorConfig config = new MirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorProducerProps = config.sourceProducerConfig();
        Map<String, Object> expectedProducerProps = new HashMap<>();
        expectedProducerProps.put("acks", "1");
        assertEquals(expectedProducerProps, connectorProducerProps,
                MirrorConnectorConfig.PRODUCER_CLIENT_PREFIX  + " source product config not matching");
    }

    @Test
    public void testSourceProducerConfigWithSourcePrefix() {
        String prefix = MirrorConnectorConfig.SOURCE_PREFIX + MirrorConnectorConfig.PRODUCER_CLIENT_PREFIX;
        Map<String, String> connectorProps = makeProps(prefix + "acks", "1");
        MirrorConnectorConfig config = new MirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorProducerProps = config.sourceProducerConfig();
        Map<String, Object> expectedProducerProps = new HashMap<>();
        expectedProducerProps.put("acks", "1");
        assertEquals(expectedProducerProps, connectorProducerProps,
                prefix + " source producer config not matching");
    }

    @Test
    public void testSourceAdminConfig() {
        Map<String, String> connectorProps = makeProps(
                MirrorConnectorConfig.ADMIN_CLIENT_PREFIX +
                        "connections.max.idle.ms", "10000"
        );
        MirrorConnectorConfig config = new MirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorAdminProps = config.sourceAdminConfig();
        Map<String, Object> expectedAdminProps = new HashMap<>();
        expectedAdminProps.put("connections.max.idle.ms", "10000");
        assertEquals(expectedAdminProps, connectorAdminProps,
                MirrorConnectorConfig.ADMIN_CLIENT_PREFIX + " source connector admin props not matching");
    }

    @Test
    public void testSourceAdminConfigWithSourcePrefix() {
        String prefix = MirrorConnectorConfig.SOURCE_PREFIX + MirrorConnectorConfig.ADMIN_CLIENT_PREFIX;
        Map<String, String> connectorProps = makeProps(prefix + "connections.max.idle.ms", "10000");
        MirrorConnectorConfig config = new MirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorAdminProps = config.sourceAdminConfig();
        Map<String, Object> expectedAdminProps = new HashMap<>();
        expectedAdminProps.put("connections.max.idle.ms", "10000");
        assertEquals(expectedAdminProps, connectorAdminProps, prefix + " source connector admin props not matching");
    }

    @Test
    public void testTargetAdminConfig() {
        Map<String, String> connectorProps = makeProps(
                MirrorConnectorConfig.ADMIN_CLIENT_PREFIX +
                        "connections.max.idle.ms", "10000"
        );
        MirrorConnectorConfig config = new MirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorAdminProps = config.targetAdminConfig();
        Map<String, Object> expectedAdminProps = new HashMap<>();
        expectedAdminProps.put("connections.max.idle.ms", "10000");
        assertEquals(expectedAdminProps, connectorAdminProps,
                MirrorConnectorConfig.ADMIN_CLIENT_PREFIX + " target connector admin props not matching");
    }

    @Test
    public void testTargetAdminConfigWithSourcePrefix() {
        String prefix = MirrorConnectorConfig.TARGET_PREFIX + MirrorConnectorConfig.ADMIN_CLIENT_PREFIX;
        Map<String, String> connectorProps = makeProps(prefix + "connections.max.idle.ms", "10000");
        MirrorConnectorConfig config = new MirrorConnectorConfig(connectorProps);
        Map<String, Object> connectorAdminProps = config.targetAdminConfig();
        Map<String, Object> expectedAdminProps = new HashMap<>();
        expectedAdminProps.put("connections.max.idle.ms", "10000");
        assertEquals(expectedAdminProps, connectorAdminProps, prefix + " source connector admin props not matching");
    }

    @Test
    public void testOffsetSyncsTopic() {
        // Invalid location
        Map<String, String> connectorProps = makeProps("offset-syncs.topic.location", "something");
        assertThrows(ConfigException.class, () -> new MirrorConnectorConfig(connectorProps));

        connectorProps.put("offset-syncs.topic.location", "source");
        MirrorConnectorConfig config = new MirrorConnectorConfig(connectorProps);
        assertEquals("mm2-offset-syncs.target2.internal", config.offsetSyncsTopic());
        connectorProps.put("offset-syncs.topic.location", "target");
        config = new MirrorConnectorConfig(connectorProps);
        assertEquals("mm2-offset-syncs.source1.internal", config.offsetSyncsTopic());
        // Default to source
        connectorProps.remove("offset-syncs.topic.location");
        config = new MirrorConnectorConfig(connectorProps);
        assertEquals("mm2-offset-syncs.target2.internal", config.offsetSyncsTopic());
    }

    @Test
    public void testConsumerConfigsForOffsetSyncsTopic() {
        Map<String, String> connectorProps = makeProps(
                "source.consumer.max.partition.fetch.bytes", "1",
                "target.consumer.heartbeat.interval.ms", "1",
                "consumer.max.poll.interval.ms", "1",
                "fetch.min.bytes", "1"
        );
        MirrorConnectorConfig config = new MirrorConnectorConfig(connectorProps);
        assertEquals(config.sourceConsumerConfig(), config.offsetSyncsTopicConsumerConfig());
        connectorProps.put("offset-syncs.topic.location", "target");
        config = new MirrorConnectorConfig(connectorProps);
        assertEquals(config.targetConsumerConfig(), config.offsetSyncsTopicConsumerConfig());
    }

    @Test
    public void testProducerConfigsForOffsetSyncsTopic() {
        Map<String, String> connectorProps = makeProps(
                "source.producer.batch.size", "1",
                "target.producer.acks", "1",
                "producer.max.poll.interval.ms", "1",
                "fetch.min.bytes", "1"
        );
        MirrorConnectorConfig config = new MirrorConnectorConfig(connectorProps);
        assertEquals(config.sourceProducerConfig(), config.offsetSyncsTopicProducerConfig());
        connectorProps.put("offset-syncs.topic.location", "target");
        config = new MirrorConnectorConfig(connectorProps);
        assertEquals(config.targetProducerConfig(), config.offsetSyncsTopicProducerConfig());
    }

    @Test
    public void testAdminConfigsForOffsetSyncsTopic() {
        Map<String, String> connectorProps = makeProps(
                "source.admin.request.timeout.ms", "1",
                "target.admin.send.buffer.bytes", "1",
                "admin.reconnect.backoff.max.ms", "1",
                "retries", "123"
        );
        MirrorConnectorConfig config = new MirrorConnectorConfig(connectorProps);
        assertEquals(config.sourceAdminConfig(), config.offsetSyncsTopicAdminConfig());
        connectorProps.put("offset-syncs.topic.location", "target");
        config = new MirrorConnectorConfig(connectorProps);
        assertEquals(config.targetAdminConfig(), config.offsetSyncsTopicAdminConfig());
    }

}
