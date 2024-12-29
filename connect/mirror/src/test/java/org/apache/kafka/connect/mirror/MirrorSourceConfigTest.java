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
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.mirror.TestUtils.assertEqualsExceptClientId;
import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MirrorSourceConfigTest {

    @Test
    public void testTaskConfigTopicPartitions() {
        List<TopicPartition> topicPartitions = Arrays.asList(new TopicPartition("topic-1", 2),
                new TopicPartition("topic-3", 4), new TopicPartition("topic-5", 6));
        MirrorSourceConfig config = new MirrorSourceConfig(makeProps());
        Map<String, String> props = config.taskConfigForTopicPartitions(topicPartitions, 1);
        MirrorSourceTaskConfig taskConfig = new MirrorSourceTaskConfig(props);
        assertEquals(taskConfig.taskTopicPartitions(), new HashSet<>(topicPartitions),
                "Setting topic property configuration failed");
    }

    @Test
    public void testTopicMatching() {
        MirrorSourceConfig config = new MirrorSourceConfig(makeProps("topics", "topic1"));
        assertTrue(config.topicFilter().shouldReplicateTopic("topic1"),
                "topic1 replication property configuration failed");
        assertFalse(config.topicFilter().shouldReplicateTopic("topic2"),
                "topic2 replication property configuration failed");
    }

    @Test
    public void testConfigPropertyMatching() {
        MirrorSourceConfig config = new MirrorSourceConfig(
                makeProps("config.properties.exclude", "prop2"));
        assertTrue(config.configPropertyFilter().shouldReplicateConfigProperty("prop1"),
                "config.properties.exclude incorrectly excluded prop1");
        assertFalse(config.configPropertyFilter().shouldReplicateConfigProperty("prop2"),
                "config.properties.exclude incorrectly included prop2");
    }

    @Test
    public void testNoTopics() {
        MirrorSourceConfig config = new MirrorSourceConfig(makeProps("topics", ""));
        assertFalse(config.topicFilter().shouldReplicateTopic("topic1"), "topic1 shouldn't exist");
        assertFalse(config.topicFilter().shouldReplicateTopic("topic2"), "topic2 shouldn't exist");
        assertFalse(config.topicFilter().shouldReplicateTopic(""), "Empty topic shouldn't exist");
    }

    @Test
    public void testAllTopics() {
        MirrorSourceConfig config = new MirrorSourceConfig(makeProps("topics", ".*"));
        assertTrue(config.topicFilter().shouldReplicateTopic("topic1"),
                "topic1 created from wildcard should exist");
        assertTrue(config.topicFilter().shouldReplicateTopic("topic2"),
                "topic2 created from wildcard should exist");
    }

    @Test
    public void testListOfTopics() {
        MirrorSourceConfig config = new MirrorSourceConfig(makeProps("topics", "topic1, topic2"));
        assertTrue(config.topicFilter().shouldReplicateTopic("topic1"), "topic1 created from list should exist");
        assertTrue(config.topicFilter().shouldReplicateTopic("topic2"), "topic2 created from list should exist");
        assertFalse(config.topicFilter().shouldReplicateTopic("topic3"), "topic3 created from list should exist");
    }

    @Test
    public void testNonMutationOfConfigDef() {
        // Sanity check to make sure that these properties are actually defined for the task config,
        // and that the task config class has been loaded and statically initialized by the JVM
        ConfigDef taskConfigDef = MirrorSourceTaskConfig.TASK_CONFIG_DEF;
        assertTrue(
                taskConfigDef.names().contains(MirrorSourceConfig.TASK_TOPIC_PARTITIONS),
                MirrorSourceConfig.TASK_TOPIC_PARTITIONS + " should be defined for task ConfigDef"
        );

        // Ensure that the task config class hasn't accidentally modified the connector config
        assertFalse(
                MirrorSourceConfig.CONNECTOR_CONFIG_DEF.names().contains(MirrorSourceConfig.TASK_TOPIC_PARTITIONS),
                MirrorSourceConfig.TASK_TOPIC_PARTITIONS + " should not be defined for connector ConfigDef"
        );
    }

    @Test
    public void testOffsetSyncsTopic() {
        // Invalid location
        Map<String, String> connectorProps = makeProps("offset-syncs.topic.location", "something");
        assertThrows(ConfigException.class, () -> new MirrorSourceConfig(connectorProps));

        connectorProps.put("offset-syncs.topic.location", "source");
        MirrorSourceConfig config = new MirrorSourceConfig(connectorProps);
        assertEquals("mm2-offset-syncs.target2.internal", config.offsetSyncsTopic());
        connectorProps.put("offset-syncs.topic.location", "target");
        config = new MirrorSourceConfig(connectorProps);
        assertEquals("mm2-offset-syncs.source1.internal", config.offsetSyncsTopic());
        // Default to source
        connectorProps.remove("offset-syncs.topic.location");
        config = new MirrorSourceConfig(connectorProps);
        assertEquals("mm2-offset-syncs.target2.internal", config.offsetSyncsTopic());
    }

    @Test
    public void testProducerConfigsForOffsetSyncsTopic() {
        Map<String, String> connectorProps = makeProps(
                "source.producer.batch.size", "1",
                "target.producer.acks", "1",
                "producer.max.poll.interval.ms", "1",
                "fetch.min.bytes", "1"
        );
        MirrorSourceConfig config = new MirrorSourceConfig(connectorProps);
        Map<String, Object> sourceProducerConfig = config.sourceProducerConfig("test");
        Map<String, Object> offsetSyncsTopicSourceProducerConfig = config.offsetSyncsTopicProducerConfig();
        assertEqualsExceptClientId(sourceProducerConfig, offsetSyncsTopicSourceProducerConfig);
        assertEquals("source1->target2|ConnectorName|test", sourceProducerConfig.get("client.id"));
        assertEquals("source1->target2|ConnectorName|" + MirrorSourceConfig.OFFSET_SYNCS_SOURCE_PRODUCER_ROLE,
                offsetSyncsTopicSourceProducerConfig.get("client.id"));
        connectorProps.put("offset-syncs.topic.location", "target");
        config = new MirrorSourceConfig(connectorProps);
        Map<String, Object> targetProducerConfig = config.targetProducerConfig("test");
        Map<String, Object> offsetSyncsTopicTargetProducerConfig = config.offsetSyncsTopicProducerConfig();
        assertEqualsExceptClientId(targetProducerConfig, offsetSyncsTopicTargetProducerConfig);
        assertEquals("source1->target2|ConnectorName|test", targetProducerConfig.get("client.id"));
        assertEquals("source1->target2|ConnectorName|" + MirrorSourceConfig.OFFSET_SYNCS_TARGET_PRODUCER_ROLE,
                offsetSyncsTopicTargetProducerConfig.get("client.id"));
    }

    @Test
    public void testAdminConfigsForOffsetSyncsTopic() {
        Map<String, String> connectorProps = makeProps(
                "source.admin.request.timeout.ms", "1",
                "target.admin.send.buffer.bytes", "1",
                "admin.reconnect.backoff.max.ms", "1",
                "retries", "123"
        );
        MirrorSourceConfig config = new MirrorSourceConfig(connectorProps);
        Map<String, Object> sourceAdminConfig = config.sourceAdminConfig("test");
        Map<String, Object> offsetSyncsTopicSourceAdminConfig = config.offsetSyncsTopicAdminConfig();
        assertEqualsExceptClientId(sourceAdminConfig, offsetSyncsTopicSourceAdminConfig);
        assertEquals("source1->target2|ConnectorName|test", sourceAdminConfig.get("client.id"));
        assertEquals("source1->target2|ConnectorName|" + MirrorSourceConfig.OFFSET_SYNCS_SOURCE_ADMIN_ROLE,
                offsetSyncsTopicSourceAdminConfig.get("client.id"));
        connectorProps.put("offset-syncs.topic.location", "target");
        config = new MirrorSourceConfig(connectorProps);
        Map<String, Object> targetAdminConfig = config.targetAdminConfig("test");
        Map<String, Object> offsetSyncsTopicTargetAdminConfig = config.offsetSyncsTopicAdminConfig();
        assertEqualsExceptClientId(targetAdminConfig, offsetSyncsTopicTargetAdminConfig);
        assertEquals("source1->target2|ConnectorName|test", targetAdminConfig.get("client.id"));
        assertEquals("source1->target2|ConnectorName|" + MirrorSourceConfig.OFFSET_SYNCS_TARGET_ADMIN_ROLE,
                offsetSyncsTopicTargetAdminConfig.get("client.id"));
    }
}
