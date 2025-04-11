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

import org.apache.kafka.common.config.ConfigDef;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.connect.mirror.TestUtils.assertEqualsExceptClientId;
import static org.apache.kafka.connect.mirror.TestUtils.makeProps;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MirrorCheckpointConfigTest {

    @Test
    public void testTaskConfigConsumerGroups() {
        List<String> groups = Arrays.asList("consumer-1", "consumer-2", "consumer-3");
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps());
        Map<String, String> props = config.taskConfigForConsumerGroups(groups, 1);
        MirrorCheckpointTaskConfig taskConfig = new MirrorCheckpointTaskConfig(props);
        assertEquals(taskConfig.taskConsumerGroups(), new HashSet<>(groups),
                "Setting consumer groups property configuration failed");
    }

    @Test
    public void testGroupMatching() {
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(makeProps("groups", "group1"));
        assertTrue(config.groupFilter().shouldReplicateGroup("group1"),
                "topic1 group matching property configuration failed");
        assertFalse(config.groupFilter().shouldReplicateGroup("group2"),
                "topic2 group matching property configuration failed");
    }

    @Test
    public void testNonMutationOfConfigDef() {
        // Sanity check to make sure that these properties are actually defined for the task config,
        // and that the task config class has been loaded and statically initialized by the JVM
        ConfigDef taskConfigDef = MirrorCheckpointTaskConfig.TASK_CONFIG_DEF;
        assertTrue(
                taskConfigDef.names().contains(MirrorCheckpointConfig.TASK_CONSUMER_GROUPS),
                MirrorCheckpointConfig.TASK_CONSUMER_GROUPS + " should be defined for task ConfigDef"
        );

        // Ensure that the task config class hasn't accidentally modified the connector config
        assertFalse(
                MirrorCheckpointConfig.CONNECTOR_CONFIG_DEF.names().contains(MirrorCheckpointConfig.TASK_CONSUMER_GROUPS),
                MirrorCheckpointConfig.TASK_CONSUMER_GROUPS + " should not be defined for connector ConfigDef"
        );
    }

    @Test
    public void testConsumerConfigsForOffsetSyncsTopic() {
        Map<String, String> connectorProps = makeProps(
                "source.consumer.max.partition.fetch.bytes", "1",
                "target.consumer.heartbeat.interval.ms", "1",
                "consumer.max.poll.interval.ms", "1",
                "fetch.min.bytes", "1"
        );
        MirrorCheckpointConfig config = new MirrorCheckpointConfig(connectorProps);
        Map<String, Object> sourceConsumerConfig = config.sourceConsumerConfig("test");
        Map<String, Object> offsetSyncsTopicSourceConsumerConfig = config.offsetSyncsTopicConsumerConfig();
        assertEqualsExceptClientId(sourceConsumerConfig, offsetSyncsTopicSourceConsumerConfig);
        assertEquals("source1->target2|ConnectorName|test", sourceConsumerConfig.get("client.id"));
        assertEquals(
                "source1->target2|ConnectorName|" + MirrorCheckpointConfig.OFFSET_SYNCS_SOURCE_CONSUMER_ROLE,
                offsetSyncsTopicSourceConsumerConfig.get("client.id"));
        connectorProps.put("offset-syncs.topic.location", "target");
        config = new MirrorCheckpointConfig(connectorProps);
        Map<String, Object> targetConsumerConfig = config.targetConsumerConfig("test");
        Map<String, Object> offsetSyncsTopicTargetConsumerConfig = config.offsetSyncsTopicConsumerConfig();
        assertEqualsExceptClientId(targetConsumerConfig, offsetSyncsTopicTargetConsumerConfig);
        assertEquals("source1->target2|ConnectorName|test", targetConsumerConfig.get("client.id"));
        assertEquals(
                "source1->target2|ConnectorName|" + MirrorCheckpointConfig.OFFSET_SYNCS_TARGET_CONSUMER_ROLE,
                offsetSyncsTopicTargetConsumerConfig.get("client.id"));
    }

    @Test
    public void testSkipValidationIfConnectorDisabled() {
        Map<String, String> configValues = MirrorCheckpointConfig.validate(makeProps(
                MirrorConnectorConfig.ENABLED, "false",
                MirrorCheckpointConfig.EMIT_CHECKPOINTS_ENABLED, "false",
                MirrorCheckpointConfig.SYNC_GROUP_OFFSETS_ENABLED, "false"));
        assertTrue(configValues.isEmpty());

        configValues = MirrorCheckpointConfig.validate(makeProps(
                MirrorConnectorConfig.ENABLED, "false",
                MirrorCheckpointConfig.EMIT_CHECKPOINTS_ENABLED, "true",
                MirrorCheckpointConfig.EMIT_OFFSET_SYNCS_ENABLED, "false"));
        assertTrue(configValues.isEmpty());
    }

    @Test
    public void testValidateIfConnectorEnabled() {
        Map<String, String> configValues = MirrorCheckpointConfig.validate(makeProps(
                MirrorCheckpointConfig.EMIT_CHECKPOINTS_ENABLED, "false",
                MirrorCheckpointConfig.SYNC_GROUP_OFFSETS_ENABLED, "false"));
        assertEquals(configValues.keySet(), Collections.singleton(MirrorCheckpointConfig.EMIT_CHECKPOINTS_ENABLED));

        configValues = MirrorCheckpointConfig.validate(makeProps(MirrorCheckpointConfig.EMIT_CHECKPOINTS_ENABLED, "true",
                MirrorCheckpointConfig.EMIT_OFFSET_SYNCS_ENABLED, "false"));
        assertEquals(configValues.keySet(), Set.of(MirrorCheckpointConfig.EMIT_OFFSET_SYNCS_ENABLED));

        configValues = MirrorCheckpointConfig.validate(makeProps(MirrorCheckpointConfig.EMIT_CHECKPOINTS_ENABLED, "true",
                MirrorCheckpointConfig.EMIT_CHECKPOINTS_ENABLED, "true",
                MirrorCheckpointConfig.EMIT_OFFSET_SYNCS_ENABLED, "true"));
        assertTrue(configValues.isEmpty());
    }
}
