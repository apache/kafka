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
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.api.streams.assignor.TopologyDescriber;
import org.apache.kafka.raft.KRaftConfigs;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class AbstractKafkaConfigTest {

    private static final String TEST_INTERNAL_GROUP_CONFIG = "group.test.internal.config";
    private static final String TEST_INTERNAL_GROUP_CONFIG_BROKER_SYNONYM = "group.test.internal.config.broker.synonym";

    @Test
    public void testPopulateSynonymsOnEmptyMap() {
        assertEquals(Collections.emptyMap(), AbstractKafkaConfig.populateSynonyms(Collections.emptyMap()));
    }

    @Test
    public void testPopulateSynonymsOnMapWithoutNodeId() {
        Map<String, String> input = new HashMap<>();
        input.put(ServerConfigs.BROKER_ID_CONFIG, "4");
        Map<String, String> expectedOutput = new HashMap<>();
        expectedOutput.put(ServerConfigs.BROKER_ID_CONFIG, "4");
        expectedOutput.put(KRaftConfigs.NODE_ID_CONFIG, "4");
        assertEquals(expectedOutput, AbstractKafkaConfig.populateSynonyms(input));
    }

    @Test
    public void testPopulateSynonymsOnMapWithoutBrokerId() {
        Map<String, String> input = new HashMap<>();
        input.put(KRaftConfigs.NODE_ID_CONFIG, "4");
        Map<String, String> expectedOutput = new HashMap<>();
        expectedOutput.put(ServerConfigs.BROKER_ID_CONFIG, "4");
        expectedOutput.put(KRaftConfigs.NODE_ID_CONFIG, "4");
        assertEquals(expectedOutput, AbstractKafkaConfig.populateSynonyms(input));
    }

    @Test
    public void testExtractGroupConfigMapExcludesInternalConfigWithUnconfiguredBrokerSynonym() {
        Map<String, Object> config = extractGroupConfigMap(Map.of(), true);

        assertFalse(config.containsKey(TEST_INTERNAL_GROUP_CONFIG));
    }

    @Test
    public void testExtractGroupConfigMapIncludesInternalConfigWithConfiguredBrokerSynonym() {
        Map<String, Object> config = extractGroupConfigMap(Map.of(TEST_INTERNAL_GROUP_CONFIG_BROKER_SYNONYM, "override-value"), true);

        assertTrue(config.containsKey(TEST_INTERNAL_GROUP_CONFIG));
        assertEquals("override-value", config.get(TEST_INTERNAL_GROUP_CONFIG));
    }

    @Test
    public void testExtractGroupConfigMapIncludesNonInternalConfig() {
        Map<String, Object> config = extractGroupConfigMap(Map.of(), false);

        assertTrue(config.containsKey(TEST_INTERNAL_GROUP_CONFIG));
        assertEquals("default-value", config.get(TEST_INTERNAL_GROUP_CONFIG));
    }

    @Test
    public void testExtractGroupConfigMapReturnsStreamsAssignorShortName() {
        try (MockedStatic<GroupConfig> mocked = mockStatic(GroupConfig.class, Mockito.CALLS_REAL_METHODS)) {
            mocked.when(GroupConfig::configNames).thenReturn(Set.of(GroupConfig.STREAMS_ASSIGNOR_NAME_CONFIG));

            // The broker synonym of the group config is read from the broker config, so it must be defined.
            AbstractKafkaConfig kafkaConfig =
                new AbstractKafkaConfig(GroupCoordinatorConfig.CONFIG_DEF, Map.of(), Map.of(), false) { };

            // The group config holds a single assignor name, so the default is the first entry of the
            // broker's list rather than the whole list.
            assertEquals("sticky",
                kafkaConfig.extractGroupConfigMap(streamsAssignors("sticky," + CustomTaskAssignor.class.getName()))
                    .get(GroupConfig.STREAMS_ASSIGNOR_NAME_CONFIG));

            // A custom assignor is configured by class name, but a group selects it by name(), so the
            // default must be the name and not the class name.
            assertEquals("CustomTaskAssignor",
                kafkaConfig.extractGroupConfigMap(streamsAssignors(CustomTaskAssignor.class.getName() + ",sticky"))
                    .get(GroupConfig.STREAMS_ASSIGNOR_NAME_CONFIG));
        }
    }

    private static GroupCoordinatorConfig streamsAssignors(String assignors) {
        return new GroupCoordinatorConfig(new AbstractConfig(
            GroupCoordinatorConfig.CONFIG_DEF,
            Map.of(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNORS_CONFIG, assignors),
            false
        ));
    }

    public static class CustomTaskAssignor implements TaskAssignor {
        @Override
        public String name() {
            return "CustomTaskAssignor";
        }

        @Override
        public GroupAssignment assign(GroupSpec groupSpec, TopologyDescriber topologyDescriber) {
            return null;
        }
    }

    private static Map<String, Object> extractGroupConfigMap(Map<String, Object> brokerProps, boolean isInternal) {
        try (MockedStatic<GroupConfig> mocked = mockStatic(GroupConfig.class, Mockito.CALLS_REAL_METHODS)) {

            // mock group config
            mocked.when(GroupConfig::configNames).thenReturn(Set.of(TEST_INTERNAL_GROUP_CONFIG));
            mocked.when(() -> GroupConfig.brokerSynonym(TEST_INTERNAL_GROUP_CONFIG)).thenReturn(Optional.of(TEST_INTERNAL_GROUP_CONFIG_BROKER_SYNONYM));
            mocked.when(() -> GroupConfig.isInternal(TEST_INTERNAL_GROUP_CONFIG)).thenReturn(isInternal);

            // mock broker synonym config
            ConfigDef configDef = new ConfigDef().define(TEST_INTERNAL_GROUP_CONFIG_BROKER_SYNONYM, ConfigDef.Type.STRING,
                "default-value", ConfigDef.Importance.LOW, "test broker synonym");

            AbstractKafkaConfig kafkaConfig = new AbstractKafkaConfig(configDef, new HashMap<>(brokerProps), Map.of(), false) { };

            // The test config is not the streams assignors config, so the coordinator config is never consulted.
            return kafkaConfig.extractGroupConfigMap(mock(GroupCoordinatorConfig.class));
        }
    }
}
