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
package org.apache.kafka.tools.consumer.group;

import kafka.test.ClusterConfig;
import kafka.test.ClusterGenerator;
import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterTemplate;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ToolsTestUtils;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static kafka.test.annotation.Type.CO_KRAFT;
import static kafka.test.annotation.Type.KRAFT;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.ConsumerGroupState.EMPTY;
import static org.apache.kafka.common.ConsumerGroupState.STABLE;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(value = ClusterTestExtensions.class)
public class ReproduceHeartbeatFailTest {

    private static void generator(ClusterGenerator clusterGenerator) {
        Map<String, String> serverProperties = new HashMap<>();
        serverProperties.put(OFFSETS_TOPIC_PARTITIONS_CONFIG, "1");
        serverProperties.put(OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1");
        serverProperties.put(NEW_GROUP_COORDINATOR_ENABLE_CONFIG, "true");

        ClusterConfig consumerGroupCoordinator = ClusterConfig.defaultBuilder()
            .setTypes(Stream.of(KRAFT, CO_KRAFT).collect(Collectors.toSet()))
            .setServerProperties(serverProperties)
            .setTags(Collections.singletonList("newGroupCoordinator"))
            .build();
        clusterGenerator.accept(consumerGroupCoordinator);
    }


    @ClusterTemplate("generator")
    void testDeleteEmptyGroup(ClusterInstance cluster) throws Exception {
        for (GroupProtocol groupProtocol : cluster.supportedGroupProtocols()) {
            String groupId = composeGroupId(groupProtocol);
            String topicName = composeTopicName(groupProtocol);
            String[] cgcArgs = new String[]{"--bootstrap-server", cluster.bootstrapServers(), "--delete", "--group", groupId};
            try (
                AutoCloseable consumerGroupCloseable = consumerGroupClosable(cluster, groupProtocol, groupId, topicName);
                ConsumerGroupCommand.ConsumerGroupService service = getConsumerGroupService(cgcArgs)
            ) {
                TestUtils.waitForCondition(
                    () -> service.listConsumerGroups().contains(groupId) && checkGroupState(service, groupId, STABLE),
                    "The group did not initialize as expected."
                );

                consumerGroupCloseable.close();

                TestUtils.waitForCondition(
                    () -> checkGroupState(service, groupId, EMPTY),
                    "The group did not become empty as expected."
                );

                Map<String, Throwable> result = new HashMap<>();
                String output = ToolsTestUtils.grabConsoleOutput(() -> result.putAll(service.deleteGroups()));

                assertTrue(output.contains("Deletion of requested consumer groups ('" + groupId + "') was successful."),
                    "The consumer group could not be deleted as expected");
                assertEquals(1, result.size());
                assertTrue(result.containsKey(groupId));
                assertNull(result.get(groupId), "The consumer group could not be deleted as expected");
            }
        }

    }

    private String composeGroupId(GroupProtocol protocol) {
        String groupPrefix = "test.";
        return protocol != null ? groupPrefix + protocol.name : groupPrefix + "dummy";
    }

    private String composeTopicName(GroupProtocol protocol) {
        String topicPrefix = "foo.";
        return protocol != null ? topicPrefix + protocol.name : topicPrefix + "dummy";
    }

    private AutoCloseable consumerGroupClosable(ClusterInstance cluster, GroupProtocol protocol, String groupId, String topicName) {
        Map<String, Object> configs = composeConfigs(
            cluster,
            groupId,
            protocol.name,
            emptyMap());

        return new KafkaConsumer<String, String>(configs);

    }

    private boolean checkGroupState(ConsumerGroupCommand.ConsumerGroupService service, String groupId, ConsumerGroupState
        state) throws Exception {
        return Objects.equals(service.collectGroupState(groupId).state, state);
    }

    private ConsumerGroupCommand.ConsumerGroupService getConsumerGroupService(String[] args) {
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(args);
        return new ConsumerGroupCommand.ConsumerGroupService(
            opts,
            singletonMap(AdminClientConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
        );
    }

    private Map<String, Object> composeConfigs(ClusterInstance cluster, String groupId, String groupProtocol, Map<String, Object> customConfigs) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        configs.put(GROUP_ID_CONFIG, groupId);
        configs.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(GROUP_PROTOCOL_CONFIG, groupProtocol);
        configs.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());

        configs.putAll(customConfigs);
        return configs;
    }
}
