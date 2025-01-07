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
package org.apache.kafka.tools;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestExtensions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(value = ClusterTestExtensions.class)
public class LogDirsCommandTest {
    private static final String TOPIC = "test-log-dirs-topic";

    @ClusterTest(brokers = 3)
    public void testLogDirsWithoutBrokers(ClusterInstance clusterInstance) {
        createTopic(clusterInstance, TOPIC);
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            String output = assertDoesNotThrow(() -> execute(fromArgsToOptions("--bootstrap-server", clusterInstance.bootstrapServers(), "--describe"), admin));

            // check all brokers are present
            clusterInstance.brokerIds().forEach(brokerId -> assertTrue(output.contains("\"broker\":" + brokerId)));

            // check all log dirs and topic partitions are present
            Map<Integer, Map<String, LogDirDescription>> logDirs = assertDoesNotThrow(() -> admin.describeLogDirs(clusterInstance.brokerIds()).allDescriptions().get());
            assertFalse(logDirs.isEmpty());
            logDirs.forEach((brokerId, logDirInfo) ->
                logDirInfo.forEach((logDir, logDirInfoValue) -> {
                    assertTrue(output.contains("\"logDir\":\"" + logDir + "\""));
                    logDirInfoValue.replicaInfos().forEach((topicPartition, replicaInfo) -> assertTrue(output.contains("\"partition\":\"" + topicPartition + "\"")));
                }));
        }
    }

    @ClusterTest(brokers = 3)
    public void testLogDirsWithBrokers(ClusterInstance clusterInstance) {
        createTopic(clusterInstance, TOPIC);
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            int brokerId = 0;
            String output = assertDoesNotThrow(() -> execute(fromArgsToOptions("--bootstrap-server", clusterInstance.bootstrapServers(), "--broker-list", String.valueOf(brokerId), "--describe"), admin));

            // check only broker in list is present
            assertTrue(output.contains("\"broker\":" + brokerId));
            clusterInstance.brokerIds().stream().filter(id -> id != brokerId).forEach(id -> assertFalse(output.contains("\"broker\":" + id)));

            // check log dir and topic partition are present
            Map<Integer, Map<String, LogDirDescription>> logDirs = assertDoesNotThrow(() -> admin.describeLogDirs(Collections.singleton(brokerId)).allDescriptions().get());
            assertEquals(1, logDirs.size());
            logDirs.forEach((brokerIdValue, logDirInfo) -> {
                assertFalse(logDirInfo.isEmpty());
                logDirInfo.forEach((logDir, logDirInfoValue) -> {
                    assertTrue(output.contains("\"logDir\":\"" + logDir + "\""));
                    Optional<TopicPartition> topicPartition = logDirInfoValue.replicaInfos().keySet().stream().filter(tp -> tp.topic().equals(TOPIC)).findFirst();
                    assertTrue(topicPartition.isPresent());
                    assertTrue(output.contains("\"partition\":\"" + topicPartition.get() + "\""));
                });
            });
        }
    }

    @ClusterTest
    public void testLogDirsWithNonExistentTopic(ClusterInstance clusterInstance) {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            String output = assertDoesNotThrow(() -> execute(fromArgsToOptions("--bootstrap-server", clusterInstance.bootstrapServers(), "--topic-list", TOPIC, "--describe"), admin));
            // check all brokers are present
            clusterInstance.brokerIds().forEach(brokerId -> assertTrue(output.contains("\"broker\":" + brokerId)));

            // check log dir is present, but topic partition is not
            Map<Integer, Map<String, LogDirDescription>> logDirs = assertDoesNotThrow(() -> admin.describeLogDirs(clusterInstance.brokerIds()).allDescriptions().get());
            assertFalse(logDirs.isEmpty());
            logDirs.forEach((brokerId, logDirInfo) ->
                logDirInfo.forEach((logDir, logDirInfoValue) -> {
                    assertTrue(output.contains("\"logDir\":\"" + logDir + "\""));
                    logDirInfoValue.replicaInfos().forEach((topicPartition, replicaInfo) -> {
                        assertFalse(output.contains("\"partition\":\"" + topicPartition + "\""));
                    });
                }));
        }
    }

    @ClusterTest
    public void testLogDirsWithSpecificTopic(ClusterInstance clusterInstance) {
        createTopic(clusterInstance, TOPIC);
        createTopic(clusterInstance, "other-topic");
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            String output = assertDoesNotThrow(() -> execute(fromArgsToOptions("--bootstrap-server", clusterInstance.bootstrapServers(), "--topic-list", TOPIC, "--describe"), admin));
            // check all brokers are present
            clusterInstance.brokerIds().forEach(brokerId -> assertTrue(output.contains("\"broker\":" + brokerId)));

            // check log dir is present and only one topic partition is present
            Map<Integer, Map<String, LogDirDescription>> logDirs = assertDoesNotThrow(() -> admin.describeLogDirs(clusterInstance.brokerIds()).allDescriptions().get());
            assertFalse(logDirs.isEmpty());
            assertTrue(output.contains("\"partition\":\"" + new TopicPartition(TOPIC, 0) + "\""));
            assertFalse(output.contains("\"partition\":\"" + new TopicPartition("other-topic", 0) + "\"")); // other-topic should not be present
            logDirs.forEach((brokerId, logDirInfo) ->
                logDirInfo.forEach((logDir, logDirInfoValue) -> {
                    assertTrue(output.contains("\"logDir\":\"" + logDir + "\""));
                    logDirInfoValue.replicaInfos().keySet().stream().filter(tp -> !tp.topic().equals(TOPIC)).forEach(tp -> {
                        assertFalse(output.contains("\"partition\":\"" + tp + "\""));
                    });
                }));
        }
    }

    @Test
    public void shouldThrowWhenQueryingNonExistentBrokers() {
        Node broker = new Node(1, "hostname", 9092);
        try (MockAdminClient adminClient = new MockAdminClient(Collections.singletonList(broker), broker)) {
            RuntimeException exception = assertThrows(RuntimeException.class, () -> execute(fromArgsToOptions("--bootstrap-server", "EMPTY", "--broker-list", "0,1,2", "--describe"), adminClient));
            assertNotNull(exception.getCause());
            assertEquals(TerseException.class, exception.getCause().getClass());
            assertEquals("ERROR: The given brokers do not exist from --broker-list: 0,2. Current existent brokers: 1", exception.getCause().getMessage());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotThrowWhenDuplicatedBrokers() throws JsonProcessingException {
        Node broker = new Node(1, "hostname", 9092);
        try (MockAdminClient adminClient = new MockAdminClient(Collections.singletonList(broker), broker)) {
            String standardOutput = execute(fromArgsToOptions("--bootstrap-server", "EMPTY", "--broker-list", "1,1", "--describe"), adminClient);
            String[] standardOutputLines = standardOutput.split("\n");
            assertEquals(3, standardOutputLines.length);
            Map<String, Object> information = new ObjectMapper().readValue(standardOutputLines[2], HashMap.class);
            List<Object> brokersInformation = (List<Object>) information.get("brokers");
            Integer brokerId = (Integer) ((HashMap<String, Object>) brokersInformation.get(0)).get("broker");
            assertEquals(1, brokersInformation.size());
            assertEquals(1, brokerId);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldQueryAllBrokersIfNonSpecified() throws JsonProcessingException {
        Node brokerOne = new Node(1, "hostname", 9092);
        Node brokerTwo = new Node(2, "hostname", 9092);
        try (MockAdminClient adminClient = new MockAdminClient(Arrays.asList(brokerTwo, brokerOne), brokerOne)) {
            String standardOutput = execute(fromArgsToOptions("--bootstrap-server", "EMPTY", "--describe"), adminClient);
            String[] standardOutputLines = standardOutput.split("\n");
            assertEquals(3, standardOutputLines.length);
            Map<String, Object> information = new ObjectMapper().readValue(standardOutputLines[2], HashMap.class);
            List<Object> brokersInformation = (List<Object>) information.get("brokers");
            Set<Integer> brokerIds = new HashSet<Integer>() {{
                    add((Integer) ((HashMap<String, Object>) brokersInformation.get(0)).get("broker"));
                    add((Integer) ((HashMap<String, Object>) brokersInformation.get(1)).get("broker"));
                }};
            assertEquals(2, brokersInformation.size());
            assertEquals(new HashSet<>(Arrays.asList(2, 1)), brokerIds);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldQuerySpecifiedBroker() throws JsonProcessingException {
        Node brokerOne = new Node(1, "hostname", 9092);
        Node brokerTwo = new Node(2, "hostname", 9092);
        try (MockAdminClient adminClient = new MockAdminClient(Arrays.asList(brokerOne, brokerTwo), brokerOne)) {
            String standardOutput = execute(fromArgsToOptions("--bootstrap-server", "EMPTY", "--broker-list", "1", "--describe"), adminClient);
            String[] standardOutputLines = standardOutput.split("\n");
            assertEquals(3, standardOutputLines.length);
            Map<String, Object> information = new ObjectMapper().readValue(standardOutputLines[2], HashMap.class);
            List<Object> brokersInformation = (List<Object>) information.get("brokers");
            Integer brokerId = (Integer) ((HashMap<String, Object>) brokersInformation.get(0)).get("broker");
            assertEquals(1, brokersInformation.size());
            assertEquals(1, brokerId);
        }
    }

    private LogDirsCommand.LogDirsCommandOptions fromArgsToOptions(String... args) {
        return new LogDirsCommand.LogDirsCommandOptions(args);
    }

    private String execute(LogDirsCommand.LogDirsCommandOptions options, Admin adminClient) {
        Runnable runnable = () -> {
            try {
                LogDirsCommand.execute(options, adminClient);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        return ToolsTestUtils.captureStandardOut(runnable);
    }

    private void createTopic(ClusterInstance clusterInstance, String topic) {
        try (Admin admin = Admin.create(Collections.singletonMap(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers()))) {
            assertDoesNotThrow(() -> admin.createTopics(Collections.singletonList(new NewTopic(topic, Collections.singletonMap(0, Collections.singletonList(0))))).topicId(topic).get());
            assertDoesNotThrow(() -> clusterInstance.waitForTopic(topic, 1));
        }
    }
}
