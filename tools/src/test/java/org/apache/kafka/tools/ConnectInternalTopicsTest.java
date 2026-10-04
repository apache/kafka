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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectInternalTopicsTest {

    private static final String CONFIG_TOPIC_NAME = "config";
    private static final String STATUS_TOPIC_NAME = "status";
    private static final String OFFSET_TOPIC_NAME = "offset";

    @TempDir
    Path workspace;

    @ClusterTest(brokers = 3)
    void testCreateInternalTopicsWithDefaultValues(ClusterInstance cluster) throws Exception {
        var properties = new Properties();
        properties.setProperty("bootstrap.servers", cluster.bootstrapServers());
        properties.setProperty("config.storage.topic", CONFIG_TOPIC_NAME);
        properties.setProperty("status.storage.topic", STATUS_TOPIC_NAME);
        properties.setProperty("offset.storage.topic", OFFSET_TOPIC_NAME);
        var workerConfigPath = setupWorkerConfig(workspace.resolve("worker.properties"), properties);
        var res = runCommand("create", "--worker-config", workerConfigPath.toString());
        assertEquals(0, res.returnCode);
        try (var adminClient = cluster.admin()) {
            waitForTopics(adminClient, Set.of(CONFIG_TOPIC_NAME, STATUS_TOPIC_NAME, OFFSET_TOPIC_NAME));
            assertTopicPartitions(adminClient, CONFIG_TOPIC_NAME, 1);
            assertTopicPartitions(adminClient, STATUS_TOPIC_NAME, 5);
            assertTopicPartitions(adminClient, OFFSET_TOPIC_NAME, 25);
            assertTopicReplicationFactor(adminClient, CONFIG_TOPIC_NAME, (short) 3);
            assertTopicReplicationFactor(adminClient, STATUS_TOPIC_NAME, (short) 3);
            assertTopicReplicationFactor(adminClient, OFFSET_TOPIC_NAME, (short) 3);
        }
    }

    @ClusterTest
    void testNoWorkerConfig() {
        var res = runCommand("create");
        assertNotEquals(0, res.returnCode);
    }

    @ClusterTest
    void testWorkerConfigBlank() {
        var res = runCommand("create", "--worker-config", "");
        assertNotEquals(0, res.returnCode);
    }

    @ClusterTest
    void testWorkerConfigFileDoesNotExist() {
        var nonExistentPath = workspace.resolve("nonexistent-worker.properties").toString();
        var res = runCommand("create", "--worker-config", nonExistentPath);
        assertNotEquals(0, res.returnCode);
        assertTrue(res.err.contains("Unable to read worker config"));
    }

    @ClusterTest(brokers = 3)
    void testNoTopicNamesInWorkerConfig(ClusterInstance cluster) throws IOException {
        var properties = new Properties();
        properties.setProperty("bootstrap.servers", cluster.bootstrapServers());
        var configPath = setupWorkerConfig(workspace.resolve("worker-no-topics.properties"), properties);
        var res = runCommand("create", "--worker-config", configPath.toString());
        assertNotEquals(0, res.returnCode);
        assertEquals("Missing required configuration \"offset.storage.topic\" which has no default value.\n", res.err);
    }

    @ClusterTest(brokers = 3)
    void testEmptyTopicNamesInWorkerConfig(ClusterInstance cluster) throws IOException {
        var properties = new Properties();
        properties.setProperty("bootstrap.servers", cluster.bootstrapServers());
        properties.setProperty("config.storage.topic", "config");
        properties.setProperty("status.storage.topic", "status");
        properties.setProperty("offset.storage.topic", "");
        var configPath = setupWorkerConfig(workspace.resolve("worker-empty-topics.properties"), properties);
        var res = runCommand("create", "--worker-config", configPath.toString());
        assertNotEquals(0, res.returnCode);
        assertEquals("Must specify non-empty value for required internal topic config: 'offset.storage.topic'.\n", res.err);
    }

    @ClusterTest(brokers = 3)
    void testTopicConfigOverrides(ClusterInstance cluster) throws Exception {
        var properties = new Properties();
        properties.setProperty("bootstrap.servers", cluster.bootstrapServers());
        properties.setProperty("config.storage.topic", CONFIG_TOPIC_NAME);
        properties.setProperty("config.storage.retention.ms", "1000");
        properties.setProperty("status.storage.topic", STATUS_TOPIC_NAME);
        properties.setProperty("status.storage.retention.ms", "2000");
        properties.setProperty("offset.storage.topic", OFFSET_TOPIC_NAME);
        properties.setProperty("offset.storage.retention.ms", "3000");
        var configPath = setupWorkerConfig(workspace.resolve("worker-topic-overrides.properties"), properties);
        var res = runCommand("create", "--worker-config", configPath.toString());
        assertEquals(0, res.returnCode);
        try (var adminClient = cluster.admin()) {
            waitForTopics(adminClient, Set.of(CONFIG_TOPIC_NAME, STATUS_TOPIC_NAME, OFFSET_TOPIC_NAME));
            assertTopicConfig(adminClient, CONFIG_TOPIC_NAME, "retention.ms", "1000");
            assertTopicConfig(adminClient, STATUS_TOPIC_NAME, "retention.ms", "2000");
            assertTopicConfig(adminClient, OFFSET_TOPIC_NAME, "retention.ms", "3000");
            assertTopicReplicationFactor(adminClient, CONFIG_TOPIC_NAME, (short) 3);
            assertTopicReplicationFactor(adminClient, STATUS_TOPIC_NAME, (short) 3);
            assertTopicReplicationFactor(adminClient, OFFSET_TOPIC_NAME, (short) 3);
        }
    }

    @ClusterTest(brokers = 3)
    void testCreateMissingTopics(ClusterInstance cluster) throws Exception {
        try (var adminClient = cluster.admin()) {
            adminClient.createTopics(Set.of(
                    new NewTopic(CONFIG_TOPIC_NAME, 1, (short) 1)
                            .configs(Map.of("retention.ms", "1000"))
            ));
            waitForTopics(adminClient, Set.of(CONFIG_TOPIC_NAME));
        }
        var properties = new Properties();
        properties.setProperty("bootstrap.servers", cluster.bootstrapServers());
        properties.setProperty("config.storage.topic", CONFIG_TOPIC_NAME);
        properties.setProperty("status.storage.topic", STATUS_TOPIC_NAME);
        properties.setProperty("offset.storage.topic", OFFSET_TOPIC_NAME);
        var configPath = setupWorkerConfig(workspace.resolve("worker-partial-topics.properties"), properties);
        var res = runCommand("create", "--worker-config", configPath.toString());
        assertEquals(0, res.returnCode);
        try (var adminClient = cluster.admin()) {
            waitForTopics(adminClient, Set.of(CONFIG_TOPIC_NAME, STATUS_TOPIC_NAME, OFFSET_TOPIC_NAME));
            assertTopicConfig(adminClient, CONFIG_TOPIC_NAME, "retention.ms", "1000");
            assertTopicPartitions(adminClient, CONFIG_TOPIC_NAME, 1);
            assertTopicPartitions(adminClient, STATUS_TOPIC_NAME, 5);
            assertTopicPartitions(adminClient, OFFSET_TOPIC_NAME, 25);
            assertTopicReplicationFactor(adminClient, CONFIG_TOPIC_NAME, (short) 1);
            assertTopicReplicationFactor(adminClient, STATUS_TOPIC_NAME, (short) 3);
            assertTopicReplicationFactor(adminClient, OFFSET_TOPIC_NAME, (short) 3);
        }
    }

    private record CommandResult(int returnCode, String out, String err) {
    }

    private static CommandResult runCommand(String... args) {
        var out = new ByteArrayOutputStream();
        var err = new ByteArrayOutputStream();
        var code = ConnectInternalTopics.mainNoExit(
                args,
                new PrintStream(out, true),
                new PrintStream(err, true)
        );
        return new CommandResult(code, out.toString(), err.toString());
    }

    private static void assertTopicConfig(Admin admin, String topic, String configKey, String expectedValue) throws Exception {
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        var describeResult = admin.describeConfigs(Set.of(resource));
        var config = describeResult.all().get().get(resource);
        assertEquals(expectedValue, config.get(configKey).value());
    }

    private static void assertTopicReplicationFactor(Admin admin, String topic, short expectedReplicationFactor) throws Exception {
        var topicDescriptionFuture = admin.describeTopics(Set.of(topic)).topicNameValues().get(topic);
        var topicDescription = topicDescriptionFuture.get();
        var replicationFactor = topicDescription.partitions().get(0).replicas().size();
        assertEquals(expectedReplicationFactor, replicationFactor);
    }

    private static void assertTopicPartitions(Admin admin, String topic, int expectedPartitions) throws Exception {
        var topicDescriptionFuture = admin.describeTopics(Set.of(topic)).topicNameValues().get(topic);
        var topicDescription = topicDescriptionFuture.get();
        var partitions = topicDescription.partitions().size();
        assertEquals(expectedPartitions, partitions);
    }

    private static void waitForTopics(Admin admin, Set<String> expectedTopics) throws InterruptedException {
        TestUtils.waitForCondition(() -> admin.listTopics().names().get().containsAll(expectedTopics),
                "timed out waiting for topics");
    }

    private static Path setupWorkerConfig(Path path, Properties properties) throws IOException {
        path.getParent().toFile().mkdirs();
        try (var outputStream = Files.newOutputStream(path)) {
            properties.store(outputStream, "worker properties file");
        }
        return path;
    }
}
