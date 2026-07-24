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
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.internals.Exit;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestUtils;

import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests local state store and global application cleanup.
 */
@ClusterTestDefaults(
    types = {Type.CO_KRAFT},
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "0"),
    }
)
public class ResetIntegrationTest extends AbstractResetIntegrationTest {
    private static final String NON_EXISTING_TOPIC = "nonExistingTopic";

    @ClusterTest
    public void testResetWhenInternalTopicsAreSpecified(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, null, appId);
            runResetWhenInternalTopicsAreSpecified(cluster, admin, null, appId);
        }
    }

    @ClusterTest
    public void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, null, appId);
            runReprocessingFromScratchWithoutIntermediateUserTopic(cluster, admin, null, appId);
        }
    }

    @ClusterTest
    public void testReprocessingFromScratchAfterResetWithIntermediateUserTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, null, appId);
            runReprocessingFromScratchWithIntermediateUserTopic(cluster, admin, null, false, appId);
        }
    }

    @ClusterTest
    public void testReprocessingFromScratchAfterResetWithIntermediateInternalTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, null, appId);
            runReprocessingFromScratchWithIntermediateUserTopic(cluster, admin, null, true, appId);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Same scenarios as above, but exercising the command line SSL setup for the reset tool.
    // ---------------------------------------------------------------------------------------------

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SSL, controllerSecurityProtocol = SecurityProtocol.SSL)
    public void testResetWhenInternalTopicsAreSpecifiedWithSsl(ClusterInstance cluster) throws Exception {
        final Map<String, Object> sslConfig = cluster.setClientSslConfig(new HashMap<>());
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, sslConfig, appId);
            runResetWhenInternalTopicsAreSpecified(cluster, admin, sslConfig, appId);
        }
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SSL, controllerSecurityProtocol = SecurityProtocol.SSL)
    public void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopicWithSsl(ClusterInstance cluster) throws Exception {
        final Map<String, Object> sslConfig = cluster.setClientSslConfig(new HashMap<>());
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, sslConfig, appId);
            runReprocessingFromScratchWithoutIntermediateUserTopic(cluster, admin, sslConfig, appId);
        }
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SSL, controllerSecurityProtocol = SecurityProtocol.SSL)
    public void testReprocessingFromScratchAfterResetWithIntermediateUserTopicWithSsl(ClusterInstance cluster) throws Exception {
        final Map<String, Object> sslConfig = cluster.setClientSslConfig(new HashMap<>());
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, sslConfig, appId);
            runReprocessingFromScratchWithIntermediateUserTopic(cluster, admin, sslConfig, false, appId);
        }
    }

    @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SSL, controllerSecurityProtocol = SecurityProtocol.SSL)
    public void testReprocessingFromScratchAfterResetWithIntermediateInternalTopicWithSsl(ClusterInstance cluster) throws Exception {
        final Map<String, Object> sslConfig = cluster.setClientSslConfig(new HashMap<>());
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, sslConfig, appId);
            runReprocessingFromScratchWithIntermediateUserTopic(cluster, admin, sslConfig, true, appId);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Argument validation against a running cluster
    // ---------------------------------------------------------------------------------------------

    @ClusterTest
    public void shouldNotAllowToResetWhileStreamsIsRunning(ClusterInstance cluster) throws Exception {
        final String appId = generateAppId();
        prepare(cluster, null, appId);
        final String[] parameters = new String[] {
            "--application-id", appId,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--input-topics", NON_EXISTING_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);

        // RUN
        final KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        startApplicationAndWaitUntilRunning(streams);

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        assertEquals(1, exitCode);

        streams.close();
    }

    @ClusterTest
    public void shouldNotAllowToResetWhenInputTopicAbsent(ClusterInstance cluster) {
        final String appId = generateAppId();
        final String[] parameters = new String[] {
            "--application-id", appId,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--input-topics", NON_EXISTING_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        assertEquals(1, exitCode);
    }

    @ClusterTest
    public void shouldDefaultToClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        final String appId = generateAppId();
        cluster.createTopic(INPUT_TOPIC, 1, (short) 1);
        final String[] parameters = new String[] {
            "--application-id", appId,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--input-topics", INPUT_TOPIC
        };
        final Properties cleanUpConfig = new Properties();

        // Set properties that are only allowed under the CLASSIC group protocol.
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));
        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        assertEquals(0, exitCode, "Resetter should use the CLASSIC group protocol");
    }

    @ClusterTest
    public void shouldAllowGroupProtocolClassic(ClusterInstance cluster) throws Exception {
        final String appId = generateAppId();
        cluster.createTopic(INPUT_TOPIC, 1, (short) 1);
        final String[] parameters = new String[] {
            "--application-id", appId,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--input-topics", INPUT_TOPIC
        };
        final Properties cleanUpConfig = new Properties();

        // Protocol config CLASSIC not needed but allowed.
        cleanUpConfig.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name());
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));
        int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        assertEquals(0, exitCode, "Resetter should allow setting group protocol to CLASSIC");
    }

    @ClusterTest
    public void shouldOverwriteGroupProtocolOtherThanClassic(ClusterInstance cluster) throws Exception {
        final String appId = generateAppId();
        cluster.createTopic(INPUT_TOPIC, 1, (short) 1);
        final String[] parameters = new String[] {
            "--application-id", appId,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--input-topics", INPUT_TOPIC
        };
        final Properties cleanUpConfig = new Properties();

        // Protocol config other than CLASSIC allowed but overwritten to CLASSIC.
        cleanUpConfig.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name());
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));
        int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        assertEquals(0, exitCode, "Resetter should overwrite the group protocol to CLASSIC");
    }

    @ClusterTest
    public void shouldNotAllowToResetWhenIntermediateTopicAbsent(ClusterInstance cluster) {
        final String appId = generateAppId();
        final String[] parameters = new String[] {
            "--application-id", appId,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--intermediate-topics", NON_EXISTING_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        assertEquals(1, exitCode);
    }

    @ClusterTest
    public void shouldNotAllowToResetWhenSpecifiedInternalTopicDoesNotExist(ClusterInstance cluster) {
        final String appId = generateAppId();
        final String[] parameters = new String[] {
            "--application-id", appId,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--internal-topics", NON_EXISTING_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        assertEquals(1, exitCode);
    }

    @ClusterTest
    public void shouldNotAllowToResetWhenSpecifiedInternalTopicIsNotInternal(ClusterInstance cluster) throws Exception {
        final String appId = generateAppId();
        cluster.createTopic(INPUT_TOPIC, 1, (short) 1);
        final String[] parameters = new String[] {
            "--application-id", appId,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--internal-topics", INPUT_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        assertEquals(1, exitCode);
    }

    @ClusterTest
    public void testDeprecatedConfig(ClusterInstance cluster) throws IOException {
        File configFile = TestUtils.tempFile("client.id=my-client");

        final String appId = generateAppId();
        final String[] parameters = new String[] {
            "--application-id", appId,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--internal-topics", INPUT_TOPIC,
            "--config-file", configFile.getAbsolutePath()
        };

        try (final MockedStatic<Admin> mockedAdmin = Mockito.mockStatic(Admin.class, Mockito.CALLS_REAL_METHODS)) {
            String output = ToolsTestUtils.captureStandardOut(() -> new StreamsResetter().execute(parameters));
            assertTrue(output.contains("Option --config-file has been deprecated and will be removed in a future version. Use --command-config instead."));

            ArgumentCaptor<Properties> argumentCaptor = ArgumentCaptor.forClass(Properties.class);
            mockedAdmin.verify(() -> Admin.create(argumentCaptor.capture()));
            final Properties actualProps = argumentCaptor.getValue();
            assertEquals("my-client", actualProps.get(AdminClientConfig.CLIENT_ID_CONFIG));
        }
    }

    @ClusterTest
    public void testCommandConfig(ClusterInstance cluster) throws IOException {
        File configFile = TestUtils.tempFile("client.id=my-client");

        final String appId = generateAppId();
        final String[] parameters = new String[] {
            "--application-id", appId,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--internal-topics", INPUT_TOPIC,
            "--command-config", configFile.getAbsolutePath()
        };

        try (final MockedStatic<Admin> mockedAdmin = Mockito.mockStatic(Admin.class, Mockito.CALLS_REAL_METHODS)) {
            new StreamsResetter().execute(parameters);

            ArgumentCaptor<Properties> argumentCaptor = ArgumentCaptor.forClass(Properties.class);
            mockedAdmin.verify(() -> Admin.create(argumentCaptor.capture()));
            final Properties actualProps = argumentCaptor.getValue();
            assertEquals("my-client", actualProps.get(AdminClientConfig.CLIENT_ID_CONFIG));
        }
    }

    @ClusterTest
    public void testCommandConfigAndDeprecatedConfigPresent(ClusterInstance cluster) throws IOException {
        File configFile = TestUtils.tempFile("client.id=my-client");

        final String appId = generateAppId();
        final String[] parameters = new String[] {
            "--application-id", appId,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--internal-topics", INPUT_TOPIC,
            "--config-file", configFile.getAbsolutePath(),
            "--command-config", configFile.getAbsolutePath()
        };

        try (final MockedStatic<Admin> mockedAdmin = Mockito.mockStatic(Admin.class, Mockito.CALLS_REAL_METHODS)) {
            // Mock Exit because CommandLineUtils.checkInvalidArgs calls exit
            Exit.setExitProcedure(new ToolsTestUtils.MockExitProcedure());

            String output = ToolsTestUtils.captureStandardErr(() -> new StreamsResetter().execute(parameters));

            assertTrue(output.contains(String.format("Option \"%s\" can't be used with option \"%s\"",
                "[config-file]", "[command-config]")));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @ClusterTest
    public void testResetWhenLongSessionTimeoutConfiguredWithForceOption(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, null, appId);
            streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
            streamsConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(STREAMS_CONSUMER_TIMEOUT * 100));

            // Run
            KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            startApplicationAndWaitUntilRunning(streams);

            final List<KeyValue<Long, Long>> result = waitUntilOutputRecordsReceived(cluster, 10);

            streams.close();

            // RESET
            streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            streams.cleanUp();

            // Reset would fail since long session timeout has been configured
            final boolean cleanResult = tryCleanGlobal(cluster, null, false, null, null, appId);
            assertFalse(cleanResult);

            // Reset will success with --force, it will force delete active members on broker side
            cleanGlobal(cluster, null, false, "--force", null, appId);
            assertTrue(isEmptyConsumerGroup(admin, appId), "Group is not empty after cleanGlobal");

            assertInternalTopicsGotDeleted(admin, null);

            // RE-RUN
            startApplicationAndWaitUntilRunning(streams);
            final List<KeyValue<Long, Long>> resultRerun = waitUntilOutputRecordsReceived(cluster, 10);
            streams.close();

            assertEquals(result, resultRerun);
            cleanGlobal(cluster, null, false, "--force", null, appId);
        }
    }

    @ClusterTest
    public void testReprocessingFromFileAfterResetWithoutIntermediateUserTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, null, appId);
            streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);

            // RUN
            KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            startApplicationAndWaitUntilRunning(streams);

            final List<KeyValue<Long, Long>> result = waitUntilOutputRecordsReceived(cluster, 10);

            streams.close();
            waitForEmptyConsumerGroup(admin, appId);

            // RESET
            final File resetFile = TestUtils.tempFile("reset", ".csv");
            try (final BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile))) {
                writer.write(INPUT_TOPIC + ",0,1");
            }

            streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            streams.cleanUp();

            cleanGlobal(cluster, null, false, "--from-file", resetFile.getAbsolutePath(), appId);
            waitForEmptyConsumerGroup(admin, appId);

            assertInternalTopicsGotDeleted(admin, null);

            resetFile.deleteOnExit();

            // RE-RUN
            startApplicationAndWaitUntilRunning(streams);
            final List<KeyValue<Long, Long>> resultRerun = waitUntilOutputRecordsReceived(cluster, 5);
            streams.close();

            result.remove(0);
            assertEquals(result, resultRerun);

            waitForEmptyConsumerGroup(admin, appId);
            cleanGlobal(cluster, null, false, null, null, appId);
        }
    }

    @ClusterTest
    public void testReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, null, appId);
            streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);

            // RUN
            KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            startApplicationAndWaitUntilRunning(streams);

            final List<KeyValue<Long, Long>> result = waitUntilOutputRecordsReceived(cluster, 10);

            streams.close();
            waitForEmptyConsumerGroup(admin, appId);

            // RESET
            final File resetFile = TestUtils.tempFile("reset", ".csv");
            try (final BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile))) {
                writer.write(INPUT_TOPIC + ",0,1");
            }

            streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            streams.cleanUp();

            final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            final Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.DATE, -1);

            cleanGlobal(cluster, null, false, "--to-datetime", format.format(calendar.getTime()), appId);
            waitForEmptyConsumerGroup(admin, appId);

            assertInternalTopicsGotDeleted(admin, null);

            resetFile.deleteOnExit();

            // RE-RUN
            startApplicationAndWaitUntilRunning(streams);

            final List<KeyValue<Long, Long>> resultRerun = waitUntilOutputRecordsReceived(cluster, 10);
            streams.close();

            assertEquals(result, resultRerun);

            waitForEmptyConsumerGroup(admin, appId);
            cleanGlobal(cluster, null, false, null, null, appId);
        }
    }

    @ClusterTest
    public void testReprocessingByDurationAfterResetWithoutIntermediateUserTopic(ClusterInstance cluster) throws Exception {
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, null, appId);
            streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);

            // RUN
            KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            startApplicationAndWaitUntilRunning(streams);

            final List<KeyValue<Long, Long>> result = waitUntilOutputRecordsReceived(cluster, 10);

            streams.close();
            waitForEmptyConsumerGroup(admin, appId);

            // RESET
            final File resetFile = TestUtils.tempFile("reset", ".csv");
            try (final BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile))) {
                writer.write(INPUT_TOPIC + ",0,1");
            }

            streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            streams.cleanUp();
            cleanGlobal(cluster, null, false, "--by-duration", "PT1M", appId);

            waitForEmptyConsumerGroup(admin, appId);

            assertInternalTopicsGotDeleted(admin, null);

            resetFile.deleteOnExit();

            // RE-RUN
            startApplicationAndWaitUntilRunning(streams);

            final List<KeyValue<Long, Long>> resultRerun = waitUntilOutputRecordsReceived(cluster, 10);
            streams.close();

            assertEquals(result, resultRerun);

            waitForEmptyConsumerGroup(admin, appId);
            cleanGlobal(cluster, null, false, null, null, appId);
        }
    }
}
