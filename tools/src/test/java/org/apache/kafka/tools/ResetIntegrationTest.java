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
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTests;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.internals.Exit;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.test.TestUtils;

import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.time.Duration.ofMillis;
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
public class ResetIntegrationTest {
    private static final String NON_EXISTING_TOPIC = "nonExistingTopic";

    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static final String OUTPUT_TOPIC_2 = "outputTopic2";
    private static final String OUTPUT_TOPIC_2_RERUN = "outputTopic2_rerun";
    private static final String INTERMEDIATE_USER_TOPIC = "userTopic";

    private static final int STREAMS_CONSUMER_TIMEOUT = 2000;
    private static final int CLEANUP_CONSUMER_TIMEOUT = 2000;
    private static final long DEFAULT_TIMEOUT_MS = 60_000L;
    private static final String APP_ID_PREFIX = "reset-integration-test";

    private Properties streamsConfig;
    private Map<String, Object> resultConsumerConfig;
    private Map<String, Object> producerConfig;
    private long recordTimestamp;

    @ClusterTests({
        @ClusterTest,
        @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SSL, controllerSecurityProtocol = SecurityProtocol.SSL)
    })
    public void testResetWhenInternalTopicsAreSpecified(ClusterInstance cluster) throws Exception {
        final Map<String, Object> sslConfig = cluster.setClientSslConfig(new HashMap<>());
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, sslConfig, appId);
            runResetWhenInternalTopicsAreSpecified(cluster, admin, sslConfig, appId);
        }
    }

    @ClusterTests({
        @ClusterTest,
        @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SSL, controllerSecurityProtocol = SecurityProtocol.SSL)
    })
    public void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic(ClusterInstance cluster) throws Exception {
        final Map<String, Object> sslConfig = cluster.setClientSslConfig(new HashMap<>());
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, sslConfig, appId);
            runReprocessingFromScratchWithoutIntermediateUserTopic(cluster, admin, sslConfig, appId);
        }
    }

    @ClusterTests({
        @ClusterTest,
        @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SSL, controllerSecurityProtocol = SecurityProtocol.SSL)
    })
    public void testReprocessingFromScratchAfterResetWithIntermediateUserTopic(ClusterInstance cluster) throws Exception {
        final Map<String, Object> sslConfig = cluster.setClientSslConfig(new HashMap<>());
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, sslConfig, appId);
            runReprocessingFromScratchWithIntermediateUserTopic(cluster, admin, sslConfig, false, appId);
        }
    }

    @ClusterTests({
        @ClusterTest,
        @ClusterTest(brokerSecurityProtocol = SecurityProtocol.SSL, controllerSecurityProtocol = SecurityProtocol.SSL)
    })
    public void testReprocessingFromScratchAfterResetWithIntermediateInternalTopic(ClusterInstance cluster) throws Exception {
        final Map<String, Object> sslConfig = cluster.setClientSslConfig(new HashMap<>());
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, sslConfig, appId);
            runReprocessingFromScratchWithIntermediateUserTopic(cluster, admin, sslConfig, true, appId);
        }
    }

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
            "--force",
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
            "--force",
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
            "--force",
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
            "--force",
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
            "--force",
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
    public void shouldSucceedWhenApplicationIdExistsAsConsumerGroup(ClusterInstance cluster) throws Exception {
        try (Admin adminClient = cluster.admin()) {
            final String appID = generateAppId();
            prepare(cluster, null, appID);
            streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

            final KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            startApplicationAndWaitUntilRunning(streams);
            waitUntilMinKeyValueRecordsReceived(cluster, resultConsumerConfig, OUTPUT_TOPIC, 10);
            streams.close();
            waitForEmptyConsumerGroup(adminClient, appID);

            final KafkaStreams cleanupInstance = new KafkaStreams(
                    setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
            cleanupInstance.cleanUp();
            cleanupInstance.close();

            final String[] parameters = new String[] {
                "--application-id", appID,
                "--bootstrap-server", cluster.bootstrapServers()
            };
            final Properties cleanUpConfig = new Properties();
            cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
            cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

            final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
            assertEquals(0, exitCode);
        }
    }

    @ClusterTest
    public void shouldFailWithErrorWhenApplicationIdDoesNotExist(ClusterInstance cluster) {
        final String nonExistentAppID = generateAppId() + "-does-not-exist";

        final String[] parameters = new String[] {
            "--application-id", nonExistentAppID,
            "--bootstrap-server", cluster.bootstrapServers()
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final String errorOutput = ToolsTestUtils.captureStandardErr(() -> {
            final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
            assertEquals(1, exitCode);
        });

        assertTrue(errorOutput.contains(nonExistentAppID));
        assertTrue(errorOutput.contains("Refusing to delete internal topics"));
    }

    @ClusterTest
    public void shouldSucceedWhenApplicationIdDoesNotExistWithForce(ClusterInstance cluster) {

        final String nonExistentAppID = generateAppId() + "-does-not-exist";

        final String[] parameters = new String[] {
            "--application-id", nonExistentAppID,
            "--force",
            "--bootstrap-server", cluster.bootstrapServers()
        };

        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "100");
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final String output = ToolsTestUtils.captureStandardOut(() -> {
            final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
            assertEquals(0, exitCode);
        });

        assertTrue(output.contains("Done."));
    }

    @ClusterTest
    public void shouldNotAllowToResetWhenSpecifiedInternalTopicIsNotInternal(ClusterInstance cluster) throws Exception {
        final String appId = generateAppId();
        cluster.createTopic(INPUT_TOPIC, 1, (short) 1);

        final String[] parameters = new String[] {
            "--application-id", appId,
            "--force",
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

    private String generateAppId() {
        return APP_ID_PREFIX + "-" + TestUtils.randomString(10);
    }

    private void prepare(final ClusterInstance cluster, final Map<String, Object> sslConfig, final String appId) throws Exception {
        prepareConfigs(cluster, sslConfig, appId);
        // align time to seconds to get clean window boundaries and thus ensure the same result for each run
        recordTimestamp = (System.currentTimeMillis() / 1000 + 1) * 1000;

        cluster.createTopic(INPUT_TOPIC, 1, (short) 1);
        cluster.createTopic(OUTPUT_TOPIC, 1, (short) 1);
        cluster.createTopic(OUTPUT_TOPIC_2, 1, (short) 1);
        cluster.createTopic(OUTPUT_TOPIC_2_RERUN, 1, (short) 1);

        add10InputElements(cluster);
    }

    private void prepareConfigs(final ClusterInstance cluster, final Map<String, Object> sslConfig, final String appId) {
        // Producers and consumers are created via ClusterInstance, which injects bootstrap servers
        // and (when the cluster runs with SSL) the client SSL config automatically.
        producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        resultConsumerConfig = new HashMap<>();
        resultConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, appId + "-result-consumer");
        resultConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        resultConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        resultConsumerConfig.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name());
        resultConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        resultConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(STREAMS_CONSUMER_TIMEOUT));
        if (sslConfig != null) {
            streamsConfig.putAll(sslConfig);
        }
    }

    private void add10InputElements(final ClusterInstance cluster) {
        final List<KeyValue<Long, String>> records = List.of(
                KeyValue.pair(0L, "aaa"),
                KeyValue.pair(1L, "bbb"),
                KeyValue.pair(0L, "ccc"),
                KeyValue.pair(1L, "ddd"),
                KeyValue.pair(0L, "eee"),
                KeyValue.pair(1L, "fff"),
                KeyValue.pair(0L, "ggg"),
                KeyValue.pair(1L, "hhh"),
                KeyValue.pair(0L, "iii"),
                KeyValue.pair(1L, "jjj")
        );

        try (Producer<Long, String> producer = cluster.producer(producerConfig)) {
            for (final KeyValue<Long, String> record : records) {
                recordTimestamp += 10;
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, recordTimestamp, record.key, record.value));
            }
            producer.flush();
        }
    }

    private void produceKeyValuesWithTimestamp(
        final ClusterInstance cluster,
        final Collection<KeyValue<Long, String>> records,
        final long timestamp
    ) {
        try (Producer<Long, String> producer = cluster.producer(producerConfig)) {
            for (final KeyValue<Long, String> record : records) {
                producer.send(new ProducerRecord<>(INTERMEDIATE_USER_TOPIC, null, timestamp, record.key, record.value));
            }
            producer.flush();
        }
    }

    private <K, V> List<KeyValue<K, V>> waitUntilMinKeyValueRecordsReceived(
        final ClusterInstance cluster,
        final Map<String, Object> consumerConfig,
        final String topic,
        final int expectedNumRecords
    ) throws InterruptedException {
        final List<KeyValue<K, V>> accumData = new ArrayList<>();
        try (Consumer<K, V> consumer = cluster.consumer(consumerConfig)) {
            consumer.subscribe(List.of(topic));
            TestUtils.waitForCondition(() -> {
                final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
                for (final ConsumerRecord<K, V> record : records) {
                    accumData.add(new KeyValue<>(record.key(), record.value()));
                }
                return accumData.size() >= expectedNumRecords;
            }, DEFAULT_TIMEOUT_MS,
                () -> "Did not receive all " + expectedNumRecords + " records from topic " + topic
                    + ", currently accumulated: " + accumData);
        }
        return accumData;
    }

    private List<KeyValue<Long, Long>> waitUntilOutputRecordsReceived(
        final ClusterInstance cluster,
        final int expectedNumRecords
    ) throws InterruptedException {
        return waitUntilMinKeyValueRecordsReceived(cluster, resultConsumerConfig, OUTPUT_TOPIC, expectedNumRecords);
    }

    private void startApplicationAndWaitUntilRunning(final KafkaStreams streams) throws InterruptedException {
        streams.start();
        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.RUNNING,
            DEFAULT_TIMEOUT_MS,
            "Streams application did not reach the RUNNING state.");
    }

    private void waitForEmptyConsumerGroup(final Admin adminClient, final String appId) throws InterruptedException {
        TestUtils.waitForCondition(
            () -> isEmptyConsumerGroup(adminClient, appId), DEFAULT_TIMEOUT_MS,
            "Consumer group " + appId + " was not empty within " + DEFAULT_TIMEOUT_MS + " ms.");
    }

    private boolean isEmptyConsumerGroup(final Admin adminClient, final String appId) {
        try {
            return adminClient.describeConsumerGroups(List.of(appId))
                .describedGroups().get(appId).get().members().isEmpty();
        } catch (final ExecutionException e) {
            return e.getCause() instanceof GroupIdNotFoundException;
        } catch (final InterruptedException e) {
            return false;
        }
    }

    private Set<String> getAllTopics(final Admin adminClient) throws ExecutionException, InterruptedException {
        return adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names().get();
    }

    private boolean tryCleanGlobal(
            final ClusterInstance cluster,
            final Map<String, Object> sslConfig,
            final boolean withIntermediateTopics,
            final String resetScenario,
            final String resetScenarioArg,
            final String appId
    ) throws Exception {
        final List<String> parameterList = new ArrayList<>(
            List.of("--application-id", appId,
                    "--bootstrap-server", cluster.bootstrapServers(),
                    "--input-topics", INPUT_TOPIC
            ));
        if (withIntermediateTopics) {
            parameterList.add("--intermediate-topics");
            parameterList.add(INTERMEDIATE_USER_TOPIC);
        }

        if (sslConfig != null && !sslConfig.isEmpty()) {
            final File configFile = TestUtils.tempFile();
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(configFile))) {
                for (final Map.Entry<String, Object> entry : sslConfig.entrySet()) {
                    final Object value = entry.getValue();
                    // Only scalar values can be round-tripped through a properties file. List-valued
                    // configs (e.g. ssl.cipher.suites, ssl.enabled.protocols) would be serialized as
                    // "[...]" and corrupt the client's SSL setup, so skip them and let them default.
                    if (value instanceof Password) {
                        writer.write(entry.getKey() + "=" + ((Password) value).value() + "\n");
                    } else if (value instanceof String) {
                        writer.write(entry.getKey() + "=" + value + "\n");
                    }
                }
            }
            parameterList.add("--config-file");
            parameterList.add(configFile.getAbsolutePath());
        }
        if (resetScenario != null) {
            parameterList.add(resetScenario);
        }
        if (resetScenarioArg != null) {
            parameterList.add(resetScenarioArg);
        }

        final String[] parameters = parameterList.toArray(new String[0]);

        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        return new StreamsResetter().execute(parameters, cleanUpConfig) == 0;
    }

    private void cleanGlobal(
        final ClusterInstance cluster,
        final Map<String, Object> sslConfig,
        final boolean withIntermediateTopics,
        final String resetScenario,
        final String resetScenarioArg,
        final String appId
    ) throws Exception {
        assertTrue(tryCleanGlobal(cluster, sslConfig, withIntermediateTopics, resetScenario, resetScenarioArg, appId));
    }

    private void assertInternalTopicsGotDeleted(final Admin adminClient, final String additionalExistingTopic) throws Exception {
        if (additionalExistingTopic != null) {
            waitForRemainingTopics(adminClient, INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN,
                    Topic.GROUP_METADATA_TOPIC_NAME, additionalExistingTopic);
        } else {
            waitForRemainingTopics(adminClient, INPUT_TOPIC, OUTPUT_TOPIC, OUTPUT_TOPIC_2, OUTPUT_TOPIC_2_RERUN,
                    Topic.GROUP_METADATA_TOPIC_NAME);
        }
    }

    private void waitForRemainingTopics(final Admin adminClient, final String... expectedTopics) throws InterruptedException {
        final Set<String> expected = Set.of(expectedTopics);
        TestUtils.waitForCondition(() -> {
            try {
                return getAllTopics(adminClient).equals(expected);
            } catch (final ExecutionException e) {
                return false;
            }
        }, 30_000, () -> "Topics are not as expected, wanted: " + expected);
    }

    private void runResetWhenInternalTopicsAreSpecified(
        final ClusterInstance cluster,
        final Admin adminClient,
        final Map<String, Object> sslConfig,
        final String appId
    ) throws Exception {
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);

        // RUN
        KafkaStreams streams = new KafkaStreams(setupTopologyWithIntermediateTopic(true, OUTPUT_TOPIC_2), streamsConfig);
        startApplicationAndWaitUntilRunning(streams);
        waitUntilMinKeyValueRecordsReceived(cluster, resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appId);

        // RESET
        streams.cleanUp();

        final List<String> internalTopics = getAllTopics(adminClient).stream()
                .filter(StreamsResetter::matchesInternalTopicFormat)
                .toList();
        cleanGlobal(cluster, sslConfig, false,
                "--internal-topics",
                String.join(",", internalTopics.subList(1, internalTopics.size())), appId);
        waitForEmptyConsumerGroup(adminClient, appId);

        assertInternalTopicsGotDeleted(adminClient, internalTopics.get(0));
    }

    private void runReprocessingFromScratchWithoutIntermediateUserTopic(
        final ClusterInstance cluster,
        final Admin adminClient,
        final Map<String, Object> sslConfig,
        final String appId
    ) throws Exception {
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);

        // RUN
        KafkaStreams streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.start();
        final List<KeyValue<Long, Long>> result = waitUntilMinKeyValueRecordsReceived(cluster, resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appId);

        // RESET
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.cleanUp();
        cleanGlobal(cluster, sslConfig, false, null, null, appId);
        waitForEmptyConsumerGroup(adminClient, appId);

        assertInternalTopicsGotDeleted(adminClient, null);

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = waitUntilMinKeyValueRecordsReceived(cluster, resultConsumerConfig, OUTPUT_TOPIC, 10);
        streams.close();

        assertEquals(result, resultRerun);

        waitForEmptyConsumerGroup(adminClient, appId);
        cleanGlobal(cluster, sslConfig, false, null, null, appId);
    }

    private void runReprocessingFromScratchWithIntermediateUserTopic(
        final ClusterInstance cluster,
        final Admin adminClient,
        final Map<String, Object> sslConfig,
        final boolean useRepartitioned,
        final String appId
    ) throws Exception {
        if (!useRepartitioned) {
            cluster.createTopic(INTERMEDIATE_USER_TOPIC, 1, (short) 1);
        }

        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);

        // RUN
        KafkaStreams streams = new KafkaStreams(setupTopologyWithIntermediateTopic(useRepartitioned, OUTPUT_TOPIC_2), streamsConfig);
        startApplicationAndWaitUntilRunning(streams);
        final List<KeyValue<Long, Long>> result = waitUntilMinKeyValueRecordsReceived(cluster, resultConsumerConfig, OUTPUT_TOPIC, 10);
        // receive only first values to make sure intermediate user topic is not consumed completely
        // => required to test "seekToEnd" for intermediate topics
        final List<KeyValue<Long, Long>> result2 = waitUntilMinKeyValueRecordsReceived(cluster, resultConsumerConfig, OUTPUT_TOPIC_2, 40);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appId);

        // insert bad record to make sure intermediate user topic gets seekToEnd()
        final KeyValue<Long, String> badMessage = new KeyValue<>(-1L, "badRecord-ShouldBeSkipped");
        if (!useRepartitioned) {
            recordTimestamp += 1;
            produceKeyValuesWithTimestamp(cluster, Set.of(badMessage), recordTimestamp);
        }

        // RESET
        streams = new KafkaStreams(setupTopologyWithIntermediateTopic(useRepartitioned, OUTPUT_TOPIC_2_RERUN), streamsConfig);
        streams.cleanUp();
        cleanGlobal(cluster, sslConfig, !useRepartitioned, null, null, appId);
        waitForEmptyConsumerGroup(adminClient, appId);

        assertInternalTopicsGotDeleted(adminClient, useRepartitioned ? null : INTERMEDIATE_USER_TOPIC);

        // RE-RUN
        startApplicationAndWaitUntilRunning(streams);
        final List<KeyValue<Long, Long>> resultRerun = waitUntilMinKeyValueRecordsReceived(cluster, resultConsumerConfig, OUTPUT_TOPIC, 10);
        final List<KeyValue<Long, Long>> resultRerun2 = waitUntilMinKeyValueRecordsReceived(cluster, resultConsumerConfig, OUTPUT_TOPIC_2_RERUN, 40);
        streams.close();

        assertEquals(result, resultRerun);
        assertEquals(result2, resultRerun2);

        if (!useRepartitioned) {
            final Map<String, Object> intermediateConsumerConfig = new HashMap<>(resultConsumerConfig);
            intermediateConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            final List<KeyValue<Long, String>> resultIntermediate = waitUntilMinKeyValueRecordsReceived(cluster, intermediateConsumerConfig, INTERMEDIATE_USER_TOPIC, 21);

            for (int i = 0; i < 10; i++) {
                assertEquals(resultIntermediate.get(i + 11), resultIntermediate.get(i));
            }
            assertEquals(badMessage, resultIntermediate.get(10));
        }

        waitForEmptyConsumerGroup(adminClient, appId);
        cleanGlobal(cluster, sslConfig, !useRepartitioned, null, null, appId);

        if (!useRepartitioned) {
            adminClient.deleteTopics(List.of(INTERMEDIATE_USER_TOPIC)).all().get();
        }
    }

    private Topology setupTopologyWithIntermediateTopic(final boolean useRepartitioned,
                                                        final String outputTopic2) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        // use map to trigger internal re-partitioning before groupByKey
        input.map(KeyValue::new)
            .groupByKey()
            .count()
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.Long()));

        final KStream<Long, String> stream;
        if (useRepartitioned) {
            stream = input.repartition();
        } else {
            input.to(INTERMEDIATE_USER_TOPIC);
            stream = builder.stream(INTERMEDIATE_USER_TOPIC);
        }
        stream.groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(35)).advanceBy(ofMillis(10)))
            .count()
            .toStream()
            .map((key, value) -> new KeyValue<>(key.window().start() + key.window().end(), value))
            .to(outputTopic2, Produced.with(Serdes.Long(), Serdes.Long()));

        return builder.build();
    }

    private Topology setupTopologyWithoutIntermediateUserTopic() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Long, String> input = builder.stream(INPUT_TOPIC);

        // use map to trigger internal re-partitioning before groupByKey
        input.map((key, value) -> new KeyValue<>(key, key))
            .to(OUTPUT_TOPIC, Produced.with(Serdes.Long(), Serdes.Long()));

        return builder.build();
    }
}
