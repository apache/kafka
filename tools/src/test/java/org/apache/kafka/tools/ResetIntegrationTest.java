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
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.utils.internals.Exit;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.test.api.Type.KRAFT;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests local state store and global application cleanup.
 */
@Timeout(600)
public class ResetIntegrationTest extends AbstractResetIntegrationTest {
    public static List<ClusterConfig> clusterConfigs() {
        return List.of(ClusterConfig.defaultBuilder()
                .setTypes(Set.of(KRAFT))
                .setBrokers(3)
                .setServerProperties(defaultBrokerProps())
                .build());
    }
    private static final String NON_EXISTING_TOPIC = "nonExistingTopic";

    @ClusterTemplate("clusterConfigs")
    public void shouldNotAllowToResetWhileStreamsIsRunning(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        final String[] parameters = new String[] {
            "--application-id", appID,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--input-topics", NON_EXISTING_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        startApplicationAndWaitUntilRunning(streams);

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        assertEquals(1, exitCode);

        streams.close();
    }

    @ClusterTemplate("clusterConfigs")
    public void shouldNotAllowToResetWhenInputTopicAbsent(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        final String[] parameters = new String[] {
            "--application-id", appID,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--input-topics", NON_EXISTING_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        assertEquals(1, exitCode);
    }

    @ClusterTemplate("clusterConfigs")
    public void shouldDefaultToClassicGroupProtocol(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        final String[] parameters = new String[] {
            "--application-id", appID,
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

    @ClusterTemplate("clusterConfigs")
    public void shouldAllowGroupProtocolClassic(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        final String[] parameters = new String[] {
            "--application-id", appID,
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

    @ClusterTemplate("clusterConfigs")
    public void shouldOverwriteGroupProtocolOtherThanClassic(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        final String[] parameters = new String[] {
            "--application-id", appID,
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

    @ClusterTemplate("clusterConfigs")
    public void shouldNotAllowToResetWhenIntermediateTopicAbsent(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        final String[] parameters = new String[] {
            "--application-id", appID,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--intermediate-topics", NON_EXISTING_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        assertEquals(1, exitCode);
    }

    @ClusterTemplate("clusterConfigs")
    public void shouldNotAllowToResetWhenSpecifiedInternalTopicDoesNotExist(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        final String[] parameters = new String[] {
            "--application-id", appID,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--internal-topics", NON_EXISTING_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        assertEquals(1, exitCode);
    }

    @ClusterTemplate("clusterConfigs")
    public void shouldNotAllowToResetWhenSpecifiedInternalTopicIsNotInternal(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        final String[] parameters = new String[] {
            "--application-id", appID,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--internal-topics", INPUT_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        assertEquals(1, exitCode);
    }

    @ClusterTemplate("clusterConfigs")
    public void testDeprecatedConfig(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        File configFile = TestUtils.tempFile("client.id=my-client");

        final String appID = safeUniqueTestName(testInfo);
        final String[] parameters = new String[] {
            "--application-id", appID,
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

    @ClusterTemplate("clusterConfigs")
    public void testCommandConfig(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        File configFile = TestUtils.tempFile("client.id=my-client");

        final String appID = safeUniqueTestName(testInfo);
        final String[] parameters = new String[] {
            "--application-id", appID,
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

    @ClusterTemplate("clusterConfigs")
    public void testCommandConfigAndDeprecatedConfigPresent(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        File configFile = TestUtils.tempFile("client.id=my-client");

        final String appID = safeUniqueTestName(testInfo);
        final String[] parameters = new String[] {
            "--application-id", appID,
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

    @ClusterTemplate("clusterConfigs")
    public void testResetWhenLongSessionTimeoutConfiguredWithForceOption(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        streamsConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(STREAMS_CONSUMER_TIMEOUT * 100));

        // Run
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        startApplicationAndWaitUntilRunning(streams);

        final List<KeyValue<Long, Long>> result = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();

        // RESET
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.cleanUp();

        // Reset would fail since long session timeout has been configured
        final boolean cleanResult = tryCleanGlobal(false, null, null, appID);
        assertFalse(cleanResult);

        // Reset will success with --force, it will force delete active members on broker side
        cleanGlobal(false, "--force", null, appID);
        assertTrue(isEmptyConsumerGroup(adminClient, appID), "Group is not empty after cleanGlobal");

        assertInternalTopicsGotDeleted(null);

        // RE-RUN
        startApplicationAndWaitUntilRunning(streams);
        final List<KeyValue<Long, Long>> resultRerun = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        streams.close();

        assertEquals(result, resultRerun);
        cleanGlobal(false, "--force", null, appID);
    }

    @ClusterTemplate("clusterConfigs")
    public void testReprocessingFromFileAfterResetWithoutIntermediateUserTopic(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        startApplicationAndWaitUntilRunning(streams);

        final List<KeyValue<Long, Long>> result = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        // RESET
        final File resetFile = TestUtils.tempFile("reset", ".csv");
        try (final BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile))) {
            writer.write(INPUT_TOPIC + ",0,1");
        }

        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.cleanUp();

        cleanGlobal(false, "--from-file", resetFile.getAbsolutePath(), appID);
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(null);

        resetFile.deleteOnExit();

        // RE-RUN
        startApplicationAndWaitUntilRunning(streams);
        final List<KeyValue<Long, Long>> resultRerun = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 5);
        streams.close();

        result.remove(0);
        assertEquals(result, resultRerun);

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(false, null, null, appID);
    }

    @ClusterTemplate("clusterConfigs")
    public void testReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        startApplicationAndWaitUntilRunning(streams);

        final List<KeyValue<Long, Long>> result = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

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

        cleanGlobal(false, "--to-datetime", format.format(calendar.getTime()), appID);
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(null);

        resetFile.deleteOnExit();

        // RE-RUN
        startApplicationAndWaitUntilRunning(streams);

        final List<KeyValue<Long, Long>> resultRerun = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        streams.close();

        assertEquals(result, resultRerun);

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(false, null, null, appID);
    }

    @ClusterTemplate("clusterConfigs")
    public void testReprocessingByDurationAfterResetWithoutIntermediateUserTopic(final ClusterInstance clusterInstance, final TestInfo testInfo) throws Exception {
        prepareTest(clusterInstance, testInfo);
        final String appID = safeUniqueTestName(testInfo);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        startApplicationAndWaitUntilRunning(streams);

        final List<KeyValue<Long, Long>> result = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();
        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        // RESET
        final File resetFile = TestUtils.tempFile("reset", ".csv");
        try (final BufferedWriter writer = new BufferedWriter(new FileWriter(resetFile))) {
            writer.write(INPUT_TOPIC + ",0,1");
        }

        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.cleanUp();
        cleanGlobal(false, "--by-duration", "PT1M", appID);

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);

        assertInternalTopicsGotDeleted(null);

        resetFile.deleteOnExit();

        // RE-RUN
        startApplicationAndWaitUntilRunning(streams);

        final List<KeyValue<Long, Long>> resultRerun = waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        streams.close();

        assertEquals(result, resultRerun);

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(false, null, null, appID);
    }

}
