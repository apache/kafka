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
package org.apache.kafka.streams.integration;

import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.tools.StreamsResetter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.test.IntegrationTest;

import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.isEmptyConsumerGroup;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForEmptyConsumerGroup;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests local state store and global application cleanup.
 */
@Category({IntegrationTest.class})
public class ResetIntegrationTest extends AbstractResetIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);
    private static final String NON_EXISTING_TOPIC = "nonExistingTopic";

    public static final EmbeddedKafkaCluster CLUSTER;

    static {
        final Properties brokerProps = new Properties();
        // we double the value passed to `time.sleep` in each iteration in one of the map functions, so we disable
        // expiration of connections by the brokers to avoid errors when `AdminClient` sends requests after potentially
        // very long sleep times
        brokerProps.put(SocketServerConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG, -1L);
        CLUSTER = new EmbeddedKafkaCluster(1, brokerProps);
    }

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Override
    Map<String, Object> getClientSslConfig() {
        return null;
    }

    @Before
    public void before() throws Exception {
        cluster = CLUSTER;
        prepareTest();
    }

    @After
    public void after() throws Exception {
        cleanupTest();
    }

    @Test
    public void shouldNotAllowToResetWhileStreamsIsRunning() {
        final String appID = IntegrationTestUtils.safeUniqueTestName(testName);
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
        streams.start();

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        Assert.assertEquals(1, exitCode);

        streams.close();
    }

    @Test
    public void shouldNotAllowToResetWhenInputTopicAbsent() {
        final String appID = IntegrationTestUtils.safeUniqueTestName(testName);
        final String[] parameters = new String[] {
            "--application-id", appID,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--input-topics", NON_EXISTING_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        Assert.assertEquals(1, exitCode);
    }

    @Test
    public void shouldNotAllowToResetWhenIntermediateTopicAbsent() {
        final String appID = IntegrationTestUtils.safeUniqueTestName(testName);
        final String[] parameters = new String[] {
            "--application-id", appID,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--intermediate-topics", NON_EXISTING_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        Assert.assertEquals(1, exitCode);
    }

    @Test
    public void shouldNotAllowToResetWhenSpecifiedInternalTopicDoesNotExist() {
        final String appID = IntegrationTestUtils.safeUniqueTestName(testName);
        final String[] parameters = new String[] {
            "--application-id", appID,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--internal-topics", NON_EXISTING_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        Assert.assertEquals(1, exitCode);
    }

    @Test
    public void shouldNotAllowToResetWhenSpecifiedInternalTopicIsNotInternal() {
        final String appID = IntegrationTestUtils.safeUniqueTestName(testName);
        final String[] parameters = new String[] {
            "--application-id", appID,
            "--bootstrap-server", cluster.bootstrapServers(),
            "--internal-topics", INPUT_TOPIC
        };
        final Properties cleanUpConfig = new Properties();
        cleanUpConfig.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
        cleanUpConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(CLEANUP_CONSUMER_TIMEOUT));

        final int exitCode = new StreamsResetter().execute(parameters, cleanUpConfig);
        Assert.assertEquals(1, exitCode);
    }

    @Test
    public void testResetWhenLongSessionTimeoutConfiguredWithForceOption() throws Exception {
        final String appID = IntegrationTestUtils.safeUniqueTestName(testName);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        streamsConfig.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.toString(STREAMS_CONSUMER_TIMEOUT * 100));

        // Run
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

        streams.close();

        // RESET
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.cleanUp();

        // Reset would fail since long session timeout has been configured
        final boolean cleanResult = tryCleanGlobal(false, null, null, appID);
        Assert.assertFalse(cleanResult);

        // Reset will success with --force, it will force delete active members on broker side
        cleanGlobal(false, "--force", null, appID);
        assertThat("Group is not empty after cleanGlobal", isEmptyConsumerGroup(adminClient, appID));

        assertInternalTopicsGotDeleted(null);

        // RE-RUN
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        streams.close();

        assertThat(resultRerun, equalTo(result));
        cleanGlobal(false, "--force", null, appID);
    }

    @Test
    public void testReprocessingFromFileAfterResetWithoutIntermediateUserTopic() throws Exception {
        final String appID = IntegrationTestUtils.safeUniqueTestName(testName);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

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
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 5);
        streams.close();

        result.remove(0);
        assertThat(resultRerun, equalTo(result));

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(false, null, null, appID);
    }

    @Test
    public void testReprocessingFromDateTimeAfterResetWithoutIntermediateUserTopic() throws Exception {
        final String appID = IntegrationTestUtils.safeUniqueTestName(testName);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

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
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        streams.close();

        assertThat(resultRerun, equalTo(result));

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(false, null, null, appID);
    }

    @Test
    public void testReprocessingByDurationAfterResetWithoutIntermediateUserTopic() throws Exception {
        final String appID = IntegrationTestUtils.safeUniqueTestName(testName);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        // RUN
        streams = new KafkaStreams(setupTopologyWithoutIntermediateUserTopic(), streamsConfig);
        streams.start();
        final List<KeyValue<Long, Long>> result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);

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
        streams.start();
        final List<KeyValue<Long, Long>> resultRerun = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(resultConsumerConfig, OUTPUT_TOPIC, 10);
        streams.close();

        assertThat(resultRerun, equalTo(result));

        waitForEmptyConsumerGroup(adminClient, appID, TIMEOUT_MULTIPLIER * STREAMS_CONSUMER_TIMEOUT);
        cleanGlobal(false, null, null, appID);
    }

}
