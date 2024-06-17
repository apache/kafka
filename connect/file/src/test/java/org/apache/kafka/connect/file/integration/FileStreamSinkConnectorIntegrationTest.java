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
package org.apache.kafka.connect.file.integration;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.file.FileStreamSinkConnector;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.connect.file.FileStreamSinkConnector.FILE_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.sink.SinkConnector.TOPICS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class FileStreamSinkConnectorIntegrationTest {

    private static final String CONNECTOR_NAME = "test-connector";
    private static final String TOPIC = "test-topic";
    private static final String MESSAGE_PREFIX = "Message ";
    private static final int NUM_MESSAGES = 5;
    private static final String FILE_NAME = "test-file";
    private final EmbeddedConnectCluster connect = new EmbeddedConnectCluster.Builder().build();

    @BeforeEach
    public void setup() {
        connect.start();
        connect.kafka().createTopic(TOPIC);
        produceMessagesToTopic(TOPIC, NUM_MESSAGES);
    }

    @AfterEach
    public void tearDown() {
        connect.stop();
    }

    @Test
    public void testSimpleSink() throws Exception {
        File tempDir = TestUtils.tempDirectory();
        Path tempFilePath = tempDir.toPath().resolve(FILE_NAME);
        Map<String, String> connectorConfigs = baseConnectorConfigs(TOPIC, tempFilePath.toString());
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 1,
            "Connector and task did not start in time");

        verifyLinesInFile(tempFilePath, NUM_MESSAGES, true);
    }

    @Test
    public void testAlterOffsets() throws Exception {
        File tempDir = TestUtils.tempDirectory();
        Path tempFilePath = tempDir.toPath().resolve(FILE_NAME);
        Map<String, String> connectorConfigs = baseConnectorConfigs(TOPIC, tempFilePath.toString());
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 1,
            "Connector and task did not start in time");

        verifyLinesInFile(tempFilePath, NUM_MESSAGES, true);

        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(CONNECTOR_NAME, "Connector did not stop in time");

        // Alter the offsets to cause the last message in the topic to be re-processed
        connect.alterSinkConnectorOffset(CONNECTOR_NAME, new TopicPartition(TOPIC, 0), (long) (NUM_MESSAGES - 1));

        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 1,
            "Connector and task did not resume in time");

        // The last message should be re-processed when the connector is resumed after the offsets are altered
        verifyLinesInFile(tempFilePath, NUM_MESSAGES + 1, false);
    }

    @Test
    public void testResetOffsets() throws Exception {
        File tempDir = TestUtils.tempDirectory();
        Path tempFilePath = tempDir.toPath().resolve(FILE_NAME);
        Map<String, String> connectorConfigs = baseConnectorConfigs(TOPIC, tempFilePath.toString());
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 1,
            "Connector and task did not start in time");

        verifyLinesInFile(tempFilePath, NUM_MESSAGES, true);

        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(CONNECTOR_NAME, "Connector did not stop in time");

        // Reset the offsets to cause all the message in the topic to be re-processed
        connect.resetConnectorOffsets(CONNECTOR_NAME);

        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 1,
            "Connector and task did not resume in time");

        // All the messages should be re-processed when the connector is resumed after the offsets are reset
        verifyLinesInFile(tempFilePath, 2 * NUM_MESSAGES, false);
    }

    @Test
    public void testSinkMultipleTopicsWithMultipleTasks() throws Exception {
        String topic2 = "test-topic-2";
        connect.kafka().createTopic(topic2);
        produceMessagesToTopic(topic2, NUM_MESSAGES);

        File tempDir = TestUtils.tempDirectory();
        Path tempFilePath = tempDir.toPath().resolve(FILE_NAME);
        Map<String, String> connectorConfigs = baseConnectorConfigs(TOPIC + "," + topic2, tempFilePath.toString());
        connectorConfigs.put(TASKS_MAX_CONFIG, "2");

        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 2,
            "Connector and task did not start in time");

        // Only verify the number of lines since the messages can be consumed in any order across the two topics
        verifyLinesInFile(tempFilePath, 2 * NUM_MESSAGES, false);
    }

    private void produceMessagesToTopic(String topic, int numMessages) {
        for (int i = 0; i < numMessages; i++) {
            connect.kafka().produce(topic, MESSAGE_PREFIX + i);
        }
    }

    private Map<String, String> baseConnectorConfigs(String topics, String filePath) {
        Map<String, String> connectorConfigs = new HashMap<>();
        connectorConfigs.put(CONNECTOR_CLASS_CONFIG, FileStreamSinkConnector.class.getName());
        connectorConfigs.put(TOPICS_CONFIG, topics);
        connectorConfigs.put(FILE_CONFIG, filePath);
        return connectorConfigs;
    }

    /**
     * Verify that the number of lines in the file at {@code filePath} is equal to {@code numLines} and that they all begin with the
     * prefix {@link #MESSAGE_PREFIX}.
     * <p>
     * If {@code verifyLinearity} is true, this method will also verify that the lines have a linearly increasing message number
     * (beginning with 0) after the prefix.
     *
     * @param filePath the file path
     * @param numLines the expected number of lines in the file
     * @param verifyLinearity true if the line contents are to be verified
     */
    private void verifyLinesInFile(Path filePath, int numLines, boolean verifyLinearity) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(filePath)))) {
            AtomicInteger i = new AtomicInteger(0);
            TestUtils.waitForCondition(() -> {
                reader.lines().forEach(line -> {
                    if (verifyLinearity) {
                        assertEquals(MESSAGE_PREFIX + i, line);
                    } else {
                        assertTrue(line.startsWith(MESSAGE_PREFIX));
                    }
                    i.getAndIncrement();
                });

                return i.get() >= numLines;
            }, "Expected to read " + numLines + " lines from the file");
        }

        // Ensure that there are exactly the expected number of lines present
        assertEquals(numLines, Files.readAllLines(filePath).size());
    }
}
