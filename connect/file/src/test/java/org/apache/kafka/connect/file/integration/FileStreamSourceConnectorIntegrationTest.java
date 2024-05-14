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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.file.FileStreamSourceConnector.FILE_CONFIG;
import static org.apache.kafka.connect.file.FileStreamSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.file.FileStreamSourceTask.FILENAME_FIELD;
import static org.apache.kafka.connect.file.FileStreamSourceTask.POSITION_FIELD;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class FileStreamSourceConnectorIntegrationTest {

    private static final String CONNECTOR_NAME = "test-connector";
    private static final String TOPIC = "test-topic";
    private static final String LINE_FORMAT = "Line %d";
    private static final int NUM_LINES = 5;
    private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(15);
    private final EmbeddedConnectCluster connect = new EmbeddedConnectCluster.Builder().build();
    private File sourceFile;

    @BeforeEach
    public void setup() throws Exception {
        connect.start();
        sourceFile = createTempFile(NUM_LINES);
        connect.kafka().createTopic(TOPIC);
    }

    @AfterEach
    public void tearDown() {
        connect.stop();
    }

    @Test
    public void testSimpleSource() throws Exception {
        Map<String, String> connectorConfigs = baseConnectorConfigs(sourceFile.getAbsolutePath());
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 1,
            "Connector and task did not start in time");

        int i = 0;
        for (ConsumerRecord<byte[], byte[]> record : connect.kafka().consume(NUM_LINES, TIMEOUT_MS, TOPIC)) {
            assertEquals(String.format(LINE_FORMAT, i++), new String(record.value()));
        }
    }

    @Test
    public void testStopResumeSavedOffset() throws Exception {
        Map<String, String> connectorConfigs = baseConnectorConfigs(sourceFile.getAbsolutePath());
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 1,
            "Connector and task did not start in time");

        // Wait for the initially written records to be sourced by the connector and produced to the configured Kafka topic
        connect.kafka().consume(NUM_LINES, TIMEOUT_MS, TOPIC);

        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(CONNECTOR_NAME, "Connector did not stop in time");

        // Append NUM_LINES more lines to the file
        try (PrintStream printStream = new PrintStream(Files.newOutputStream(sourceFile.toPath(), StandardOpenOption.APPEND))) {
            for (int i = NUM_LINES; i < 2 * NUM_LINES; i++) {
                printStream.println(String.format(LINE_FORMAT, i));
            }
        }

        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 1,
            "Connector and task did not resume in time");

        int i = 0;
        for (ConsumerRecord<byte[], byte[]> record : connect.kafka().consume(2 * NUM_LINES, TIMEOUT_MS, TOPIC)) {
            assertEquals(String.format(LINE_FORMAT, i++), new String(record.value()));
        }

        // We expect exactly (2 * NUM_LINES) messages to be produced since the connector should continue from where it left off on being resumed.
        // We verify this by consuming all the messages from the topic after we've already ensured that at least (2 * NUM_LINES) messages can be
        // consumed above.
        assertEquals(2 * NUM_LINES, connect.kafka().consumeAll(TIMEOUT_MS, TOPIC).count());
    }

    @Test
    public void testAlterOffsets() throws Exception {
        Map<String, String> connectorConfigs = baseConnectorConfigs(sourceFile.getAbsolutePath());
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 1,
            "Connector and task did not start in time");

        // Wait for the initially written records to be sourced by the connector and produced to the configured Kafka topic
        connect.kafka().consume(NUM_LINES, TIMEOUT_MS, TOPIC);

        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(CONNECTOR_NAME, "Connector did not stop in time");

        // Alter the offsets to make the connector re-process the last line in the file
        connect.alterSourceConnectorOffset(
            CONNECTOR_NAME,
            Collections.singletonMap(FILENAME_FIELD, sourceFile.getAbsolutePath()),
            Collections.singletonMap(POSITION_FIELD, 28L)
        );

        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 1,
            "Connector and task did not resume in time");

        Iterator<ConsumerRecord<byte[], byte[]>> recordIterator = connect.kafka().consume(NUM_LINES + 1, TIMEOUT_MS, TOPIC).iterator();

        for (int i = 0; i < NUM_LINES; i++) {
            assertEquals(String.format(LINE_FORMAT, i), new String(recordIterator.next().value()));
        }

        // Verify that the last line has been sourced again after the alter offsets request
        assertEquals(String.format(LINE_FORMAT, NUM_LINES - 1), new String(recordIterator.next().value()));
    }

    @Test
    public void testResetOffsets() throws Exception {
        Map<String, String> connectorConfigs = baseConnectorConfigs(sourceFile.getAbsolutePath());
        connect.configureConnector(CONNECTOR_NAME, connectorConfigs);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 1,
            "Connector and task did not start in time");

        // Wait for the initially written records to be sourced by the connector and produced to the configured Kafka topic
        connect.kafka().consume(NUM_LINES, TIMEOUT_MS, TOPIC);

        connect.stopConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorIsStopped(CONNECTOR_NAME, "Connector did not stop in time");

        // Reset the offsets to make the connector re-read all the previously written lines
        connect.resetConnectorOffsets(CONNECTOR_NAME);

        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(CONNECTOR_NAME, 1,
            "Connector and task did not resume in time");

        Iterator<ConsumerRecord<byte[], byte[]>> recordIterator = connect.kafka().consume(2 * NUM_LINES, TIMEOUT_MS, TOPIC).iterator();

        int i = 0;
        while (i < NUM_LINES) {
            assertEquals(String.format(LINE_FORMAT, i++), new String(recordIterator.next().value()));
        }

        // Verify that the same lines have been sourced again after the reset offsets request
        while (i < 2 * NUM_LINES) {
            assertEquals(String.format(LINE_FORMAT, i - NUM_LINES), new String(recordIterator.next().value()));
            i++;
        }

        // We expect exactly (2 * NUM_LINES) messages to be produced since the connector should reprocess exactly the same NUM_LINES messages after
        // the offsets have been reset. We verify this by consuming all the messages from the topic after we've already ensured that at least
        // (2 * NUM_LINES) messages can be consumed above.
        assertEquals(2 * NUM_LINES, connect.kafka().consumeAll(TIMEOUT_MS, TOPIC).count());
    }

    /**
     * Create a temporary file and append {@code numLines} to it
     *
     * @param numLines the number of lines to be appended to the created file
     * @return the created file
     */
    private File createTempFile(int numLines) throws Exception {
        File sourceFile = TestUtils.tempFile();

        try (PrintStream printStream = new PrintStream(Files.newOutputStream(sourceFile.toPath()))) {
            for (int i = 0; i < numLines; i++) {
                printStream.println(String.format(LINE_FORMAT, i));
            }
        }

        return sourceFile;
    }

    private Map<String, String> baseConnectorConfigs(String filePath) {
        Map<String, String> connectorConfigs = new HashMap<>();
        connectorConfigs.put(CONNECTOR_CLASS_CONFIG, FileStreamSourceConnector.class.getName());
        connectorConfigs.put(TOPIC_CONFIG, TOPIC);
        connectorConfigs.put(FILE_CONFIG, filePath);
        return connectorConfigs;
    }
}
