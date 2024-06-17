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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.utils.Exit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerPerformanceTest {
    private final ToolsTestUtils.MockExitProcedure exitProcedure = new ToolsTestUtils.MockExitProcedure();
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

    @TempDir
    static Path tempDir;

    @BeforeEach
    public void beforeEach() {
        Exit.setExitProcedure(exitProcedure);
    }

    @AfterEach
    public void afterEach() {
        Exit.resetExitProcedure();
    }

    @Test
    public void testDetailedHeaderMatchBody() {
        testHeaderMatchContent(true, 2,
            () -> ConsumerPerformance.printConsumerProgress(1, 1024 * 1024, 0, 1, 0, 0, 1, dateFormat, 1L));
    }

    @Test
    public void testNonDetailedHeaderMatchBody() {
        testHeaderMatchContent(false, 2,
            () -> ConsumerPerformance.printConsumerProgress(1, 1024 * 1024, 0, 1, 0, 0, 1, dateFormat, 1L));
    }

    @Test
    public void testConfigBrokerList() {
        String[] args = new String[]{
            "--broker-list", "localhost:9092",
            "--topic", "test",
            "--messages", "10"
        };

        ConsumerPerformance.ConsumerPerfOptions config = new ConsumerPerformance.ConsumerPerfOptions(args);

        assertEquals("localhost:9092", config.brokerHostsAndPorts());
        assertTrue(config.topic().contains("test"));
        assertEquals(10, config.numMessages());
    }

    @Test
    public void testConfigBootStrapServer() {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--messages", "10",
            "--print-metrics"
        };

        ConsumerPerformance.ConsumerPerfOptions config = new ConsumerPerformance.ConsumerPerfOptions(args);

        assertEquals("localhost:9092", config.brokerHostsAndPorts());
        assertTrue(config.topic().contains("test"));
        assertEquals(10, config.numMessages());
    }

    @Test
    public void testBrokerListOverride() {
        String[] args = new String[]{
            "--broker-list", "localhost:9094",
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--messages", "10"
        };

        ConsumerPerformance.ConsumerPerfOptions config = new ConsumerPerformance.ConsumerPerfOptions(args);

        assertEquals("localhost:9092", config.brokerHostsAndPorts());
        assertTrue(config.topic().contains("test"));
        assertEquals(10, config.numMessages());
    }

    @Test
    public void testConfigWithUnrecognizedOption() {
        String[] args = new String[]{
            "--broker-list", "localhost:9092",
            "--topic", "test",
            "--messages", "10",
            "--new-consumer"
        };

        String err = ToolsTestUtils.captureStandardErr(() -> new ConsumerPerformance.ConsumerPerfOptions(args));

        assertTrue(err.contains("new-consumer is not a recognized option"));
    }

    @Test
    public void testClientIdOverride() throws IOException {
        File tempFile = Files.createFile(tempDir.resolve("test_consumer_config.conf")).toFile();
        try (PrintWriter output = new PrintWriter(Files.newOutputStream(tempFile.toPath()))) {
            output.println("client.id=consumer-1");
            output.flush();
        }

        String[] args = new String[]{
            "--broker-list", "localhost:9092",
            "--topic", "test",
            "--messages", "10",
            "--consumer.config", tempFile.getAbsolutePath()
        };

        ConsumerPerformance.ConsumerPerfOptions config = new ConsumerPerformance.ConsumerPerfOptions(args);

        assertEquals("consumer-1", config.props().getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    public void testDefaultClientId() throws IOException {
        String[] args = new String[]{
            "--broker-list", "localhost:9092",
            "--topic", "test",
            "--messages", "10"
        };

        ConsumerPerformance.ConsumerPerfOptions config = new ConsumerPerformance.ConsumerPerfOptions(args);

        assertEquals("perf-consumer-client", config.props().getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    }

    private void testHeaderMatchContent(boolean detailed, int expectedOutputLineCount, Runnable runnable) {
        String out = ToolsTestUtils.captureStandardOut(() -> {
            ConsumerPerformance.printHeader(detailed);
            runnable.run();
        });

        String[] contents = out.split("\n");
        assertEquals(expectedOutputLineCount, contents.length);
        String header = contents[0];
        String body = contents[1];

        assertEquals(header.split(",\\s").length, body.split(",\\s").length);
    }
}
