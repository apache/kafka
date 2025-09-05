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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

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
import java.util.Properties;
import java.util.function.Function;

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
    public void testConfigBootStrapServer() {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--num-records", "10",
            "--print-metrics"
        };

        ConsumerPerformance.ConsumerPerfOptions config = new ConsumerPerformance.ConsumerPerfOptions(args);

        assertEquals("localhost:9092", config.brokerHostsAndPorts());
        assertTrue(config.topic().get().contains("test"));
        assertEquals(10, config.numRecords());
    }

    @Test
    public void testBootstrapServerNotPresent() {
        String[] args = new String[]{
            "--topic", "test"
        };

        String err = ToolsTestUtils.captureStandardErr(() ->
                new ConsumerPerformance.ConsumerPerfOptions(args));
        assertTrue(err.contains("Missing required argument \"[bootstrap-server]\""));
    }

    @Test
    public void testNumOfRecordsNotPresent() {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test"
        };

        String err = ToolsTestUtils.captureStandardErr(() ->
                new ConsumerPerformance.ConsumerPerfOptions(args));
        assertTrue(err.contains("Exactly one of the following arguments is required:"));
    }

    @Test
    public void testMessagesDeprecated() {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--messages", "10"
        };

        ConsumerPerformance.ConsumerPerfOptions config = new ConsumerPerformance.ConsumerPerfOptions(args);
        assertEquals(10, config.numRecords());
    }

    @Test
    public void testNumOfRecordsWithMessagesPresent() {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--messages", "10",
            "--num-records", "20"
        };

        String err = ToolsTestUtils.captureStandardErr(() ->
                new ConsumerPerformance.ConsumerPerfOptions(args));
        assertTrue(err.contains("Exactly one of the following arguments is required"));
    }

    @Test
    public void testConfigWithUnrecognizedOption() {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--num-records", "10",
            "--new-consumer"
        };

        String err = ToolsTestUtils.captureStandardErr(() -> new ConsumerPerformance.ConsumerPerfOptions(args));

        assertTrue(err.contains("new-consumer is not a recognized option"));
    }

    @Test
    public void testConfigWithInclude() {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--include", "test.*",
            "--num-records", "10"
        };

        ConsumerPerformance.ConsumerPerfOptions config = new ConsumerPerformance.ConsumerPerfOptions(args);

        assertEquals("localhost:9092", config.brokerHostsAndPorts());
        assertTrue(config.include().get().toString().contains("test.*"));
        assertEquals(10, config.numRecords());
    }

    @Test
    public void testConfigWithTopicAndInclude() {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--include", "test.*",
            "--num-records", "10"
        };

        String err = ToolsTestUtils.captureStandardErr(() -> new ConsumerPerformance.ConsumerPerfOptions(args));

        assertTrue(err.contains("Exactly one of the following arguments is required: [topic], [include]"));
    }

    @Test
    public void testConfigWithoutTopicAndInclude() {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--num-records", "10"
        };

        String err = ToolsTestUtils.captureStandardErr(() -> new ConsumerPerformance.ConsumerPerfOptions(args));

        assertTrue(err.contains("Exactly one of the following arguments is required: [topic], [include]"));
    }

    @Test
    public void testCommandProperty() throws IOException {
        Path configPath = tempDir.resolve("test_command_property_consumer_perf.conf");
        Files.deleteIfExists(configPath);
        File tempFile = Files.createFile(configPath).toFile();
        try (PrintWriter output = new PrintWriter(Files.newOutputStream(tempFile.toPath()))) {
            output.println("client.id=consumer-1");
            output.flush();
        }

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--num-records", "10",
            "--command-property", "client.id=consumer-2",
            "--command-config", tempFile.getAbsolutePath(),
            "--command-property", "prop=val"
        };

        ConsumerPerformance.ConsumerPerfOptions config = new ConsumerPerformance.ConsumerPerfOptions(args);

        assertEquals("consumer-2", config.props().getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
        assertEquals("val", config.props().getProperty("prop"));
    }

    @Test
    public void testClientIdOverride() throws IOException {
        Path configPath = tempDir.resolve("test_client_id_override_consumer_perf.conf");
        Files.deleteIfExists(configPath);
        File tempFile = Files.createFile(configPath).toFile();
        try (PrintWriter output = new PrintWriter(Files.newOutputStream(tempFile.toPath()))) {
            output.println("client.id=consumer-1");
            output.flush();
        }

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--num-records", "10",
            "--command-config", tempFile.getAbsolutePath()
        };

        ConsumerPerformance.ConsumerPerfOptions config = new ConsumerPerformance.ConsumerPerfOptions(args);

        assertEquals("consumer-1", config.props().getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    public void testConsumerConfigDeprecated() throws IOException {
        Path configPath = tempDir.resolve("test_consumer_config_deprecated_consumer_perf.conf");
        Files.deleteIfExists(configPath);
        File tempFile = Files.createFile(configPath).toFile();
        try (PrintWriter output = new PrintWriter(Files.newOutputStream(tempFile.toPath()))) {
            output.println("client.id=consumer-1");
            output.flush();
        }

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--num-records", "10",
            "--consumer.config", tempFile.getAbsolutePath()
        };

        ConsumerPerformance.ConsumerPerfOptions config = new ConsumerPerformance.ConsumerPerfOptions(args);

        assertEquals("consumer-1", config.props().getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    public void testCommandConfigWithConsumerConfigPresent() {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--num-records", "10",
            "--consumer.config", "some-path",
            "--command-config", "some-path"
        };

        String err = ToolsTestUtils.captureStandardErr(() ->
                new ConsumerPerformance.ConsumerPerfOptions(args));
        assertTrue(err.contains(String.format("Option \"%s\" can't be used with option \"%s\"",
                "[consumer.config]", "[command-config]")));
    }

    @Test
    public void testDefaultClientId() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--num-records", "10"
        };

        ConsumerPerformance.ConsumerPerfOptions config = new ConsumerPerformance.ConsumerPerfOptions(args);

        assertEquals("perf-consumer-client", config.props().getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    public void testMetricsRetrievedBeforeConsumerClosed() {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--num-records", "0",
            "--print-metrics"
        };

        Function<Properties, Consumer<byte[], byte[]>> consumerCreator = properties -> new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());

        String err = ToolsTestUtils.captureStandardErr(() -> ConsumerPerformance.run(args, consumerCreator));
        assertTrue(Utils.isBlank(err), "Should be no stderr message, but was \"" + err + "\"");
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
