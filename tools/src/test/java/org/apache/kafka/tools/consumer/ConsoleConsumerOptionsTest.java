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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.test.MockDeserializer;
import org.apache.kafka.tools.ToolsTestUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsoleConsumerOptionsTest {

    @Test
    public void shouldParseValidConsumerValidConfig() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--from-beginning"
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);

        assertEquals("localhost:9092", config.bootstrapServer());
        assertEquals("test", config.topicArg().orElse(""));
        assertTrue(config.fromBeginning());
        assertFalse(config.enableSystestEventsLogging());
        assertFalse(config.skipMessageOnError());
        assertEquals(-1, config.maxMessages());
        assertEquals(Long.MAX_VALUE, config.timeoutMs());
    }

    @Test
    public void shouldParseIncludeArgument() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--include", "includeTest*",
            "--from-beginning"
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);

        assertEquals("localhost:9092", config.bootstrapServer());
        assertEquals("includeTest*", config.includedTopicsArg().orElse(""));
        assertTrue(config.fromBeginning());
    }

    @Test
    public void shouldParseValidSimpleConsumerValidConfigWithNumericOffset() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--partition", "0",
            "--offset", "3"
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);

        assertEquals("localhost:9092", config.bootstrapServer());
        assertEquals("test", config.topicArg().orElse(""));
        assertTrue(config.partitionArg().isPresent());
        assertEquals(0, config.partitionArg().getAsInt());
        assertEquals(3, config.offsetArg());
        assertFalse(config.fromBeginning());
    }

    @Test
    public void shouldExitOnUnrecognizedNewConsumerOption() {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        String[] args = new String[]{
            "--new-consumer",
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--from-beginning"
        };

        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void shouldExitIfPartitionButNoTopic() {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--include", "test.*",
            "--partition", "0"
        };

        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void shouldExitIfFromBeginningAndOffset() {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--partition", "0",
            "--from-beginning",
            "--offset", "123"
        };

        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void shouldParseValidSimpleConsumerValidConfigWithStringOffset() throws Exception {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--partition", "0",
            "--offset", "LatEst",
            "--property", "print.value=false"
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);

        assertEquals("localhost:9092", config.bootstrapServer());
        assertEquals("test", config.topicArg().orElse(""));
        assertTrue(config.partitionArg().isPresent());
        assertEquals(0, config.partitionArg().getAsInt());
        assertEquals(-1, config.offsetArg());
        assertFalse(config.fromBeginning());
        assertFalse(((DefaultMessageFormatter) config.formatter()).printValue());
    }

    @Test
    public void shouldParseValidConsumerConfigWithAutoOffsetResetLatest() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--consumer-property", "auto.offset.reset=latest"
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);
        Properties consumerProperties = config.consumerProps();

        assertEquals("localhost:9092", config.bootstrapServer());
        assertEquals("test", config.topicArg().orElse(""));
        assertFalse(config.fromBeginning());
        assertEquals("latest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    @Test
    public void shouldParseValidConsumerConfigWithAutoOffsetResetEarliest() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--consumer-property", "auto.offset.reset=earliest"
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);
        Properties consumerProperties = config.consumerProps();

        assertEquals("localhost:9092", config.bootstrapServer());
        assertEquals("test", config.topicArg().orElse(""));
        assertFalse(config.fromBeginning());
        assertEquals("earliest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    @Test
    public void shouldParseValidConsumerConfigWithAutoOffsetResetAndMatchingFromBeginning() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--consumer-property", "auto.offset.reset=earliest",
            "--from-beginning"
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);
        Properties consumerProperties = config.consumerProps();

        assertEquals("localhost:9092", config.bootstrapServer());
        assertEquals("test", config.topicArg().orElse(""));
        assertTrue(config.fromBeginning());
        assertEquals("earliest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    @Test
    public void shouldParseValidConsumerConfigWithNoOffsetReset() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test"
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);
        Properties consumerProperties = config.consumerProps();

        assertEquals("localhost:9092", config.bootstrapServer());
        assertEquals("test", config.topicArg().orElse(""));
        assertFalse(config.fromBeginning());
        assertEquals("latest", consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    @Test
    public void shouldExitOnInvalidConfigWithAutoOffsetResetAndConflictingFromBeginning() {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--consumer-property", "auto.offset.reset=latest",
            "--from-beginning"
        };
        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void shouldParseConfigsFromFile() throws IOException {
        Map<String, String> configs = new HashMap<>();
        configs.put("request.timeout.ms", "1000");
        configs.put("group.id", "group1");
        File propsFile = ToolsTestUtils.tempPropertiesFile(configs);
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--consumer.config", propsFile.getAbsolutePath()
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);
        assertEquals("1000", config.consumerProps().get("request.timeout.ms"));
        assertEquals("group1", config.consumerProps().get("group.id"));
    }

    @Test
    public void groupIdsProvidedInDifferentPlacesMustMatch() throws IOException {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        // different in all three places
        File propsFile = ToolsTestUtils.tempPropertiesFile(Collections.singletonMap("group.id", "group-from-file"));
        final String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--group", "group-from-arguments",
            "--consumer-property", "group.id=group-from-properties",
            "--consumer.config", propsFile.getAbsolutePath()
        };

        assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(args));

        // the same in all three places
        propsFile = ToolsTestUtils.tempPropertiesFile(Collections.singletonMap("group.id", "test-group"));
        final String[] args1 = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--group", "test-group",
            "--consumer-property", "group.id=test-group",
            "--consumer.config", propsFile.getAbsolutePath()
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args1);
        Properties props = config.consumerProps();
        assertEquals("test-group", props.getProperty("group.id"));

        // different via --consumer-property and --consumer.config
        propsFile = ToolsTestUtils.tempPropertiesFile(Collections.singletonMap("group.id", "group-from-file"));
        final String[] args2 = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--consumer-property", "group.id=group-from-properties",
            "--consumer.config", propsFile.getAbsolutePath()
        };

        assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(args2));

        // different via --consumer-property and --group
        final String[] args3 = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--group", "group-from-arguments",
            "--consumer-property", "group.id=group-from-properties"
        };

        assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(args3));

        // different via --group and --consumer.config
        propsFile = ToolsTestUtils.tempPropertiesFile(Collections.singletonMap("group.id", "group-from-file"));
        final String[] args4 = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--group", "group-from-arguments",
            "--consumer.config", propsFile.getAbsolutePath()
        };
        assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(args4));

        // via --group only
        final String[] args5 = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--group", "group-from-arguments"
        };

        config = new ConsoleConsumerOptions(args5);
        props = config.consumerProps();
        assertEquals("group-from-arguments", props.getProperty("group.id"));

        Exit.resetExitProcedure();
    }

    @Test
    public void testCustomPropertyShouldBePassedToConfigureMethod() throws Exception {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--property", "print.key=true",
            "--property", "key.deserializer=org.apache.kafka.test.MockDeserializer",
            "--property", "key.deserializer.my-props=abc"
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);

        assertInstanceOf(DefaultMessageFormatter.class, config.formatter());
        assertTrue(config.formatterArgs().containsKey("key.deserializer.my-props"));
        DefaultMessageFormatter formatter = (DefaultMessageFormatter) config.formatter();
        assertTrue(formatter.keyDeserializer().isPresent());
        assertInstanceOf(MockDeserializer.class, formatter.keyDeserializer().get());
        MockDeserializer keyDeserializer = (MockDeserializer) formatter.keyDeserializer().get();
        assertEquals(1, keyDeserializer.configs.size());
        assertEquals("abc", keyDeserializer.configs.get("my-props"));
        assertTrue(keyDeserializer.isKey);
    }

    @Test
    public void testCustomConfigShouldBePassedToConfigureMethod() throws Exception {
        Map<String, String> configs = new HashMap<>();
        configs.put("key.deserializer.my-props", "abc");
        configs.put("print.key", "false");
        File propsFile = ToolsTestUtils.tempPropertiesFile(configs);
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--property", "print.key=true",
            "--property", "key.deserializer=org.apache.kafka.test.MockDeserializer",
            "--formatter-config", propsFile.getAbsolutePath()
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);

        assertInstanceOf(DefaultMessageFormatter.class, config.formatter());
        assertTrue(config.formatterArgs().containsKey("key.deserializer.my-props"));
        DefaultMessageFormatter formatter = (DefaultMessageFormatter) config.formatter();
        assertTrue(formatter.keyDeserializer().isPresent());
        assertInstanceOf(MockDeserializer.class, formatter.keyDeserializer().get());
        MockDeserializer keyDeserializer = (MockDeserializer) formatter.keyDeserializer().get();
        assertEquals(1, keyDeserializer.configs.size());
        assertEquals("abc", keyDeserializer.configs.get("my-props"));
        assertTrue(keyDeserializer.isKey);
    }

    @Test
    public void shouldParseGroupIdFromBeginningGivenTogether() throws IOException {
        // Start from earliest
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--group", "test-group",
            "--from-beginning"
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);
        assertEquals("localhost:9092", config.bootstrapServer());
        assertEquals("test", config.topicArg().orElse(""));
        assertEquals(-2, config.offsetArg());
        assertTrue(config.fromBeginning());

        // Start from latest
        args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--group", "test-group"
        };

        config = new ConsoleConsumerOptions(args);
        assertEquals("localhost:9092", config.bootstrapServer());
        assertEquals("test", config.topicArg().orElse(""));
        assertEquals(-1, config.offsetArg());
        assertFalse(config.fromBeginning());
    }

    @Test
    public void shouldExitOnGroupIdAndPartitionGivenTogether() {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--group", "test-group",
            "--partition", "0"
        };

        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void shouldExitOnOffsetWithoutPartition() {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--offset", "10"
        };

        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void shouldExitIfNoTopicOrFilterSpecified() {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092"
        };

        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void shouldExitIfTopicAndIncludeSpecified() {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--include", "includeTest*"
        };

        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testClientIdOverride() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--from-beginning",
            "--consumer-property", "client.id=consumer-1"
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);
        Properties consumerProperties = config.consumerProps();

        assertEquals("consumer-1", consumerProperties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    public void testDefaultClientId() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--from-beginning"
        };

        ConsoleConsumerOptions config = new ConsoleConsumerOptions(args);
        Properties consumerProperties = config.consumerProps();

        assertEquals("console-consumer", consumerProperties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    public void testParseOffset() throws Exception {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        try {
            final String[] badOffset = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--partition", "0",
                "--offset", "bad"
            };
            assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(badOffset));

            final String[] negativeOffset = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--partition", "0",
                "--offset", "-100"
            };
            assertThrows(IllegalArgumentException.class, () -> new ConsoleConsumerOptions(negativeOffset));

            final String[] earliestOffset = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--partition", "0",
                "--offset", "earliest"
            };
            ConsoleConsumerOptions config = new ConsoleConsumerOptions(earliestOffset);
            assertEquals(ListOffsetsRequest.EARLIEST_TIMESTAMP, config.offsetArg());
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testParseTimeoutMs() throws Exception {
        String[] withoutTimeoutMs = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--partition", "0"
        };
        assertEquals(Long.MAX_VALUE, new ConsoleConsumerOptions(withoutTimeoutMs).timeoutMs());

        String[] negativeTimeoutMs = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--partition", "0",
            "--timeout-ms", "-100"
        };
        assertEquals(Long.MAX_VALUE, new ConsoleConsumerOptions(negativeTimeoutMs).timeoutMs());

        String[] validTimeoutMs = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--partition", "0",
            "--timeout-ms", "100"
        };
        assertEquals(100, new ConsoleConsumerOptions(validTimeoutMs).timeoutMs());
    }

    @Test
    public void testParseFormatter() throws Exception {
        String[] defaultMessageFormatter = generateArgsForFormatter("org.apache.kafka.tools.consumer.DefaultMessageFormatter");
        assertInstanceOf(DefaultMessageFormatter.class, new ConsoleConsumerOptions(defaultMessageFormatter).formatter());

        String[] loggingMessageFormatter = generateArgsForFormatter("org.apache.kafka.tools.consumer.LoggingMessageFormatter");
        assertInstanceOf(LoggingMessageFormatter.class, new ConsoleConsumerOptions(loggingMessageFormatter).formatter());

        String[] noOpMessageFormatter = generateArgsForFormatter("org.apache.kafka.tools.consumer.NoOpMessageFormatter");
        assertInstanceOf(NoOpMessageFormatter.class, new ConsoleConsumerOptions(noOpMessageFormatter).formatter());
    }
    
    private String[] generateArgsForFormatter(String formatter) {
        return new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--partition", "0",
            "--formatter", formatter,
        };
    }
}
