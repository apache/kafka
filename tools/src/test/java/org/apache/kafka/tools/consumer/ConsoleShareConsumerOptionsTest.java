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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.MockDeserializer;
import org.apache.kafka.tools.ToolsTestUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsoleShareConsumerOptionsTest {

    @Test
    public void shouldParseValidConsumerValidConfig() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
        };

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);

        assertEquals("localhost:9092", config.bootstrapServer());
        assertEquals("test", config.topicArg());
        assertFalse(config.rejectMessageOnError());
        assertEquals(-1, config.maxMessages());
        assertEquals(-1, config.timeoutMs());
    }

    @Test
    public void shouldExitOnUnrecognizedNewConsumerOption() {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        String[] args = new String[]{
            "--new-consumer",
            "--bootstrap-server", "localhost:9092",
            "--topic", "test"
        };

        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleShareConsumerOptions(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void shouldParseValidConsumerConfigWithSessionTimeoutDeprecated() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--consumer-property", "session.timeout.ms=10000"
        };

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);
        Properties consumerProperties = config.consumerProps();

        assertEquals("localhost:9092", config.bootstrapServer());
        assertEquals("test", config.topicArg());
        assertEquals("10000", consumerProperties.getProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG));
    }

    @Test
    public void shouldParseConfigsFromFileDeprecated() throws IOException {
        Map<String, String> configs = new HashMap<>();
        configs.put("request.timeout.ms", "1000");
        configs.put("group.id", "group1");
        File propsFile = ToolsTestUtils.tempPropertiesFile(configs);
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--consumer-config", propsFile.getAbsolutePath()
        };

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);

        // KafkaShareConsumer uses Utils.propsToMap to convert the properties to a map,
        // so using the same method to check the map has the expected values
        Map<String, Object> configMap = Utils.propsToMap(config.consumerProps());
        assertEquals("1000", configMap.get("request.timeout.ms"));
        assertEquals("group1", configMap.get("group.id"));
    }

    @Test
    public void groupIdsProvidedInDifferentPlacesMustMatchDeprecated() throws IOException {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        try {
            // different in all three places
            File propsFile = ToolsTestUtils.tempPropertiesFile(Map.of("group.id", "group-from-file"));
            final String[] args = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--group", "group-from-arguments",
                "--consumer-property", "group.id=group-from-properties",
                "--consumer-config", propsFile.getAbsolutePath()
            };

            assertThrows(IllegalArgumentException.class, () -> new ConsoleShareConsumerOptions(args));

            // the same in all three places
            propsFile = ToolsTestUtils.tempPropertiesFile(Map.of("group.id", "test-group"));
            final String[] args1 = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--group", "test-group",
                "--consumer-property", "group.id=test-group",
                "--consumer-config", propsFile.getAbsolutePath()
            };

            ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args1);
            Properties props = config.consumerProps();
            assertEquals("test-group", props.getProperty("group.id"));

            // different via --consumer-property and --consumer-config
            propsFile = ToolsTestUtils.tempPropertiesFile(Map.of("group.id", "group-from-file"));
            final String[] args2 = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--consumer-property", "group.id=group-from-properties",
                "--consumer-config", propsFile.getAbsolutePath()
            };

            assertThrows(IllegalArgumentException.class, () -> new ConsoleShareConsumerOptions(args2));

            // different via --consumer-property and --group
            final String[] args3 = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--group", "group-from-arguments",
                "--consumer-property", "group.id=group-from-properties"
            };

            assertThrows(IllegalArgumentException.class, () -> new ConsoleShareConsumerOptions(args3));

            // different via --group and --consumer-config
            propsFile = ToolsTestUtils.tempPropertiesFile(Map.of("group.id", "group-from-file"));
            final String[] args4 = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--group", "group-from-arguments",
                "--consumer-config", propsFile.getAbsolutePath()
            };
            assertThrows(IllegalArgumentException.class, () -> new ConsoleShareConsumerOptions(args4));

            // via --group only
            final String[] args5 = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--group", "group-from-arguments"
            };

            config = new ConsoleShareConsumerOptions(args5);
            props = config.consumerProps();
            assertEquals("group-from-arguments", props.getProperty("group.id"));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void shouldExitIfNoTopicSpecified() {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092"
        };

        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleShareConsumerOptions(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testClientIdOverrideDeprecated() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--consumer-property", "client.id=consumer-1"
        };

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);
        Properties consumerProperties = config.consumerProps();

        assertEquals("consumer-1", consumerProperties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    public void testCustomPropertyShouldBePassedToConfigureMethodDeprecated() throws Exception {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--property", "print.key=true",
            "--property", "key.deserializer=org.apache.kafka.test.MockDeserializer",
            "--property", "key.deserializer.my-props=abc"
        };

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);

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
    public void testCustomPropertyShouldBePassedToConfigureMethod() throws Exception {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--formatter-property", "print.key=true",
            "--formatter-property", "key.deserializer=org.apache.kafka.test.MockDeserializer",
            "--formatter-property", "key.deserializer.my-props=abc"
        };

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);

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
    public void testCustomConfigShouldBePassedToConfigureMethodDeprecated() throws Exception {
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

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);

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
            "--formatter-property", "print.key=true",
            "--formatter-property", "key.deserializer=org.apache.kafka.test.MockDeserializer",
            "--formatter-config", propsFile.getAbsolutePath()
        };

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);

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
    public void testDefaultClientId() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
        };

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);
        Properties consumerProperties = config.consumerProps();

        assertEquals("console-share-consumer", consumerProperties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    public void testRejectOption() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--reject"
        };

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);
        assertEquals(AcknowledgeType.REJECT, config.acknowledgeType());
    }

    @Test
    public void testReleaseOption() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--release"
        };

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);
        assertEquals(AcknowledgeType.RELEASE, config.acknowledgeType());
    }

    @Test
    public void testRejectAndReleaseOption() throws IOException {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--reject",
            "--release"
        };

        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleShareConsumerOptions(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void shouldExitOnBothConsumerPropertyAndCommandProperty() {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--consumer-property", "session.timeout.ms=10000",
            "--command-property", "request.timeout.ms=30000"
        };

        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleShareConsumerOptions(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void shouldExitOnBothConsumerConfigAndCommandConfig() throws IOException {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        Map<String, String> configs = new HashMap<>();
        configs.put("request.timeout.ms", "1000");
        File propsFile = ToolsTestUtils.tempPropertiesFile(configs);

        Map<String, String> configs2 = new HashMap<>();
        configs2.put("session.timeout.ms", "10000");
        File propsFile2 = ToolsTestUtils.tempPropertiesFile(configs2);

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--consumer-config", propsFile.getAbsolutePath(),
            "--command-config", propsFile2.getAbsolutePath()
        };

        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleShareConsumerOptions(args));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void shouldParseValidConsumerConfigWithSessionTimeout() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--command-property", "session.timeout.ms=10000"
        };

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);
        Properties consumerProperties = config.consumerProps();

        assertEquals("localhost:9092", config.bootstrapServer());
        assertEquals("test", config.topicArg());
        assertEquals("10000", consumerProperties.getProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG));
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
            "--command-config", propsFile.getAbsolutePath()
        };

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);

        // KafkaShareConsumer uses Utils.propsToMap to convert the properties to a map,
        // so using the same method to check the map has the expected values
        Map<String, Object> configMap = Utils.propsToMap(config.consumerProps());
        assertEquals("1000", configMap.get("request.timeout.ms"));
        assertEquals("group1", configMap.get("group.id"));
    }

    @Test
    public void groupIdsProvidedInDifferentPlacesMustMatch() throws IOException {
        Exit.setExitProcedure((code, message) -> {
            throw new IllegalArgumentException(message);
        });

        try {
            // different in all three places
            File propsFile = ToolsTestUtils.tempPropertiesFile(Map.of("group.id", "group-from-file"));
            final String[] args = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--group", "group-from-arguments",
                "--command-property", "group.id=group-from-properties",
                "--command-config", propsFile.getAbsolutePath()
            };

            assertThrows(IllegalArgumentException.class, () -> new ConsoleShareConsumerOptions(args));

            // the same in all three places
            propsFile = ToolsTestUtils.tempPropertiesFile(Map.of("group.id", "test-group"));
            final String[] args1 = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--group", "test-group",
                "--command-property", "group.id=test-group",
                "--command-config", propsFile.getAbsolutePath()
            };

            ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args1);
            Properties props = config.consumerProps();
            assertEquals("test-group", props.getProperty("group.id"));

            // different via --command-property and --command-config
            propsFile = ToolsTestUtils.tempPropertiesFile(Map.of("group.id", "group-from-file"));
            final String[] args2 = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--command-property", "group.id=group-from-properties",
                "--command-config", propsFile.getAbsolutePath()
            };

            assertThrows(IllegalArgumentException.class, () -> new ConsoleShareConsumerOptions(args2));

            // different via --command-property and --group
            final String[] args3 = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--group", "group-from-arguments",
                "--command-property", "group.id=group-from-properties"
            };

            assertThrows(IllegalArgumentException.class, () -> new ConsoleShareConsumerOptions(args3));

            // different via --group and --command-config
            propsFile = ToolsTestUtils.tempPropertiesFile(Map.of("group.id", "group-from-file"));
            final String[] args4 = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--group", "group-from-arguments",
                "--command-config", propsFile.getAbsolutePath()
            };
            assertThrows(IllegalArgumentException.class, () -> new ConsoleShareConsumerOptions(args4));

            // via --group only
            final String[] args5 = new String[]{
                "--bootstrap-server", "localhost:9092",
                "--topic", "test",
                "--group", "group-from-arguments"
            };

            config = new ConsoleShareConsumerOptions(args5);
            props = config.consumerProps();
            assertEquals("group-from-arguments", props.getProperty("group.id"));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testClientIdOverride() throws IOException {
        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--command-property", "client.id=consumer-1"
        };

        ConsoleShareConsumerOptions config = new ConsoleShareConsumerOptions(args);
        Properties consumerProperties = config.consumerProps();

        assertEquals("consumer-1", consumerProperties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG));
    }
}
