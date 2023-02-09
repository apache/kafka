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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ConsoleProducer.ConsoleProducerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.Future;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ConsoleProducerTest {
    @Mock
    KafkaProducer<byte[], byte[]> producerMock;
    @Mock
    MessageReader readerMock;
    @Spy
    ConsoleProducer consoleProducerSpy;

    private static final String[] BROKER_LIST_VALID_ARGS = new String[] {
        "--broker-list",
        "localhost:1001,localhost:1002",
        "--topic",
        "t3",
        "--property",
        "parse.key=true",
        "--property",
        "key.separator=#"
    };

    private static final String[] BOOTSTRAP_SERVER_VALID_ARGS = new String[] {
        "--bootstrap-server",
        "localhost:1003,localhost:1004",
        "--topic",
        "t3",
        "--property",
        "parse.key=true",
        "--property",
        "key.separator=#"
    };

    private static final String[] INVALID_ARGS = new String[] {
        "--t", // not a valid argument
        "t3"
    };

    private static final String[] BOOTSTRAP_SERVER_OVERRIDE = new String[] {
        "--broker-list",
        "localhost:1001",
        "--bootstrap-server",
        "localhost:1002",
        "--topic",
        "t3"
    };

    private static final String[] CLIENT_ID_OVERRIDE = new String[] {
        "--broker-list",
        "localhost:1001",
        "--topic",
        "t3",
        "--producer-property",
        "client.id=producer-1"
    };

    private static final String[] BATCH_SIZE_OVERRIDDEN_BY_MAX_PARTITION_MEMORY_BYTES_VALUE = new String[] {
        "--broker-list",
        "localhost:1001",
        "--bootstrap-server",
        "localhost:1002",
        "--topic",
        "t3",
        "--batch-size",
        "123",
        "--max-partition-memory-bytes",
        "456"
    };

    private static final String[] BATCH_SIZE_SET_AND_MAX_PARTITION_MEMORY_BYTES_NOT_SET = new String[] {
        "--broker-list",
        "localhost:1001",
        "--bootstrap-server",
        "localhost:1002",
        "--topic",
        "t3",
        "--batch-size",
        "123"
    };

    private static final String[] BATCH_SIZE_NOT_SET_AND_MAX_PARTITION_MEMORY_BYTES_SET = new String[] {
        "--broker-list",
        "localhost:1001",
        "--bootstrap-server",
        "localhost:1002",
        "--topic",
        "t3",
        "--max-partition-memory-bytes",
        "456"
    };

    private static final String[] BATCH_SIZE_DEFAULT = new String[] {
        "--broker-list",
        "localhost:1001",
        "--bootstrap-server",
        "localhost:1002",
        "--topic",
        "t3"
    };

    @Test
    public void testValidConfigsBrokerList() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BROKER_LIST_VALID_ARGS);
        ProducerConfig consoleProducerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(asList("localhost:1001", "localhost:1002"),
                consoleProducerConfig.getList(BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testValidConfigsBootstrapServer() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BOOTSTRAP_SERVER_VALID_ARGS);
        ProducerConfig consoleProducerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(asList("localhost:1003", "localhost:1004"),
                consoleProducerConfig.getList(BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testInvalidConfigs() {
        Exit.setExitProcedure((statusCode, message) -> {
            throw new IllegalArgumentException(message);
        });
        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleProducerConfig(INVALID_ARGS));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testParseKeyProp() throws Exception {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BROKER_LIST_VALID_ARGS);
        LineMessageReader reader = (LineMessageReader) Class.forName(config.readerClass()).getDeclaredConstructor().newInstance();
        reader.init(System.in, config.getReaderProps());

        assertEquals("#", reader.keySeparator());
        assertTrue(reader.parseKey());
    }

    @Test
    public void testParseReaderConfigFile() throws Exception {
        File propsFile = TestUtils.tempFile();
        OutputStream propsStream = Files.newOutputStream(propsFile.toPath());
        propsStream.write("parse.key=true\n".getBytes());
        propsStream.write("key.separator=|".getBytes());
        propsStream.close();

        String[]  args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--property", "key.separator=;",
            "--property", "parse.headers=true",
            "--reader-config", propsFile.getAbsolutePath()
        };

        ConsoleProducerConfig config = new ConsoleProducerConfig(args);
        LineMessageReader reader = (LineMessageReader) Class.forName(config.readerClass()).getDeclaredConstructor().newInstance();
        reader.init(System.in, config.getReaderProps());

        assertEquals(";", reader.keySeparator());
        assertTrue(reader.parseKey());
        assertTrue(reader.parseHeaders());
    }

    @Test
    public void testBootstrapServerOverride() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BOOTSTRAP_SERVER_OVERRIDE);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(asList("localhost:1002"), producerConfig.getList(BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testClientIdOverride() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(CLIENT_ID_OVERRIDE);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals("producer-1", producerConfig.getString(CLIENT_ID_CONFIG));
    }

    @Test
    public void testDefaultClientId() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BROKER_LIST_VALID_ARGS);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals("console-producer", producerConfig.getString(CLIENT_ID_CONFIG));
    }

    @Test
    public void testBatchSizeOverriddenByMaxPartitionMemoryBytesValue() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BATCH_SIZE_OVERRIDDEN_BY_MAX_PARTITION_MEMORY_BYTES_VALUE);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(456, producerConfig.getInt(BATCH_SIZE_CONFIG));
    }

    @Test
    public void testBatchSizeSetAndMaxPartitionMemoryBytesNotSet() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BATCH_SIZE_SET_AND_MAX_PARTITION_MEMORY_BYTES_NOT_SET);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(123, producerConfig.getInt(BATCH_SIZE_CONFIG));
    }

    @Test
    public void testDefaultBatchSize() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BATCH_SIZE_DEFAULT);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(16 * 1024, producerConfig.getInt(BATCH_SIZE_CONFIG));
    }

    @Test
    public void testBatchSizeNotSetAndMaxPartitionMemoryBytesSet() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BATCH_SIZE_NOT_SET_AND_MAX_PARTITION_MEMORY_BYTES_SET);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(456, producerConfig.getInt(BATCH_SIZE_CONFIG));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void testRecordsFromTheReaderAreSentByTheProducer(boolean sync) throws Exception {
        Exit.setExitProcedure((statusCode, message) -> {
            if (statusCode != 0) {
                throw new AssertionError(message);
            }
        });

        try {
            String topic = "p1nKFl0yD";
            byte[] one = "1".getBytes(UTF_8), two = "2".getBytes(UTF_8), three = "3".getBytes(UTF_8);
            ProducerRecord<byte[], byte[]>
                    r1 = new ProducerRecord<>(topic, one, one),
                    r2 = new ProducerRecord<>(topic, two, two),
                    r3 = new ProducerRecord<>(topic, three, three);

            Future<RecordMetadata> producerResponse = completedFuture(null);

            if (sync) {
                doReturn(producerResponse).when(producerMock).send(any());

            } else {
                doReturn(producerResponse).when(producerMock).send(any(), any());
            }

            doReturn(readerMock).when(consoleProducerSpy).createMessageReader(any(ConsoleProducerConfig.class));
            doReturn(producerMock).when(consoleProducerSpy).createKafkaProducer(any(Properties.class));
            when(readerMock.readMessage())
                    .thenReturn(r1)
                    .thenReturn(r2)
                    .thenReturn(r3)
                    .thenReturn(null);

            String[] args = new String[] {
                "--bootstrap-server", "localhost:9092",
                "--topic", topic,
                sync ? "--sync" : ""
            };

            consoleProducerSpy.start(args);

            if (sync) {
                verify(producerMock).send(eq(r1));
                verify(producerMock).send(eq(r2));
                verify(producerMock).send(eq(r3));

            } else {
                verify(producerMock).send(eq(r1), any(Callback.class));
                verify(producerMock).send(eq(r2), any(Callback.class));
                verify(producerMock).send(eq(r3), any(Callback.class));
            }
        } finally {
            Exit.resetExitProcedure();
        }
    }
}
