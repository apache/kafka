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

import kafka.utils.TestUtils;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.tools.ConsoleProducer.ConsoleProducerOptions;
import org.apache.kafka.tools.api.RecordReader;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsoleProducerTest {
    private static final String[] BOOTSTRAP_SERVER_VALID_ARGS = new String[]{
        "--bootstrap-server", "localhost:1003,localhost:1004",
        "--topic", "t3",
        "--property", "parse.key=true",
        "--property", "key.separator=#"
    };
    private static final String[] INVALID_ARGS = new String[]{
        "--t", // not a valid argument
        "t3"
    };
    private static final String[] BOOTSTRAP_SERVER_OVERRIDE = new String[]{
        "--bootstrap-server", "localhost:1002",
        "--topic", "t3",
    };
    private static final String[] CLIENT_ID_OVERRIDE = new String[]{
        "--bootstrap-server", "localhost:1001",
        "--topic", "t3",
        "--producer-property", "client.id=producer-1"
    };
    private static final String[] BATCH_SIZE_OVERRIDDEN_BY_MAX_PARTITION_MEMORY_BYTES_VALUE = new String[]{
        "--bootstrap-server", "localhost:1002",
        "--topic", "t3",
        "--batch-size", "123",
        "--max-partition-memory-bytes", "456"
    };
    private static final String[] BATCH_SIZE_SET_AND_MAX_PARTITION_MEMORY_BYTES_NOT_SET = new String[]{
        "--bootstrap-server", "localhost:1002",
        "--topic", "t3",
        "--batch-size", "123"
    };
    private static final String[] BATCH_SIZE_NOT_SET_AND_MAX_PARTITION_MEMORY_BYTES_SET = new String[]{
        "--bootstrap-server", "localhost:1002",
        "--topic", "t3",
        "--max-partition-memory-bytes", "456"
    };
    private static final String[] BATCH_SIZE_DEFAULT = new String[]{
        "--bootstrap-server", "localhost:1002",
        "--topic", "t3"
    };
    private static final String[] TEST_RECORD_READER = new String[]{
        "--bootstrap-server", "localhost:1002",
        "--topic", "t3",
        "--line-reader", TestRecordReader.class.getName()
    };

    @Test
    public void testValidConfigsBootstrapServer() throws IOException {
        ConsoleProducerOptions opts = new ConsoleProducerOptions(BOOTSTRAP_SERVER_VALID_ARGS);
        ProducerConfig producerConfig = new ProducerConfig(opts.producerProps());

        assertEquals(asList("localhost:1003", "localhost:1004"), producerConfig.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testInvalidConfigs() {
        Exit.setExitProcedure((statusCode, message) -> {
            throw new IllegalArgumentException(message);
        });
        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleProducerOptions(INVALID_ARGS));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testParseKeyProp() throws ReflectiveOperationException, IOException {
        ConsoleProducerOptions opts = new ConsoleProducerOptions(BOOTSTRAP_SERVER_VALID_ARGS);
        LineMessageReader reader = (LineMessageReader) Class.forName(opts.readerClass()).getDeclaredConstructor().newInstance();
        reader.configure(opts.readerProps());

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

        String[] args = new String[]{
            "--bootstrap-server", "localhost:9092",
            "--topic", "test",
            "--property", "key.separator=;",
            "--property", "parse.headers=true",
            "--reader-config", propsFile.getAbsolutePath()
        };
        ConsoleProducerOptions opts = new ConsoleProducerOptions(args);
        LineMessageReader reader = (LineMessageReader) Class.forName(opts.readerClass()).getDeclaredConstructor().newInstance();
        reader.configure(opts.readerProps());

        assertEquals(";", reader.keySeparator());
        assertTrue(reader.parseKey());
        assertTrue(reader.parseHeaders());
    }

    @Test
    public void testBootstrapServerOverride() throws IOException {
        ConsoleProducerOptions opts = new ConsoleProducerOptions(BOOTSTRAP_SERVER_OVERRIDE);
        ProducerConfig producerConfig = new ProducerConfig(opts.producerProps());

        assertEquals(Collections.singletonList("localhost:1002"), producerConfig.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testClientIdOverride() throws IOException {
        ConsoleProducerOptions opts = new ConsoleProducerOptions(CLIENT_ID_OVERRIDE);
        ProducerConfig producerConfig = new ProducerConfig(opts.producerProps());

        assertEquals("producer-1", producerConfig.getString(ProducerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    public void testDefaultClientId() throws IOException {
        ConsoleProducerOptions opts = new ConsoleProducerOptions(BOOTSTRAP_SERVER_VALID_ARGS);
        ProducerConfig producerConfig = new ProducerConfig(opts.producerProps());

        assertEquals("console-producer", producerConfig.getString(ProducerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    public void testBatchSizeOverriddenByMaxPartitionMemoryBytesValue() throws IOException {
        ConsoleProducerOptions opts = new ConsoleProducerOptions(BATCH_SIZE_OVERRIDDEN_BY_MAX_PARTITION_MEMORY_BYTES_VALUE);
        ProducerConfig producerConfig = new ProducerConfig(opts.producerProps());

        assertEquals(456, producerConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG));
    }

    @Test
    public void testBatchSizeSetAndMaxPartitionMemoryBytesNotSet() throws IOException {
        ConsoleProducerOptions opts = new ConsoleProducerOptions(BATCH_SIZE_SET_AND_MAX_PARTITION_MEMORY_BYTES_NOT_SET);
        ProducerConfig producerConfig = new ProducerConfig(opts.producerProps());

        assertEquals(123, producerConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG));
    }

    @Test
    public void testDefaultBatchSize() throws IOException {
        ConsoleProducerOptions opts = new ConsoleProducerOptions(BATCH_SIZE_DEFAULT);
        ProducerConfig producerConfig = new ProducerConfig(opts.producerProps());

        assertEquals(16 * 1024, producerConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG));
    }

    @Test
    public void testBatchSizeNotSetAndMaxPartitionMemoryBytesSet() throws IOException {
        ConsoleProducerOptions opts = new ConsoleProducerOptions(BATCH_SIZE_NOT_SET_AND_MAX_PARTITION_MEMORY_BYTES_SET);
        ProducerConfig producerConfig = new ProducerConfig(opts.producerProps());

        assertEquals(456, producerConfig.getInt(ProducerConfig.BATCH_SIZE_CONFIG));
    }

    @Test
    public void testNewReader() throws Exception {
        ConsoleProducer producer = new ConsoleProducer();
        TestRecordReader reader = (TestRecordReader) producer.messageReader(new ConsoleProducerOptions(TEST_RECORD_READER));

        assertEquals(1, reader.configureCount());
        assertEquals(0, reader.closeCount());

        reader.close();
        assertEquals(1, reader.closeCount());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLoopReader() throws Exception {
        ConsoleProducer producer = new ConsoleProducer();
        TestRecordReader reader = (TestRecordReader) producer.messageReader(new ConsoleProducerOptions(TEST_RECORD_READER));

        producer.loopReader(Mockito.mock(Producer.class), reader, false);

        assertEquals(1, reader.configureCount());
        assertEquals(1, reader.closeCount());
    }

    public static class TestRecordReader implements RecordReader {
        private int configureCount = 0;
        private int closeCount = 0;

        @Override
        public void configure(Map<String, ?> configs) {
            configureCount += 1;
        }

        @Override
        public Iterator<ProducerRecord<byte[], byte[]>> readRecords(InputStream inputStream) {
            return Collections.emptyIterator();
        }

        @Override
        public void close() {
            closeCount += 1;
        }

        public int configureCount() {
            return configureCount;
        }

        public int closeCount() {
            return closeCount;
        }
    }
}
