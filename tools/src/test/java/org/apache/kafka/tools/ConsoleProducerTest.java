package org.apache.kafka.tools;

import kafka.tools.ConsoleProducer.LineMessageReader;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ConsoleProducer.ConsoleProducerConfig;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;

import static java.util.Arrays.asList;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsoleProducerTest {
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
    private void testValidConfigsBrokerList() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BROKER_LIST_VALID_ARGS);
        ProducerConfig consoleProducerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(asList("localhost:1001", "localhost:1002"),
                consoleProducerConfig.getList(BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    private void testValidConfigsBootstrapServer() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BOOTSTRAP_SERVER_VALID_ARGS);
        ProducerConfig consoleProducerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(asList("localhost:1003", "localhost:1004"),
                consoleProducerConfig.getList(BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    private void testInvalidConfigs() {
        Exit.setExitProcedure((statusCode, message) -> { throw new IllegalArgumentException(message); });
        try {
            assertThrows(IllegalArgumentException.class, () -> new ConsoleProducerConfig(INVALID_ARGS));
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    private void testParseKeyProp() throws Exception {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BROKER_LIST_VALID_ARGS);
        LineMessageReader reader = (LineMessageReader) Class.forName(config.readerClass()).getDeclaredConstructor().newInstance();
        reader.init(System.in, config.getReaderProps());

        assertEquals("#", reader.keySeparator());
        assertTrue(reader.parseKey());
    }

    @Test
    private void testParseReaderConfigFile() throws Exception {
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
    private void  testBootstrapServerOverride() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BOOTSTRAP_SERVER_OVERRIDE);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(asList("localhost:1002"), producerConfig.getList(BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    private void testClientIdOverride() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(CLIENT_ID_OVERRIDE);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals("producer-1", producerConfig.getString(CLIENT_ID_CONFIG));
    }

    @Test
    private void testDefaultClientId() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BROKER_LIST_VALID_ARGS);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals("console-producer", producerConfig.getString(CLIENT_ID_CONFIG));
    }

    @Test
    private void testBatchSizeOverriddenByMaxPartitionMemoryBytesValue() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BATCH_SIZE_OVERRIDDEN_BY_MAX_PARTITION_MEMORY_BYTES_VALUE);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(456, producerConfig.getInt(BATCH_SIZE_CONFIG));
    }

    @Test
    private void testBatchSizeSetAndMaxPartitionMemoryBytesNotSet() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BATCH_SIZE_SET_AND_MAX_PARTITION_MEMORY_BYTES_NOT_SET);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(123, producerConfig.getInt(BATCH_SIZE_CONFIG));
    }

    @Test
    private void testDefaultBatchSize() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BATCH_SIZE_DEFAULT);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(16*1024, producerConfig.getInt(BATCH_SIZE_CONFIG));
    }

    @Test
    private void testBatchSizeNotSetAndMaxPartitionMemoryBytesSet() throws IOException {
        ConsoleProducerConfig config = new ConsoleProducerConfig(BATCH_SIZE_NOT_SET_AND_MAX_PARTITION_MEMORY_BYTES_SET);
        ProducerConfig producerConfig = new ProducerConfig(config.getProducerProps());

        assertEquals(456, producerConfig.getInt(BATCH_SIZE_CONFIG));
    }
}
