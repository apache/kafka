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
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.utils.Utils;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class ProducerPerformanceTest {

    @Mock
    KafkaProducer<byte[], byte[]> producerMock;

    @Spy
    ProducerPerformance producerPerformanceSpy;

    private File createTempFile(String contents) throws IOException {
        File file = File.createTempFile("ProducerPerformanceTest", ".tmp");
        file.deleteOnExit();
        Files.write(file.toPath(), contents.getBytes());
        return file;
    }

    @Test
    public void testReadPayloadFile() throws Exception {
        File payloadFile = createTempFile("Hello\nKafka");
        String payloadFilePath = payloadFile.getAbsolutePath();
        String payloadDelimiter = "\n";

        List<byte[]> payloadByteList = ProducerPerformance.readPayloadFile(payloadFilePath, payloadDelimiter);

        assertEquals(2, payloadByteList.size());
        assertEquals("Hello", new String(payloadByteList.get(0)));
        assertEquals("Kafka", new String(payloadByteList.get(1)));
        Utils.delete(payloadFile);
    }

    @Test
    public void testReadProps() throws Exception {
        List<String> producerProps = Collections.singletonList("bootstrap.servers=localhost:9000");
        File producerConfig = createTempFile("acks=1");

        Properties prop = ProducerPerformance.readProps(producerProps, producerConfig.getAbsolutePath());

        assertNotNull(prop);
        assertEquals(5, prop.size());
        Utils.delete(producerConfig);
    }

    @Test
    public void testNumberOfCallsForSendAndClose() throws IOException {
        doReturn(null).when(producerMock).send(any(), any());
        doReturn(producerMock).when(producerPerformanceSpy).createKafkaProducer(any(Properties.class));

        String[] args = new String[] {
            "--topic", "Hello-Kafka", 
            "--num-records", "5", 
            "--throughput", "100", 
            "--record-size", "100", 
            "--producer-props", "bootstrap.servers=localhost:9000"};
        producerPerformanceSpy.start(args);
        verify(producerMock, times(5)).send(any(), any());
        verify(producerMock, times(1)).close();
    }

    @Test
    public void testEnableTransaction() throws IOException {
        doReturn(null).when(producerMock).send(any(), any());
        doReturn(producerMock).when(producerPerformanceSpy).createKafkaProducer(any(Properties.class));

        String[] args = new String[] {
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "100",
            "--record-size", "100",
            "--transactional-id", "foobar",
            "--producer-props", "bootstrap.servers=localhost:9000"};
        producerPerformanceSpy.start(args);
        verify(producerMock, times(1)).beginTransaction();
        verify(producerMock, times(1)).commitTransaction();
        verify(producerMock, times(1)).close();
    }

    @Test
    public void testNumberOfSuccessfulSendAndClose() throws IOException {
        doReturn(producerMock).when(producerPerformanceSpy).createKafkaProducer(any(Properties.class));
        doAnswer(invocation -> {
            producerPerformanceSpy.cb.onCompletion(null, null);
            return null;
        }).when(producerMock).send(any(), any());

        String[] args = new String[] {
            "--topic", "Hello-Kafka",
            "--num-records", "10",
            "--throughput", "1",
            "--record-size", "100",
            "--producer-props", "bootstrap.servers=localhost:9000"};
        producerPerformanceSpy.start(args);

        verify(producerMock, times(10)).send(any(), any());
        assertEquals(10, producerPerformanceSpy.stats.totalCount());
        verify(producerMock, times(1)).close();
    }

    @Test
    public void testNumberOfFailedSendAndClose() throws IOException {
        doReturn(producerMock).when(producerPerformanceSpy).createKafkaProducer(any(Properties.class));
        doAnswer(invocation -> {
            producerPerformanceSpy.cb.onCompletion(null, new AuthorizationException("not authorized."));
            return null;
        }).when(producerMock).send(any(), any());

        String[] args = new String[] {
            "--topic", "Hello-Kafka",
            "--num-records", "10",
            "--throughput", "1",
            "--record-size", "100",
            "--producer-props", "bootstrap.servers=localhost:9000"};
        producerPerformanceSpy.start(args);

        verify(producerMock, times(10)).send(any(), any());
        assertEquals(0, producerPerformanceSpy.stats.currentWindowCount());
        assertEquals(0, producerPerformanceSpy.stats.totalCount());
        verify(producerMock, times(1)).close();
    }

    @Test
    public void testMutuallyExclusiveGroup() {
        String[] args1 = new String[] {
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "100",
            "--record-size", "100",
            "--payload-monotonic",
            "--producer-props", "bootstrap.servers=localhost:9000"};
        ArgumentParser parser1 = ProducerPerformance.argParser();
        ArgumentParserException thrown = assertThrows(ArgumentParserException.class, () ->  parser1.parseArgs(args1));
        assertEquals("argument --payload-monotonic: not allowed with argument --record-size", thrown.getMessage());

        String[] args2 = new String[] {
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "100",
            "--payload-file",  "abc.txt",
            "--payload-monotonic",
            "--producer-props", "bootstrap.servers=localhost:9000"};
        ArgumentParser parser2 = ProducerPerformance.argParser();
        thrown = assertThrows(ArgumentParserException.class, () -> parser2.parseArgs(args2));
        assertEquals("argument --payload-monotonic: not allowed with argument --payload-file", thrown.getMessage());
    }

    @Test
    public void testUnexpectedArg() {
        String[] args = new String[] {
            "--test", "test", 
            "--topic", "Hello-Kafka", 
            "--num-records", "5", 
            "--throughput", "100", 
            "--record-size", "100", 
            "--producer-props", "bootstrap.servers=localhost:9000"};
        ArgumentParser parser = ProducerPerformance.argParser();
        ArgumentParserException thrown = assertThrows(ArgumentParserException.class, () -> parser.parseArgs(args));
        assertEquals("unrecognized arguments: '--test'", thrown.getMessage());
    }

    @Test
    public void testFractionalThroughput() {
        String[] args = new String[] {
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "1.25",
            "--record-size", "100",
            "--producer-props", "bootstrap.servers=localhost:9000"};
        ArgumentParser parser = ProducerPerformance.argParser();
        assertDoesNotThrow(() -> parser.parseArgs(args));
    }

    @Test
    public void testGenerateRandomPayloadByPayloadFile() {
        Integer recordSize = null;
        String inputString = "Hello Kafka";
        byte[] byteArray = inputString.getBytes(StandardCharsets.UTF_8);
        List<byte[]> payloadByteList = new ArrayList<>();
        payloadByteList.add(byteArray);
        byte[] payload = null;
        SplittableRandom random = new SplittableRandom(0);

        payload = ProducerPerformance.generateRandomPayload(recordSize, payloadByteList, payload, random, false, 0L);
        assertEquals(inputString, new String(payload));
    }

    @Test
    public void testGenerateRandomPayloadByRecordSize() {
        Integer recordSize = 100;
        byte[] payload = new byte[recordSize];
        List<byte[]> payloadByteList = new ArrayList<>();
        SplittableRandom random = new SplittableRandom(0);

        payload = ProducerPerformance.generateRandomPayload(recordSize, payloadByteList, payload, random, false, 0L);
        for (byte b : payload) {
            assertNotEquals(0, b);
        }
    }

    @Test
    public void testGenerateMonotonicPayload() {
        byte[] payload = null;
        List<byte[]> payloadByteList = new ArrayList<>();
        SplittableRandom random = new SplittableRandom(0);

        for (int i = 0; i < 10; i++) {
            payload = ProducerPerformance.generateRandomPayload(null, payloadByteList, payload, random, true, i);
            assertEquals(1, payload.length);
            assertEquals(i + '0', payload[0]);
        }
    }

    @Test
    public void testGenerateRandomPayloadException() {
        Integer recordSize = null;
        byte[] payload = null;
        List<byte[]> payloadByteList = new ArrayList<>();
        SplittableRandom random = new SplittableRandom(0);

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> ProducerPerformance.generateRandomPayload(recordSize, payloadByteList, payload, random, false, 0L));
        assertEquals("no payload File Path or record Size or payload-monotonic option provided", thrown.getMessage());
    }

    @Test
    public void testClientIdOverride()  throws Exception {
        List<String> producerProps = Collections.singletonList("client.id=producer-1");

        Properties prop = ProducerPerformance.readProps(producerProps, null);

        assertNotNull(prop);
        assertEquals("producer-1", prop.getProperty("client.id"));
    }

    @Test
    public void testDefaultClientId() throws Exception {
        List<String> producerProps = Collections.singletonList("acks=1");

        Properties prop = ProducerPerformance.readProps(producerProps, null);

        assertNotNull(prop);
        assertEquals("perf-producer-client", prop.getProperty("client.id"));
    }

    @Test
    public void testStatsInitializationWithLargeNumRecords() {
        long numRecords = Long.MAX_VALUE;
        assertDoesNotThrow(() -> new ProducerPerformance.Stats(numRecords, 5000));
    }

    @Test
    public void testStatsCorrectness() throws Exception {
        ExecutorService singleThreaded = Executors.newSingleThreadExecutor();
        final long numRecords = 1000000;
        ProducerPerformance.Stats stats = new ProducerPerformance.Stats(numRecords, 5000);
        for (long i = 0; i < numRecords; i++) {
            final Callback callback = new ProducerPerformance.PerfCallback(0, 100, stats);
            CompletableFuture.runAsync(() -> {
                callback.onCompletion(null, null);
            }, singleThreaded);
        }

        singleThreaded.shutdown();
        final boolean success = singleThreaded.awaitTermination(60, TimeUnit.SECONDS);

        assertTrue(success, "should have terminated");
        assertEquals(numRecords, stats.totalCount());
        assertEquals(numRecords, stats.iteration());
        assertEquals(500000, stats.index());
        assertEquals(1000000 * 100, stats.bytes());
    }

    @Test
    public void testConfigPostProcessor() throws IOException, ArgumentParserException {
        ArgumentParser parser = ProducerPerformance.argParser();
        String[] args = new String[]{
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "100",
            "--record-size", "100",
            "--print-metrics",
            "--producer-props", "bootstrap.servers=localhost:9000",
            "--transactional-id", "foobar",
            "--transaction-duration-ms", "5000",
        };
        ProducerPerformance.ConfigPostProcessor configs = new ProducerPerformance.ConfigPostProcessor(parser, args);
        assertEquals("Hello-Kafka", configs.topicName);
        assertEquals(5, configs.numRecords);
        assertEquals(100, configs.throughput);
        assertEquals(100, configs.recordSize);
        assertFalse(configs.payloadMonotonic);
        assertTrue(configs.shouldPrintMetrics);
        assertTrue(configs.payloadByteList.isEmpty());
        Properties props = configs.producerProps;
        assertEquals(5, props.size());
        assertTrue(configs.transactionsEnabled);
        assertEquals(5000, configs.transactionDurationMs);
    }

    @Test
    public void testInvalidConfigPostProcessor() {
        ArgumentParser parser = ProducerPerformance.argParser();
        String[] invalidProducerProps = new String[]{
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "100",
            "--record-size", "100"};
        assertEquals("Either --producer-props or --producer.config must be specified.",
                assertThrows(ArgumentParserException.class,
                        () -> new ProducerPerformance.ConfigPostProcessor(parser, invalidProducerProps)).getMessage());

        String[] invalidTransactionDurationMs = new String[]{
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "100",
            "--record-size", "100",
            "--producer-props", "bootstrap.servers=localhost:9000",
            "--transaction-duration-ms", "0"};
        assertEquals("--transaction-duration-ms should be greater than zero",
                assertThrows(ArgumentParserException.class,
                        () -> new ProducerPerformance.ConfigPostProcessor(parser, invalidTransactionDurationMs)).getMessage());

        String[] invalidNumRecords = new String[]{
            "--topic", "Hello-Kafka",
            "--num-records", "-5",
            "--throughput", "100",
            "--record-size", "100",
            "--producer-props", "bootstrap.servers=localhost:9000"};
        assertEquals("--num-records should be greater than zero",
            assertThrows(ArgumentParserException.class,
                () -> new ProducerPerformance.ConfigPostProcessor(parser, invalidNumRecords)).getMessage());

        String[] invalidRecordSize = new String[]{
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "100",
            "--record-size", "-100",
            "--producer-props", "bootstrap.servers=localhost:9000"};
        assertEquals("--record-size should be greater than zero",
            assertThrows(ArgumentParserException.class,
                () -> new ProducerPerformance.ConfigPostProcessor(parser, invalidRecordSize)).getMessage());
    }

    @Test
    public void testNoTransactionRelatedConfigs() throws IOException, ArgumentParserException {
        ArgumentParser parser = ProducerPerformance.argParser();
        String[] args = new String[]{
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "100",
            "--record-size", "100",
            "--producer-props", "bootstrap.servers=localhost:9000"};
        ProducerPerformance.ConfigPostProcessor configs = new ProducerPerformance.ConfigPostProcessor(parser, args);
        assertFalse(configs.transactionsEnabled);
        assertNull(configs.transactionDurationMs);
        assertFalse(configs.producerProps.contains(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
    }

    @Test
    public void testEnableTransactionByProducerProps() throws IOException, ArgumentParserException {
        ArgumentParser parser = ProducerPerformance.argParser();
        String[] args = new String[]{
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "100",
            "--record-size", "100",
            "--producer-props", "bootstrap.servers=localhost:9000", "transactional.id=foobar"};
        ProducerPerformance.ConfigPostProcessor configs = new ProducerPerformance.ConfigPostProcessor(parser, args);
        assertTrue(configs.transactionsEnabled);
        assertEquals(ProducerPerformance.DEFAULT_TRANSACTION_DURATION_MS, configs.transactionDurationMs);
        assertEquals("foobar", configs.producerProps.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
    }

    @Test
    public void testEnableTransactionByTransactionId() throws IOException, ArgumentParserException {
        File producerConfigFile = createTempFile("transactional.id=foobar");
        ArgumentParser parser = ProducerPerformance.argParser();
        String[] args = new String[]{
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "100",
            "--record-size", "100",
            "--producer.config", producerConfigFile.getAbsolutePath(),
            "--producer-props", "bootstrap.servers=localhost:9000"};
        ProducerPerformance.ConfigPostProcessor configs = new ProducerPerformance.ConfigPostProcessor(parser, args);
        assertTrue(configs.transactionsEnabled);
        assertEquals(ProducerPerformance.DEFAULT_TRANSACTION_DURATION_MS, configs.transactionDurationMs);
        assertEquals("foobar", configs.producerProps.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));

        args = new String[]{
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "100",
            "--record-size", "100",
            "--producer.config", producerConfigFile.getAbsolutePath(),
            "--producer-props", "bootstrap.servers=localhost:9000", "transactional.id=hello_kafka"};
        configs = new ProducerPerformance.ConfigPostProcessor(parser, args);
        assertTrue(configs.transactionsEnabled);
        assertEquals(ProducerPerformance.DEFAULT_TRANSACTION_DURATION_MS, configs.transactionDurationMs);
        assertEquals("hello_kafka", configs.producerProps.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));

        args = new String[]{
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "100",
            "--record-size", "100",
            "--transactional-id", "kafka_hello",
            "--producer.config", producerConfigFile.getAbsolutePath(),
            "--producer-props", "bootstrap.servers=localhost:9000", "transactional.id=hello_kafka"};
        configs = new ProducerPerformance.ConfigPostProcessor(parser, args);
        assertTrue(configs.transactionsEnabled);
        assertEquals(ProducerPerformance.DEFAULT_TRANSACTION_DURATION_MS, configs.transactionDurationMs);
        assertEquals("kafka_hello", configs.producerProps.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));

        Utils.delete(producerConfigFile);
    }

    @Test
    public void testEnableTransactionByTransactionDurationMs() throws IOException, ArgumentParserException {
        ArgumentParser parser = ProducerPerformance.argParser();
        String[] args = new String[]{
            "--topic", "Hello-Kafka",
            "--num-records", "5",
            "--throughput", "100",
            "--record-size", "100",
            "--transaction-duration-ms", "5000",
            "--producer-props", "bootstrap.servers=localhost:9000"};
        ProducerPerformance.ConfigPostProcessor configs = new ProducerPerformance.ConfigPostProcessor(parser, args);
        assertTrue(configs.transactionsEnabled);
        assertEquals(5000, configs.transactionDurationMs);
        assertTrue(configs.producerProps.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG).toString()
                .startsWith(ProducerPerformance.DEFAULT_TRANSACTION_ID_PREFIX));
    }
}
