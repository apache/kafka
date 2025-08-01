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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_MESSAGE_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_OFFSET_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_PARTITION_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_STACKTRACE_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_TOPIC_NAME;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.utils.TestUtils.waitForApplicationState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Timeout(60)
public class DeadLetterQueueIntegrationTest {
    private static final int NUM_BROKERS = 3;

    private static EmbeddedKafkaCluster cluster;

    @BeforeAll
    public static void startCluster() throws IOException {
        cluster = new EmbeddedKafkaCluster(NUM_BROKERS);
        cluster.start();
    }

    @AfterAll
    public static void closeCluster() {
        cluster.stop();
        cluster = null;
    }

    private String applicationId;
    private static final int NUM_TOPIC_PARTITIONS = 3;
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static final String DLQ_TOPIC = "dlqTopic";

    private final List<KeyValue<String, String>> data = prepareData();
    private final List<KeyValue<String, byte[]>> bytesData = prepareBytesData();

    @BeforeEach
    public void createTopics(final TestInfo testInfo) throws Exception {
        applicationId = "appId-" + safeUniqueTestName(testInfo);
        cluster.deleteTopics(
            INPUT_TOPIC,
            OUTPUT_TOPIC,
            DLQ_TOPIC);
        cluster.createTopic(INPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
        cluster.createTopic(OUTPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
        cluster.createTopic(DLQ_TOPIC, NUM_TOPIC_PARTITIONS, 1);
    }

    @Test
    public void shouldSendToDlqAndFailFromDsl() throws Exception {
        try (final KafkaStreams streams = getDslStreams(LogAndFailProcessingExceptionHandler.class.getName())) {

            startApplicationAndWaitUntilRunning(streams);

            // Produce data to the input topic
            IntegrationTestUtils.produceKeyValuesSynchronously(
                INPUT_TOPIC,
                data,
                TestUtils.producerConfig(cluster.bootstrapServers(), StringSerializer.class, StringSerializer.class),
                cluster.time
            );

            // Consume the output records
            final List<ConsumerRecord<String, String>> outputRecords = readResult(OUTPUT_TOPIC, 1, StringDeserializer.class, StringDeserializer.class, 30000L);

            // Only the first record is available
            assertEquals(1, outputRecords.size(), "Only one record should be available in the output topic");
            assertEquals("value-1", outputRecords.get(0).value(), "Output record should be the first one");

            // Consume the DLQ records
            final List<ConsumerRecord<byte[], byte[]>> dlqRecords = readResult(DLQ_TOPIC, 1, ByteArrayDeserializer.class, ByteArrayDeserializer.class, 30000L);

            // Stream should be in ERROR state
            waitForApplicationState(singletonList(streams), KafkaStreams.State.ERROR, Duration.ofSeconds(30));

            assertEquals("key", new String(dlqRecords.get(0).key()), "Output record should be sent to DLQ topic");
            assertEquals("KABOOM", new String(dlqRecords.get(0).value()), "Output record should be sent to DLQ topic");

            assertEquals("java.lang.RuntimeException: KABOOM", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_EXCEPTION_NAME).value()));
            assertEquals("KABOOM", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_EXCEPTION_MESSAGE_NAME).value()));
            assertTrue(new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_STACKTRACE_NAME).value()).contains("org.apache.kafka.streams.kstream.internals.KStreamMapValues$KStreamMapProcessor.process"));
            assertEquals(INPUT_TOPIC, new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_TOPIC_NAME).value()));
            assertEquals("1", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_PARTITION_NAME).value()));
            assertEquals("1", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_OFFSET_NAME).value()));
        }
    }

    @Test
    public void shouldSendToDlqAndContinueFromDsl() throws Exception {
        try (final KafkaStreams streams = getDslStreams(LogAndContinueProcessingExceptionHandler.class.getName())) {

            startApplicationAndWaitUntilRunning(streams);

            // Produce data to the input topic
            IntegrationTestUtils.produceKeyValuesSynchronously(
                INPUT_TOPIC,
                data,
                TestUtils.producerConfig(cluster.bootstrapServers(), StringSerializer.class, StringSerializer.class),
                cluster.time
            );

            // Consume the output records
            final List<ConsumerRecord<String, String>> outputRecords = readResult(OUTPUT_TOPIC, 2, StringDeserializer.class, StringDeserializer.class, 30000L);

            // Only the first and third records are available
            assertEquals(2, outputRecords.size(), "Two records should be available in the output topic");
            assertEquals("value-1", outputRecords.get(0).value(), "Output record should be the first one");
            assertEquals("value-2", outputRecords.get(1).value(), "Output record should be the third one");

            // Consume the DLQ records
            final List<ConsumerRecord<byte[], byte[]>> dlqRecords = readResult(DLQ_TOPIC, 1, ByteArrayDeserializer.class, ByteArrayDeserializer.class, 30000L);

            // Stream should be in RUNNING state
            assertThrows(AssertionError.class, () -> waitForApplicationState(singletonList(streams), KafkaStreams.State.ERROR, Duration.ofSeconds(10)));
            waitForApplicationState(singletonList(streams), KafkaStreams.State.RUNNING, Duration.ofSeconds(5));

            assertEquals("key", new String(dlqRecords.get(0).key()), "Output record should be sent to DLQ topic");
            assertEquals("KABOOM", new String(dlqRecords.get(0).value()), "Output record should be sent to DLQ topic");

            assertEquals("java.lang.RuntimeException: KABOOM", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_EXCEPTION_NAME).value()));
            assertEquals("KABOOM", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_EXCEPTION_MESSAGE_NAME).value()));
            assertTrue(new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_STACKTRACE_NAME).value()).contains("org.apache.kafka.streams.kstream.internals.KStreamMapValues$KStreamMapProcessor.process"));
            assertEquals(INPUT_TOPIC, new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_TOPIC_NAME).value()));
            assertEquals("1", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_PARTITION_NAME).value()));
            assertEquals("1", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_OFFSET_NAME).value()));
        }
    }

    @Test
    public void shouldSendToDlqAndFailFromProcessorAPI() throws Exception {
        try (final KafkaStreams streams = getProcessorAPIStreams(LogAndFailProcessingExceptionHandler.class.getName())) {

            startApplicationAndWaitUntilRunning(streams);

            // Produce data to the input topic
            IntegrationTestUtils.produceKeyValuesSynchronously(
                INPUT_TOPIC,
                data,
                TestUtils.producerConfig(cluster.bootstrapServers(), StringSerializer.class, StringSerializer.class),
                cluster.time
            );

            // Consume the output records
            final List<ConsumerRecord<String, String>> outputRecords = readResult(OUTPUT_TOPIC, 1, StringDeserializer.class, StringDeserializer.class, 30000L);

           // Only the first record is available
            assertEquals(1, outputRecords.size(), "Only one record should be available in the output topic");
            assertEquals("value-1", outputRecords.get(0).value(), "Output record should be the first one");

            // Consume the DLQ records
            final List<ConsumerRecord<byte[], byte[]>> dlqRecords = readResult(DLQ_TOPIC, 1, ByteArrayDeserializer.class, ByteArrayDeserializer.class, 30000L);

            // Stream should be in ERROR state
            waitForApplicationState(singletonList(streams), KafkaStreams.State.ERROR, Duration.ofSeconds(30));

            assertEquals("key", new String(dlqRecords.get(0).key()), "Output record should be sent to DLQ topic");
            assertEquals("KABOOM", new String(dlqRecords.get(0).value()), "Output record should be sent to DLQ topic");

            assertEquals("java.lang.RuntimeException: KABOOM", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_EXCEPTION_NAME).value()));
            assertEquals("KABOOM", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_EXCEPTION_MESSAGE_NAME).value()));
            assertTrue(new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_STACKTRACE_NAME).value()).contains("org.apache.kafka.streams.integration.DeadLetterQueueIntegrationTest$1.process"));
            assertEquals(INPUT_TOPIC, new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_TOPIC_NAME).value()));
            assertEquals("1", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_PARTITION_NAME).value()));
            assertEquals("1", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_OFFSET_NAME).value()));
        }
    }

    @Test
    public void shouldSendToDlqAndContinueFromProcessorAPI() throws Exception {
        try (final KafkaStreams streams = getProcessorAPIStreams(LogAndContinueProcessingExceptionHandler.class.getName())) {

            startApplicationAndWaitUntilRunning(streams);

            // Produce data to the input topic
            IntegrationTestUtils.produceKeyValuesSynchronously(
                INPUT_TOPIC,
                data,
                TestUtils.producerConfig(cluster.bootstrapServers(), StringSerializer.class, StringSerializer.class),
                cluster.time
            );

            // Consume the output records
            final List<ConsumerRecord<String, String>> outputRecords = readResult(OUTPUT_TOPIC, 2, StringDeserializer.class, StringDeserializer.class, 30000L);

            // Only the first and third records are available
            assertEquals(2, outputRecords.size(), "Two records should be available in the output topic");
            assertEquals("value-1", outputRecords.get(0).value(), "Output record should be the first one");
            assertEquals("value-2", outputRecords.get(1).value(), "Output record should be the third one");

            // Consume the DLQ records
            final List<ConsumerRecord<byte[], byte[]>> dlqRecords = readResult(DLQ_TOPIC, 1, ByteArrayDeserializer.class, ByteArrayDeserializer.class, 30000L);

            // Stream should be in RUNNING state
            assertThrows(AssertionError.class, () -> waitForApplicationState(singletonList(streams), KafkaStreams.State.ERROR, Duration.ofSeconds(10)));
            waitForApplicationState(singletonList(streams), KafkaStreams.State.RUNNING, Duration.ofSeconds(5));

            assertEquals("key", new String(dlqRecords.get(0).key()), "Output record should be sent to DLQ topic");
            assertEquals("KABOOM", new String(dlqRecords.get(0).value()), "Output record should be sent to DLQ topic");

            assertEquals("java.lang.RuntimeException: KABOOM", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_EXCEPTION_NAME).value()));
            assertEquals("KABOOM", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_EXCEPTION_MESSAGE_NAME).value()));
            assertTrue(new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_STACKTRACE_NAME).value()).contains("org.apache.kafka.streams.integration.DeadLetterQueueIntegrationTest$1.process"));
            assertEquals(INPUT_TOPIC, new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_TOPIC_NAME).value()));
            assertEquals("1", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_PARTITION_NAME).value()));
            assertEquals("1", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_OFFSET_NAME).value()));
        }
    }

    @Test
    public void shouldSendToDlqAndFailFromDeserializationError() throws Exception {
        try (final KafkaStreams streams = getDeserializationStreams(LogAndFailExceptionHandler.class.getName())) {

            startApplicationAndWaitUntilRunning(streams);

            // Produce data to the input topic
            IntegrationTestUtils.produceKeyValuesSynchronously(
                INPUT_TOPIC,
                bytesData,
                TestUtils.producerConfig(cluster.bootstrapServers(), StringSerializer.class, ByteArraySerializer.class),
                cluster.time
            );

            // Consume the output records
            // No records of the same batch should be available in the output topic due to deserialization error
            final AssertionError error = assertThrows(AssertionError.class,
                                   () -> readResult(OUTPUT_TOPIC, 1, StringDeserializer.class, StringDeserializer.class, 10000L)
            );
            assertEquals("""
                Did not receive all 1 records from topic outputTopic within 10000 ms
                Expected: is a value equal to or greater than <1>
                     but: <0> was less than <1>""", error.getMessage());

            // Consume the DLQ records
            final List<ConsumerRecord<byte[], byte[]>> dlqRecords = readResult(DLQ_TOPIC, 1, ByteArrayDeserializer.class, ByteArrayDeserializer.class, 30000L);

            // Stream should be in ERROR state
            waitForApplicationState(singletonList(streams), KafkaStreams.State.ERROR, Duration.ofSeconds(30));

            assertEquals("key", new String(dlqRecords.get(0).key()), "Output record should be sent to DLQ topic");
            assertEquals("value", new String(dlqRecords.get(0).value()), "Output record should be sent to DLQ topic");

            assertEquals("org.apache.kafka.common.errors.SerializationException: Size of data received by LongDeserializer is not 8", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_EXCEPTION_NAME).value()));
            assertEquals("Size of data received by LongDeserializer is not 8", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_EXCEPTION_MESSAGE_NAME).value()));
            assertTrue(new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_STACKTRACE_NAME).value()).contains("org.apache.kafka.common.errors.SerializationException: Size of data received by LongDeserializer is not 8"));
            assertEquals(INPUT_TOPIC, new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_TOPIC_NAME).value()));
            assertEquals("1", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_PARTITION_NAME).value()));
            assertEquals("1", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_OFFSET_NAME).value()));
        }
    }

    @Test
    public void shouldSendToDlqAndContinueFromDeserializationError() throws Exception {
        try (final KafkaStreams streams = getDeserializationStreams(LogAndContinueExceptionHandler.class.getName())) {

            startApplicationAndWaitUntilRunning(streams);

            // Produce data to the input topic
            IntegrationTestUtils.produceKeyValuesSynchronously(
                INPUT_TOPIC,
                bytesData,
                TestUtils.producerConfig(cluster.bootstrapServers(), StringSerializer.class, ByteArraySerializer.class),
                cluster.time
            );

            // Consume the output records
            final List<ConsumerRecord<String, String>> outputRecords = readResult(OUTPUT_TOPIC, 1, StringDeserializer.class, StringDeserializer.class, 30000L);

            // Only the first and third records are available
            assertEquals(2, outputRecords.size(), "Two records should be available in the output topic");
            assertEquals("1", outputRecords.get(0).value(), "Output record should be the first one");
            assertEquals("3", outputRecords.get(1).value(), "Output record should be the third one");

            // Consume the DLQ records
            final List<ConsumerRecord<byte[], byte[]>> dlqRecords = readResult(DLQ_TOPIC, 1, ByteArrayDeserializer.class, ByteArrayDeserializer.class, 30000L);

            // Stream should be in RUNNING state
            assertThrows(AssertionError.class, () -> waitForApplicationState(singletonList(streams), KafkaStreams.State.ERROR, Duration.ofSeconds(10)));
            waitForApplicationState(singletonList(streams), KafkaStreams.State.RUNNING, Duration.ofSeconds(5));

            assertEquals("key", new String(dlqRecords.get(0).key()), "Output record should be sent to DLQ topic");
            assertEquals("value", new String(dlqRecords.get(0).value()), "Output record should be sent to DLQ topic");

            assertEquals("org.apache.kafka.common.errors.SerializationException: Size of data received by LongDeserializer is not 8", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_EXCEPTION_NAME).value()));
            assertEquals("Size of data received by LongDeserializer is not 8", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_EXCEPTION_MESSAGE_NAME).value()));
            assertTrue(new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_STACKTRACE_NAME).value()).contains("org.apache.kafka.common.errors.SerializationException: Size of data received by LongDeserializer is not 8"));
            assertEquals(INPUT_TOPIC, new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_TOPIC_NAME).value()));
            assertEquals("1", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_PARTITION_NAME).value()));
            assertEquals("1", new String(dlqRecords.get(0).headers().lastHeader(HEADER_ERRORS_OFFSET_NAME).value()));
        }
    }

    private KafkaStreams getDslStreams(final String processingExceptionHandlerClass) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .mapValues((k, v) -> {
                if ("KABOOM".equals(v)) {
                    // Simulate a processing error
                    throw new RuntimeException("KABOOM");
                }
                return v;
            }
            )
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return new KafkaStreams(builder.build(), getProcessingProperties(processingExceptionHandlerClass));
    }

    private KafkaStreams getProcessorAPIStreams(final String processingExceptionHandlerClass) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .process(() -> new ContextualProcessor<String, String, String, String>() {
                @Override
                public void process(final Record<String, String> record) {
                    if ("KABOOM".equals(record.value())) {
                        // Simulate a processing error
                        throw new RuntimeException("KABOOM");
                    }
                    // For example, forwarding to another topic
                    context().forward(record);
                }
            })
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return new KafkaStreams(builder.build(), getProcessingProperties(processingExceptionHandlerClass));
    }

    private KafkaStreams getDeserializationStreams(final String deserializationExceptionHandlerClass) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.Long()))
            .mapValues((k, v) -> String.valueOf(v))
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return new KafkaStreams(builder.build(), getDeserializationProperties(deserializationExceptionHandlerClass));
    }

    private Properties getDeserializationProperties(final String deserializationExceptionHandlerClass) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, DLQ_TOPIC);
        properties.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, deserializationExceptionHandlerClass);

        return getConfig(properties);
    }

    private Properties getProcessingProperties(final String processingExceptionHandlerClass) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, DLQ_TOPIC);
        properties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, processingExceptionHandlerClass);

        return getConfig(properties);
    }

    private Properties getConfig(final Properties properties) {
        return StreamsTestUtils.getStreamsConfig(
            applicationId,
            cluster.bootstrapServers(),
            Serdes.StringSerde.class.getName(),
            Serdes.StringSerde.class.getName(),
            properties);
    }

    private List<KeyValue<String, String>> prepareData() {
        final List<KeyValue<String, String>> data = new ArrayList<>();

        data.add(new KeyValue<>("key", "value-1"));
        data.add(new KeyValue<>("key", "KABOOM"));
        data.add(new KeyValue<>("key", "value-2"));

        return data;
    }

    private List<KeyValue<String, byte[]>> prepareBytesData() {
        final List<KeyValue<String, byte[]>> bytesData = new ArrayList<>();

        bytesData.add(new KeyValue<>("key", ByteBuffer.allocate(Long.BYTES).putLong(1L).array()));
        bytesData.add(new KeyValue<>("key", "value".getBytes()));
        bytesData.add(new KeyValue<>("key", ByteBuffer.allocate(Long.BYTES).putLong(3L).array()));

        return bytesData;
    }

    private <K, V> List<ConsumerRecord<K, V>> readResult(final String topic,
                                                         final int numberOfRecords,
                                                         final Class<? extends Deserializer<K>> keyDeserializer,
                                                         final Class<? extends Deserializer<V>> valueDeserializer,
                                                         final long timeout) throws Exception {
        return IntegrationTestUtils.waitUntilMinRecordsReceived(
            TestUtils.consumerConfig(cluster.bootstrapServers(), keyDeserializer, valueDeserializer),
            topic,
            numberOfRecords,
            timeout);
    }

}
