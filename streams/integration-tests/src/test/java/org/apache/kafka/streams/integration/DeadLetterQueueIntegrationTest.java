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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_MESSAGE_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_OFFSET_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_PARTITION_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_STACKTRACE_NAME;
import static org.apache.kafka.streams.errors.internals.ExceptionHandlerUtils.HEADER_ERRORS_TOPIC_NAME;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.utils.TestUtils.waitForApplicationState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Timeout(60)
public class DeadLetterQueueIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(DeadLetterQueueIntegrationTest.class);
    private static final int NUM_BROKERS = 3;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        NUM_BROKERS,
        Utils.mkProperties(mkMap(
            mkEntry("auto.create.topics.enable", "true")
        ))
    );

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private String applicationId;
    private static final int NUM_TOPIC_PARTITIONS = 3;
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";
    private static final String DLQ_TOPIC = "dlqTopic";

    private static final AtomicInteger TEST_NUMBER = new AtomicInteger(0);

    private final List<KeyValue<String, String>> data = prepareData();

    @BeforeEach
    public void createTopics() throws Exception {
        applicationId = "appId-" + TEST_NUMBER.getAndIncrement();
        CLUSTER.deleteTopics(
            INPUT_TOPIC,
            OUTPUT_TOPIC,
            DLQ_TOPIC);
        CLUSTER.createTopic(INPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
        CLUSTER.createTopic(OUTPUT_TOPIC, NUM_TOPIC_PARTITIONS, 1);
    }

    @Test
    public void shouldSendToDlqAndFailFromDsl() throws Exception {

        try (final KafkaStreams streams = getDslStreams(LogAndFailProcessingExceptionHandler.class.getName())) {

            startApplicationAndWaitUntilRunning(streams);

            // Produce data to the input topic
            IntegrationTestUtils.produceKeyValuesSynchronously(
                INPUT_TOPIC,
                data,
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
                CLUSTER.time
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
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
                CLUSTER.time
            );

            // Consume the output records
            final List<ConsumerRecord<String, String>> outputRecords = readResult(OUTPUT_TOPIC, 2, StringDeserializer.class, StringDeserializer.class, 30000L);

            // Only the first and third records are available
            assertEquals(2, outputRecords.size(), "Only two records should be available in the output topic");
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
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
                CLUSTER.time
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
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
                CLUSTER.time
            );

            // Consume the output records
            final List<ConsumerRecord<String, String>> outputRecords = readResult(OUTPUT_TOPIC, 2, StringDeserializer.class, StringDeserializer.class, 30000L);

            // Only the first and third records are available
            assertEquals(2, outputRecords.size(), "Only two records should be available in the output topic");
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

    private KafkaStreams getDslStreams(final String processingExceptionHandlerClass) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC)
            .mapValues((k, v) -> {
                if ("KABOOM".equals(v)) {
                    // Simulate a processing error
                    throw new RuntimeException("KABOOM");
                }
                return v;
            }
            )
            .to(OUTPUT_TOPIC);

        return new KafkaStreams(builder.build(), getConfig(processingExceptionHandlerClass));
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
            .to(OUTPUT_TOPIC);

        return new KafkaStreams(builder.build(), getConfig(processingExceptionHandlerClass));
    }

    private Properties getConfig(final String processingExceptionHandlerClass) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, DLQ_TOPIC);
        properties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, processingExceptionHandlerClass);

        return StreamsTestUtils.getStreamsConfig(
            applicationId,
            CLUSTER.bootstrapServers(),
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

    private <K, V> List<ConsumerRecord<K, V>> readResult(final String topic,
                                                   final int numberOfRecords,
                                                   final Class<? extends Deserializer<K>> keyDeserializer,
                                                   final Class<? extends Deserializer<V>> valueDeserializer,
                                                   final long timeout) throws Exception {

        return IntegrationTestUtils.waitUntilMinRecordsReceived(
            TestUtils.consumerConfig(CLUSTER.bootstrapServers(), keyDeserializer, valueDeserializer),
            topic,
            numberOfRecords,
            timeout);
    }

}
