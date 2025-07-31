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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.MockProcessorSupplier;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class ProcessingExceptionHandlerIntegrationTest {
    private final String threadId = Thread.currentThread().getName();

    private static final Instant TIMESTAMP = Instant.now();

    @Test
    public void shouldFailWhenProcessingExceptionOccursIfExceptionHandlerReturnsFail() {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-1", "ID123-A1"),
            new KeyValue<>("ID123-2-ERR", "ID123-A2"),
            new KeyValue<>("ID123-3", "ID123-A3"),
            new KeyValue<>("ID123-4", "ID123-A4")
        );

        final List<KeyValueTimestamp<String, String>> expectedProcessedRecords = Collections.singletonList(
            new KeyValueTimestamp<>("ID123-1", "ID123-A1", TIMESTAMP.toEpochMilli())
        );

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .map(KeyValue::new)
            .mapValues(value -> value)
            .process(runtimeErrorProcessorSupplierMock())
            .process(processor);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, FailProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());

            final StreamsException exception = assertThrows(StreamsException.class,
                () -> inputTopic.pipeKeyValueList(events, TIMESTAMP, Duration.ZERO));

            assertTrue(exception.getMessage().contains("Exception caught in process. "
                + "taskId=0_0, processor=KSTREAM-PROCESSOR-0000000003, topic=TOPIC_NAME, "
                + "partition=0, offset=1"));
            assertEquals(1, processor.theCapturedProcessor().processed().size());
            assertIterableEquals(expectedProcessedRecords, processor.theCapturedProcessor().processed());

            final MetricName dropTotal = droppedRecordsTotalMetric();
            final MetricName dropRate = droppedRecordsRateMetric();

            assertEquals(0.0, driver.metrics().get(dropTotal).metricValue());
            assertEquals(0.0, driver.metrics().get(dropRate).metricValue());
        }
    }

    @Test
    public void shouldFailWhenProcessingExceptionOccursFromFlushingCacheIfExceptionHandlerReturnsFail() {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-1", "ID123-A1"),
            new KeyValue<>("ID123-1", "ID123-A2"),
            new KeyValue<>("ID123-1", "ID123-A3"),
            new KeyValue<>("ID123-1", "ID123-A4")
        );

        final List<KeyValueTimestamp<String, String>> expectedProcessedRecords = Arrays.asList(
            new KeyValueTimestamp<>("ID123-1", "1", TIMESTAMP.toEpochMilli()),
            new KeyValueTimestamp<>("ID123-1", "2", TIMESTAMP.toEpochMilli())
        );

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .count()
            .toStream()
            .mapValues(value -> value.toString())
            .process(runtimeErrorProcessorSupplierMock())
            .process(processor);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndFailProcessingExceptionHandler.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());

            final StreamsException exception = assertThrows(StreamsException.class,
                () -> inputTopic.pipeKeyValueList(events, TIMESTAMP, Duration.ZERO));

            assertTrue(exception.getMessage().contains("Failed to flush cache of store KSTREAM-AGGREGATE-STATE-STORE-0000000001"));
            assertEquals(expectedProcessedRecords.size(), processor.theCapturedProcessor().processed().size());
            assertIterableEquals(expectedProcessedRecords, processor.theCapturedProcessor().processed());

            final MetricName dropTotal = droppedRecordsTotalMetric();
            final MetricName dropRate = droppedRecordsRateMetric();

            assertEquals(0.0, driver.metrics().get(dropTotal).metricValue());
            assertEquals(0.0, driver.metrics().get(dropRate).metricValue());
        }
    }

    @Test
    public void shouldContinueWhenProcessingExceptionOccursIfExceptionHandlerReturnsContinue() {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-1", "ID123-A1"),
            new KeyValue<>("ID123-2-ERR", "ID123-A2"),
            new KeyValue<>("ID123-3", "ID123-A3"),
            new KeyValue<>("ID123-4", "ID123-A4"),
            new KeyValue<>("ID123-5-ERR", "ID123-A5"),
            new KeyValue<>("ID123-6", "ID123-A6")
        );

        final List<KeyValueTimestamp<String, String>> expectedProcessedRecords = Arrays.asList(
            new KeyValueTimestamp<>("ID123-1", "ID123-A1", TIMESTAMP.toEpochMilli()),
            new KeyValueTimestamp<>("ID123-3", "ID123-A3", TIMESTAMP.toEpochMilli()),
            new KeyValueTimestamp<>("ID123-4", "ID123-A4", TIMESTAMP.toEpochMilli()),
            new KeyValueTimestamp<>("ID123-6", "ID123-A6", TIMESTAMP.toEpochMilli())
        );

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .map(KeyValue::new)
            .mapValues(value -> value)
            .process(runtimeErrorProcessorSupplierMock())
            .process(processor);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, ContinueProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());
            inputTopic.pipeKeyValueList(events, TIMESTAMP, Duration.ZERO);

            assertEquals(expectedProcessedRecords.size(), processor.theCapturedProcessor().processed().size());
            assertIterableEquals(expectedProcessedRecords, processor.theCapturedProcessor().processed());

            final MetricName dropTotal = droppedRecordsTotalMetric();
            final MetricName dropRate = droppedRecordsRateMetric();

            assertEquals(2.0, driver.metrics().get(dropTotal).metricValue());
            assertTrue((Double) driver.metrics().get(dropRate).metricValue() > 0.0);
        }
    }

    @Test
    public void shouldContinueWhenProcessingExceptionOccursFromFlushingCacheIfExceptionHandlerReturnsContinue() {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-1", "ID123-A1"),
            new KeyValue<>("ID123-1", "ID123-A2"),
            new KeyValue<>("ID123-1", "ID123-A3"),
            new KeyValue<>("ID123-1", "ID123-A4")
        );

        final List<KeyValueTimestamp<String, String>> expectedProcessedRecords = Arrays.asList(
            new KeyValueTimestamp<>("ID123-1", "1", TIMESTAMP.toEpochMilli()),
            new KeyValueTimestamp<>("ID123-1", "2", TIMESTAMP.toEpochMilli()),
            new KeyValueTimestamp<>("ID123-1", "4", TIMESTAMP.toEpochMilli())
        );

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .count()
            .toStream()
            .mapValues(value -> value.toString())
            .process(runtimeErrorProcessorSupplierMock())
            .process(processor);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueProcessingExceptionHandler.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());
            inputTopic.pipeKeyValueList(events, TIMESTAMP, Duration.ZERO);

            assertEquals(expectedProcessedRecords.size(), processor.theCapturedProcessor().processed().size());
            assertIterableEquals(expectedProcessedRecords, processor.theCapturedProcessor().processed());

            final MetricName dropTotal = droppedRecordsTotalMetric();
            final MetricName dropRate = droppedRecordsRateMetric();

            assertEquals(1.0, driver.metrics().get(dropTotal).metricValue());
            assertTrue((Double) driver.metrics().get(dropRate).metricValue() > 0.0);
        }
    }

    @Test
    public void shouldStopOnFailedProcessorWhenProcessingExceptionOccursInFailProcessingExceptionHandler() {
        final KeyValue<String, String> event = new KeyValue<>("ID123-1", "ID123-A1");
        final KeyValue<String, String> eventError = new KeyValue<>("ID123-2-ERR", "ID123-A2");

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        final AtomicBoolean isExecuted = new AtomicBoolean(false);
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .map(KeyValue::new)
            .mapValues(value -> value)
            .process(runtimeErrorProcessorSupplierMock())
            .map((k, v) -> {
                isExecuted.set(true);
                return KeyValue.pair(k, v);
            })
            .process(processor);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, FailProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());
            isExecuted.set(false);
            inputTopic.pipeInput(event.key, event.value, TIMESTAMP);
            assertTrue(isExecuted.get());
            isExecuted.set(false);
            final StreamsException e = assertThrows(StreamsException.class, () -> inputTopic.pipeInput(eventError.key, eventError.value, TIMESTAMP));
            assertTrue(e.getMessage().contains("Exception caught in process. "
                + "taskId=0_0, processor=KSTREAM-PROCESSOR-0000000003, topic=TOPIC_NAME, "
                + "partition=0, offset=1"));
            assertFalse(isExecuted.get());
        }
    }

    @Test
    public void shouldStopOnFailedProcessorWhenProcessingExceptionOccursInContinueProcessingExceptionHandler() {
        final KeyValue<String, String> event = new KeyValue<>("ID123-1", "ID123-A1");
        final KeyValue<String, String> eventFalse = new KeyValue<>("ID123-2-ERR", "ID123-A2");

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        final AtomicBoolean isExecuted = new AtomicBoolean(false);
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .map(KeyValue::new)
            .mapValues(value -> value)
            .process(runtimeErrorProcessorSupplierMock())
            .map((k, v) -> {
                isExecuted.set(true);
                return KeyValue.pair(k, v);
            })
            .process(processor);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, ContinueProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());
            isExecuted.set(false);
            inputTopic.pipeInput(event.key, event.value, TIMESTAMP);
            assertTrue(isExecuted.get());
            isExecuted.set(false);
            inputTopic.pipeInput(eventFalse.key, eventFalse.value, TIMESTAMP);
            assertFalse(isExecuted.get());
        }
    }

    @Test
    public void shouldStopProcessingWhenProcessingExceptionHandlerReturnsNull() {
        final KeyValue<String, String> event = new KeyValue<>("ID123-1", "ID123-A1");
        final KeyValue<String, String> eventError = new KeyValue<>("ID123-ERR-NULL", "ID123-A2");

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        final AtomicBoolean isExecuted = new AtomicBoolean(false);
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .map(KeyValue::new)
            .mapValues(value -> value)
            .process(runtimeErrorProcessorSupplierMock())
            .map((k, v) -> {
                isExecuted.set(true);
                return KeyValue.pair(k, v);
            })
            .process(processor);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, ContinueProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());
            isExecuted.set(false);
            inputTopic.pipeInput(event.key, event.value, TIMESTAMP);
            assertTrue(isExecuted.get());
            isExecuted.set(false);
            final StreamsException e = assertThrows(StreamsException.class, () -> inputTopic.pipeInput(eventError.key, eventError.value, Instant.EPOCH));
            assertEquals("Fatal user code error in processing error callback", e.getMessage());
            assertInstanceOf(NullPointerException.class, e.getCause());
            assertEquals("Invalid ProcessingExceptionHandler response.", e.getCause().getMessage());
            assertFalse(isExecuted.get());
        }
    }

    @Test
    public void shouldStopProcessingWhenFatalUserExceptionProcessingExceptionHandler() {
        final KeyValue<String, String> event = new KeyValue<>("ID123-1", "ID123-A1");
        final KeyValue<String, String> eventError = new KeyValue<>("ID123-ERR-FATAL", "ID123-A2");

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        final AtomicBoolean isExecuted = new AtomicBoolean(false);
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .map(KeyValue::new)
            .mapValues(value -> value)
            .process(runtimeErrorProcessorSupplierMock())
            .map((k, v) -> {
                isExecuted.set(true);
                return KeyValue.pair(k, v);
            })
            .process(processor);

        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, ContinueProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());
            isExecuted.set(false);
            inputTopic.pipeInput(event.key, event.value, TIMESTAMP);
            assertTrue(isExecuted.get());
            isExecuted.set(false);
            final StreamsException e = assertThrows(StreamsException.class, () -> inputTopic.pipeInput(eventError.key, eventError.value, Instant.EPOCH));
            assertEquals("Fatal user code error in processing error callback", e.getMessage());
            assertEquals("KABOOM!", e.getCause().getMessage());
            assertFalse(isExecuted.get());
        }
    }

    static Stream<Arguments> sourceRawRecordTopologyTestCases() {
        // Validate source raw key and source raw value for fully stateless topology
        final List<ProducerRecord<String, String>> statelessTopologyEvent = List.of(new ProducerRecord<>("TOPIC_NAME", "ID123-1", "ID123-A1"));
        final StreamsBuilder statelessTopologyBuilder = new StreamsBuilder();
        statelessTopologyBuilder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .selectKey((key, value) -> "newKey")
            .mapValues(value -> {
                throw new RuntimeException("Error");
            });

        // Validate source raw key and source raw value for processing exception in aggregator with caching enabled
        final List<ProducerRecord<String, String>> cacheAggregateExceptionInAggregatorEvent = List.of(new ProducerRecord<>("TOPIC_NAME", "INITIAL-KEY123-1", "ID123-A1"));
        final StreamsBuilder cacheAggregateExceptionInAggregatorTopologyBuilder = new StreamsBuilder();
        cacheAggregateExceptionInAggregatorTopologyBuilder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .groupBy((key, value) -> "ID123-1", Grouped.with(Serdes.String(), Serdes.String()))
            .aggregate(() -> "initialValue",
                (key, value, aggregate) -> {
                    throw new RuntimeException("Error");
                },
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("aggregate")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .withCachingEnabled());

        // Validate source raw key and source raw value for processing exception after aggregation with caching enabled
        final List<ProducerRecord<String, String>> cacheAggregateExceptionAfterAggregationEvent = List.of(new ProducerRecord<>("TOPIC_NAME", "INITIAL-KEY123-1", "ID123-A1"));
        final StreamsBuilder cacheAggregateExceptionAfterAggregationTopologyBuilder = new StreamsBuilder();
        cacheAggregateExceptionAfterAggregationTopologyBuilder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .groupBy((key, value) -> "ID123-1", Grouped.with(Serdes.String(), Serdes.String()))
            .aggregate(() -> "initialValue",
                (key, value, aggregate) -> value,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("aggregate")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .withCachingEnabled())
            .mapValues(value -> {
                throw new RuntimeException("Error");
            });

        // Validate source raw key and source raw value for processing exception after aggregation with caching disabled
        final List<ProducerRecord<String, String>> noCacheAggregateExceptionAfterAggregationEvents = List.of(new ProducerRecord<>("TOPIC_NAME", "INITIAL-KEY123-1", "ID123-A1"));
        final StreamsBuilder noCacheAggregateExceptionAfterAggregationTopologyBuilder = new StreamsBuilder();
        noCacheAggregateExceptionAfterAggregationTopologyBuilder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .groupBy((key, value) -> "ID123-1", Grouped.with(Serdes.String(), Serdes.String()))
            .aggregate(() -> "initialValue",
                (key, value, aggregate) -> value,
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("aggregate")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .withCachingDisabled())
            .mapValues(value -> {
                throw new RuntimeException("Error");
            });

        // Validate source raw key and source raw value for processing exception after table creation with caching enabled
        final List<ProducerRecord<String, String>> cacheTableEvents = List.of(new ProducerRecord<>("TOPIC_NAME", "ID123-1", "ID123-A1"));
        final StreamsBuilder cacheTableTopologyBuilder = new StreamsBuilder();
        cacheTableTopologyBuilder
            .table("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("table")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String())
                .withCachingEnabled())
            .mapValues(value -> {
                throw new RuntimeException("Error");
            });

        // Validate source raw key and source raw value for processing exception in join
        final List<ProducerRecord<String, String>> joinEvents = List.of(
            new ProducerRecord<>("TOPIC_NAME_2", "INITIAL-KEY123-1", "ID123-A1"),
            new ProducerRecord<>("TOPIC_NAME", "INITIAL-KEY123-2", "ID123-A1")
        );
        final StreamsBuilder joinTopologyBuilder = new StreamsBuilder();
        joinTopologyBuilder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .selectKey((key, value) -> "ID123-1")
            .leftJoin(joinTopologyBuilder.stream("TOPIC_NAME_2", Consumed.with(Serdes.String(), Serdes.String()))
                    .selectKey((key, value) -> "ID123-1"),
                (key, left, right) -> {
                    throw new RuntimeException("Error");
                },
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)),
                StreamJoined.with(
                        Serdes.String(), Serdes.String(), Serdes.String())
                    .withName("join-rekey")
                    .withStoreName("join-store"));

        return Stream.of(
            Arguments.of(statelessTopologyEvent, statelessTopologyBuilder.build()),
            Arguments.of(cacheAggregateExceptionInAggregatorEvent, cacheAggregateExceptionInAggregatorTopologyBuilder.build()),
            Arguments.of(cacheAggregateExceptionAfterAggregationEvent, noCacheAggregateExceptionAfterAggregationTopologyBuilder.build()),
            Arguments.of(noCacheAggregateExceptionAfterAggregationEvents, cacheAggregateExceptionInAggregatorTopologyBuilder.build()),
            Arguments.of(cacheTableEvents, cacheTableTopologyBuilder.build()),
            Arguments.of(joinEvents, joinTopologyBuilder.build())
        );
    }

    @ParameterizedTest
    @MethodSource("sourceRawRecordTopologyTestCases")
    public void shouldVerifySourceRawKeyAndSourceRawValuePresentOrNotInErrorHandlerContext(final List<ProducerRecord<String, String>> events,
                                                                                           final Topology topology) {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
                AssertSourceRawRecordProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, properties, Instant.ofEpochMilli(0L))) {
            for (final ProducerRecord<String, String> event : events) {
                final TestInputTopic<String, String> inputTopic = driver.createInputTopic(event.topic(), new StringSerializer(), new StringSerializer());

                final String key = event.key();
                final String value = event.value();

                if (event.topic().equals("TOPIC_NAME")) {
                    assertThrows(StreamsException.class, () -> inputTopic.pipeInput(key, value, TIMESTAMP));
                } else {
                    inputTopic.pipeInput(event.key(), event.value(), TIMESTAMP);
                }
            }
        }
    }

    public static class ContinueProcessingExceptionHandlerMockTest implements ProcessingExceptionHandler {
        @Override
        public Response handleError(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            if (((String) record.key()).contains("FATAL")) {
                throw new RuntimeException("KABOOM!");
            }
            if (((String) record.key()).contains("NULL")) {
                return null;
            }
            assertProcessingExceptionHandlerInputs(context, record, exception);
            return Response.resume();
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            // No-op
        }
    }

    public static class FailProcessingExceptionHandlerMockTest implements ProcessingExceptionHandler {
        @Override
        public Response handleError(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            assertProcessingExceptionHandlerInputs(context, record, exception);
            return Response.fail();
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            // No-op
        }
    }

    private static void assertProcessingExceptionHandlerInputs(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
        assertTrue(Arrays.asList("ID123-2-ERR", "ID123-5-ERR").contains((String) record.key()));
        assertTrue(Arrays.asList("ID123-A2", "ID123-A5").contains((String) record.value()));
        assertEquals("TOPIC_NAME", context.topic());
        assertEquals("KSTREAM-PROCESSOR-0000000003", context.processorNodeId());
        assertTrue(Arrays.equals("ID123-2-ERR".getBytes(), context.sourceRawKey())
            || Arrays.equals("ID123-5-ERR".getBytes(), context.sourceRawKey()));
        assertTrue(Arrays.equals("ID123-A2".getBytes(), context.sourceRawValue())
            || Arrays.equals("ID123-A5".getBytes(), context.sourceRawValue()));
        assertEquals(TIMESTAMP.toEpochMilli(), context.timestamp());
        assertTrue(exception.getMessage().contains("Exception should be handled by processing exception handler"));
    }

    public static class AssertSourceRawRecordProcessingExceptionHandlerMockTest implements ProcessingExceptionHandler {
        @Override
        public ProcessingExceptionHandler.ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            assertEquals("ID123-1", Serdes.String().deserializer().deserialize("topic", context.sourceRawKey()));
            assertEquals("ID123-A1", Serdes.String().deserializer().deserialize("topic", context.sourceRawValue()));
            return ProcessingExceptionHandler.ProcessingHandlerResponse.FAIL;
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            // No-op
        }
    }

    /**
     * Metric name for dropped records total.
     *
     * @return the metric name
     */
    private MetricName droppedRecordsTotalMetric() {
        return new MetricName(
            "dropped-records-total",
            "stream-task-metrics",
            "The total number of dropped records",
            mkMap(
                mkEntry("thread-id", threadId),
                mkEntry("task-id", "0_0")
            )
        );
    }

    /**
     * Metric name for dropped records rate.
     *
     * @return the metric name
     */
    private MetricName droppedRecordsRateMetric() {
        return new MetricName(
            "dropped-records-rate",
            "stream-task-metrics",
            "The average number of dropped records per second",
            mkMap(
                mkEntry("thread-id", threadId),
                mkEntry("task-id", "0_0")
            )
        );
    }

    /**
     * Processor supplier that throws a runtime exception on process.
     *
     * @return the processor supplier
     */
    private ProcessorSupplier<String, String, String, String> runtimeErrorProcessorSupplierMock() {
        return () -> new ContextualProcessor<String, String, String, String>() {
            @Override
            public void process(final Record<String, String> record) {
                if (record.key().contains("ERR") || record.value().equals("3")) {
                    throw new RuntimeException("Exception should be handled by processing exception handler");
                }

                context().forward(new Record<>(record.key(), record.value(), record.timestamp()));
            }
        };
    }
}