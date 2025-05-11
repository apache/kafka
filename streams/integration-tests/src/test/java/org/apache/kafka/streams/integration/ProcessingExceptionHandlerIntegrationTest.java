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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.test.MockProcessorSupplier;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

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
            assertEquals("Invalid ProductionExceptionHandler response.", e.getCause().getMessage());
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

    public static class ContinueProcessingExceptionHandlerMockTest implements ProcessingExceptionHandler {
        @Override
        public ProcessingExceptionHandler.ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            if (((String) record.key()).contains("FATAL")) {
                throw new RuntimeException("KABOOM!");
            }
            if (((String) record.key()).contains("NULL")) {
                return null;
            }
            assertProcessingExceptionHandlerInputs(context, record, exception);
            return ProcessingExceptionHandler.ProcessingHandlerResponse.CONTINUE;
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            // No-op
        }
    }

    public static class FailProcessingExceptionHandlerMockTest implements ProcessingExceptionHandler {
        @Override
        public ProcessingExceptionHandler.ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            assertProcessingExceptionHandlerInputs(context, record, exception);
            return ProcessingExceptionHandler.ProcessingHandlerResponse.FAIL;
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
        assertEquals(TIMESTAMP.toEpochMilli(), context.timestamp());
        assertTrue(exception.getMessage().contains("Exception should be handled by processing exception handler"));
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