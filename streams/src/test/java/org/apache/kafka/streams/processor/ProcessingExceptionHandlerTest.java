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
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.test.MockProcessorSupplier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class ProcessingExceptionHandlerTest {
    @Test
    public void shouldContinueInProcessorOnProcessingRecordAtBeginningExceptions() {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-1", "ID123-A1"),
            new KeyValue<>("ID123-2", "ID123-A2"),
            new KeyValue<>("ID123-3", "ID123-A3"),
            new KeyValue<>("ID123-4", "ID123-A4")
        );

        final List<KeyValueTimestamp<String, String>> expected = Arrays.asList(
            new KeyValueTimestamp<>("ID123-2", "ID123-A2", 0),
            new KeyValueTimestamp<>("ID123-3", "ID123-A3", 0),
            new KeyValueTimestamp<>("ID123-4", "ID123-A4", 0)
        );

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .process(runtimeErrorProcessorSupplierMock())
            .process(processor);

        final Properties properties = new Properties();
        properties.put("processing.exception.handler", ContinueProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());
            inputTopic.pipeKeyValueList(events, Instant.EPOCH, Duration.ZERO);

            assertEquals(expected.size(), processor.theCapturedProcessor().processed().size());
            assertIterableEquals(expected, processor.theCapturedProcessor().processed());
        }
    }

    @Test
    public void shouldContinueInProcessorOnProcessingRecordInMiddleExceptions() {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-2", "ID123-A2"),
            new KeyValue<>("ID123-1", "ID123-A1"),
            new KeyValue<>("ID123-3", "ID123-A3"),
            new KeyValue<>("ID123-4", "ID123-A4")
        );

        final List<KeyValueTimestamp<String, String>> expected = Arrays.asList(
            new KeyValueTimestamp<>("ID123-2", "ID123-A2", 0),
            new KeyValueTimestamp<>("ID123-3", "ID123-A3", 0),
            new KeyValueTimestamp<>("ID123-4", "ID123-A4", 0)
        );

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .process(runtimeErrorProcessorSupplierMock())
            .process(processor);

        final Properties properties = new Properties();
        properties.put("processing.exception.handler", ContinueProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());
            inputTopic.pipeKeyValueList(events, Instant.EPOCH, Duration.ZERO);

            assertEquals(expected.size(), processor.theCapturedProcessor().processed().size());
            assertArrayEquals(expected.toArray(), processor.theCapturedProcessor().processed().toArray());
        }
    }

    @Test
    public void shouldContinueInProcessorOnProcessingRecordAtEndExceptions() {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-2", "ID123-A2"),
            new KeyValue<>("ID123-3", "ID123-A3"),
            new KeyValue<>("ID123-4", "ID123-A4"),
            new KeyValue<>("ID123-1", "ID123-A1")
        );

        final List<KeyValueTimestamp<String, String>> expected = Arrays.asList(
            new KeyValueTimestamp<>("ID123-2", "ID123-A2", 0),
            new KeyValueTimestamp<>("ID123-3", "ID123-A3", 0),
            new KeyValueTimestamp<>("ID123-4", "ID123-A4", 0)
        );

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .process(runtimeErrorProcessorSupplierMock())
            .process(processor);

        final Properties properties = new Properties();
        properties.put("processing.exception.handler", ContinueProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());
            inputTopic.pipeKeyValueList(events, Instant.EPOCH, Duration.ZERO);

            assertEquals(expected.size(), processor.theCapturedProcessor().processed().size());
            assertArrayEquals(expected.toArray(), processor.theCapturedProcessor().processed().toArray());
        }
    }

    @Test
    public void shouldFailInProcessorOnProcessingRecordAtBeginningExceptions() {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-1", "ID123-A1"),
            new KeyValue<>("ID123-2", "ID123-A2"),
            new KeyValue<>("ID123-3", "ID123-A3"),
            new KeyValue<>("ID123-4", "ID123-A4")
        );

        final List<KeyValue<String, String>> expected = Collections.emptyList();

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .process(runtimeErrorProcessorSupplierMock())
            .process(processor);

        final Properties properties = new Properties();
        properties.put("processing.exception.handler", FailProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());

            final RuntimeException exception = assertThrows(RuntimeException.class,
                () -> inputTopic.pipeKeyValueList(events, Instant.EPOCH, Duration.ZERO));

            assertEquals("Exception should be handled by processing exception handler", exception.getCause().getMessage());
            assertEquals(0, processor.theCapturedProcessor().processed().size());
            assertArrayEquals(expected.toArray(), processor.theCapturedProcessor().processed().toArray());
        }
    }

    @Test
    public void shouldFailInProcessorOnProcessingRecordInMiddleExceptions() {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-2", "ID123-A2"),
            new KeyValue<>("ID123-1", "ID123-A1"),
            new KeyValue<>("ID123-3", "ID123-A3"),
            new KeyValue<>("ID123-4", "ID123-A4")
        );

        final List<KeyValueTimestamp<String, String>> expected = Collections.singletonList(
            new KeyValueTimestamp<>("ID123-2", "ID123-A2", 0)
        );

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .process(runtimeErrorProcessorSupplierMock())
            .process(processor);

        final Properties properties = new Properties();
        properties.put("processing.exception.handler", FailProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());

            final RuntimeException exception = assertThrows(RuntimeException.class,
                () -> inputTopic.pipeKeyValueList(events, Instant.EPOCH, Duration.ZERO));

            assertEquals("Exception should be handled by processing exception handler", exception.getCause().getMessage());
            assertEquals(expected.size(), processor.theCapturedProcessor().processed().size());
            assertArrayEquals(expected.toArray(), processor.theCapturedProcessor().processed().toArray());
        }
    }

    @Test
    public void shouldFailInProcessorOnProcessingRecordAtEndExceptions() {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-2", "ID123-A2"),
            new KeyValue<>("ID123-3", "ID123-A3"),
            new KeyValue<>("ID123-4", "ID123-A4"),
            new KeyValue<>("ID123-1", "ID123-A1")
        );

        final List<KeyValueTimestamp<String, String>> expected = Arrays.asList(
            new KeyValueTimestamp<>("ID123-2", "ID123-A2", 0),
            new KeyValueTimestamp<>("ID123-3", "ID123-A3", 0),
            new KeyValueTimestamp<>("ID123-4", "ID123-A4", 0)
        );

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .process(runtimeErrorProcessorSupplierMock())
            .process(processor);

        final Properties properties = new Properties();
        properties.put("processing.exception.handler", FailProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());

            final RuntimeException exception = assertThrows(RuntimeException.class,
                () -> inputTopic.pipeKeyValueList(events, Instant.EPOCH, Duration.ZERO));

            assertEquals("Exception should be handled by processing exception handler", exception.getCause().getMessage());
            assertEquals(expected.size(), processor.theCapturedProcessor().processed().size());
            assertArrayEquals(expected.toArray(), processor.theCapturedProcessor().processed().toArray());
        }
    }

    @Test
    public void shouldContinueOnPunctuateExceptions() {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-1", "ID123-A1"),
            new KeyValue<>("ID123-2", "ID123-A2"),
            new KeyValue<>("ID123-3", "ID123-A3"),
            new KeyValue<>("ID123-4", "ID123-A4")
        );

        final List<KeyValueTimestamp<String, String>> expected = Arrays.asList(
            new KeyValueTimestamp<>("ID123-1", "ID123-A1", 0),
            new KeyValueTimestamp<>("ID123-2", "ID123-A2", 0),
            new KeyValueTimestamp<>("ID123-3", "ID123-A3", 0),
            new KeyValueTimestamp<>("ID123-4", "ID123-A4", 0)
        );

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .process(runtimeErrorPunctuateProcessorSupplierMock())
            .process(processor);

        final Properties properties = new Properties();
        properties.put("processing.exception.handler", ContinuePunctuateProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());
            inputTopic.pipeKeyValueList(events, Instant.EPOCH, Duration.ZERO);

            assertEquals(expected.size(), processor.theCapturedProcessor().processed().size());
            assertArrayEquals(expected.toArray(), processor.theCapturedProcessor().processed().toArray());
        }
    }

    @Test
    public void shouldFailOnPunctuateExceptions() {
        final List<KeyValue<String, String>> events = Arrays.asList(
            new KeyValue<>("ID123-1", "ID123-A1"),
            new KeyValue<>("ID123-2", "ID123-A2"),
            new KeyValue<>("ID123-3", "ID123-A3")
        );

        final List<KeyValueTimestamp<String, String>> expected = Arrays.asList(
            new KeyValueTimestamp<>("ID123-1", "ID123-A1", 0),
            new KeyValueTimestamp<>("ID123-2", "ID123-A2", 0),
            new KeyValueTimestamp<>("ID123-3", "ID123-A3", 0)
        );

        final MockProcessorSupplier<String, String, Void, Void> processor = new MockProcessorSupplier<>();
        final StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream("TOPIC_NAME", Consumed.with(Serdes.String(), Serdes.String()))
            .process(runtimeErrorPunctuateProcessorSupplierMock())
            .process(processor);

        final Properties properties = new Properties();
        properties.put("processing.exception.handler", FailPunctuateProcessingExceptionHandlerMockTest.class);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(0L))) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("TOPIC_NAME", new StringSerializer(), new StringSerializer());

            final RuntimeException exception = assertThrows(RuntimeException.class,
                () -> {
                    inputTopic.pipeKeyValueList(events, Instant.EPOCH, Duration.ZERO);
                    driver.advanceWallClockTime(Duration.ofSeconds(2));
                    inputTopic.pipeInput("ID123-4", "ID123-A4", 0);
                });

            assertEquals("Exception should be handled by processing exception handler", exception.getCause().getMessage());
            assertEquals(expected.size(), processor.theCapturedProcessor().processed().size());
            assertArrayEquals(expected.toArray(), processor.theCapturedProcessor().processed().toArray());
        }
    }

    public static class ContinueProcessingExceptionHandlerMockTest implements ProcessingExceptionHandler {
        @Override
        public ProcessingExceptionHandler.ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            assertEquals("ID123-1", new String(context.sourceRawKey()));
            assertEquals("ID123-A1", new String(context.sourceRawValue()));
            assertEquals("ID123-1", record.key());
            assertEquals("ID123-A1", record.value());
            assertEquals("TOPIC_NAME", context.topic());
            assertEquals("KSTREAM-PROCESSOR-0000000001", context.processorNodeId());
            assertTrue(exception.getMessage().contains("Exception should be handled by processing exception handler"));

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
            assertEquals("ID123-1", new String(context.sourceRawKey()));
            assertEquals("ID123-A1", new String(context.sourceRawValue()));
            assertEquals("ID123-1", record.key());
            assertEquals("ID123-A1", record.value());
            assertEquals("TOPIC_NAME", context.topic());
            assertEquals("KSTREAM-PROCESSOR-0000000001", context.processorNodeId());
            assertTrue(exception.getMessage().contains("Exception should be handled by processing exception handler"));

            return ProcessingExceptionHandler.ProcessingHandlerResponse.FAIL;
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            // No-op
        }
    }

    public static class ContinuePunctuateProcessingExceptionHandlerMockTest implements ProcessingExceptionHandler {
        @Override
        public ProcessingExceptionHandler.ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            assertNull(context.sourceRawKey());
            assertNull(context.sourceRawValue());
            assertNull(record);
            assertNull(context.topic());
            assertEquals("KSTREAM-PROCESSOR-0000000001", context.processorNodeId());
            assertTrue(exception.getMessage().contains("Exception should be handled by processing exception handler"));

            return ProcessingExceptionHandler.ProcessingHandlerResponse.CONTINUE;
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            // No-op
        }
    }

    public static class FailPunctuateProcessingExceptionHandlerMockTest implements ProcessingExceptionHandler {
        @Override
        public ProcessingExceptionHandler.ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            assertNull(context.sourceRawKey());
            assertNull(context.sourceRawValue());
            assertNull(record);
            assertNull(context.topic());
            assertEquals("KSTREAM-PROCESSOR-0000000001", context.processorNodeId());
            assertTrue(exception.getMessage().contains("Exception should be handled by processing exception handler"));

            return ProcessingExceptionHandler.ProcessingHandlerResponse.FAIL;
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            // No-op
        }
    }

    /**
     * Processor supplier that throws a runtime exception on schedule.
     *
     * @return the processor supplier
     */
    private org.apache.kafka.streams.processor.api.ProcessorSupplier<String, String, String, String> runtimeErrorPunctuateProcessorSupplierMock() {
        return () -> new org.apache.kafka.streams.processor.api.Processor<String, String, String, String>() {
            org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context;

            @Override
            public void init(final org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context) {
                this.context = context;
                this.context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, ts -> {
                    throw new RuntimeException("Exception should be handled by processing exception handler");
                });
            }

            @Override
            public void process(final Record<String, String> record) {
                context.forward(record);
            }
        };
    }

    /**
     * Processor supplier that throws a runtime exception on process.
     *
     * @return the processor supplier
     */
    private ProcessorSupplier<String, String, String, String> runtimeErrorProcessorSupplierMock() {
        return () -> new Processor<String, String, String, String>() {
            org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context;

            @Override
            public void init(final org.apache.kafka.streams.processor.api.ProcessorContext<String, String> context) {
                this.context = context;
            }

            @Override
            public void process(final Record<String, String> record) {
                if (record.key().equals("ID123-1")) {
                    throw new RuntimeException("Exception should be handled by processing exception handler");
                }

                context.forward(new Record<>(record.key(), record.value(), record.timestamp()));
            }
        };
    }
}
