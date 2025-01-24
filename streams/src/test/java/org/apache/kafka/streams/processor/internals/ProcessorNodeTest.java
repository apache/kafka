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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.errors.internals.FailedProcessingException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.ROLLUP_VALUE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
public class ProcessorNodeTest {
    private static final String TOPIC = "topic";
    private static final int PARTITION = 0;
    private static final Long OFFSET = 0L;
    private static final Long TIMESTAMP = 0L;
    private static final TaskId TASK_ID = new TaskId(0, 0);
    private static final String NAME = "name";
    private static final String KEY = "key";
    private static final String VALUE = "value";

    @Test
    public void shouldThrowStreamsExceptionIfExceptionCaughtDuringInit() {
        final ProcessorNode<Object, Object, Object, Object> node =
            new ProcessorNode<>(NAME, new ExceptionalProcessor(), Collections.emptySet());
        assertThrows(StreamsException.class, () -> node.init(null));
    }

    @Test
    public void shouldThrowStreamsExceptionIfExceptionCaughtDuringClose() {
        final ProcessorNode<Object, Object, Object, Object> node =
            new ProcessorNode<>(NAME, new ExceptionalProcessor(), Collections.emptySet());
        assertThrows(StreamsException.class, () -> node.init(null));
    }

    @Test
    public void shouldThrowFailedProcessingExceptionWhenProcessingExceptionHandlerRepliesWithFail() {
        final ProcessorNode<Object, Object, Object, Object> node =
            new ProcessorNode<>(NAME, new IgnoredInternalExceptionsProcessor(), Collections.emptySet());

        final InternalProcessorContext<Object, Object> internalProcessorContext = mockInternalProcessorContext();
        node.init(internalProcessorContext, new ProcessingExceptionHandlerMock(ProcessingExceptionHandler.ProcessingHandlerResponse.FAIL, internalProcessorContext, false));

        final FailedProcessingException failedProcessingException = assertThrows(FailedProcessingException.class,
            () -> node.process(new Record<>(KEY, VALUE, TIMESTAMP)));

        assertTrue(failedProcessingException.getCause() instanceof RuntimeException);
        assertEquals("Processing exception should be caught and handled by the processing exception handler.",
            failedProcessingException.getCause().getMessage());
        assertEquals(NAME, failedProcessingException.failedProcessorNodeName());
    }

    @Test
    public void shouldNotThrowFailedProcessingExceptionWhenProcessingExceptionHandlerRepliesWithContinue() {
        final ProcessorNode<Object, Object, Object, Object> node =
            new ProcessorNode<>(NAME, new IgnoredInternalExceptionsProcessor(), Collections.emptySet());

        final InternalProcessorContext<Object, Object> internalProcessorContext = mockInternalProcessorContext();
        node.init(internalProcessorContext, new ProcessingExceptionHandlerMock(ProcessingExceptionHandler.ProcessingHandlerResponse.CONTINUE, internalProcessorContext, false));

        assertDoesNotThrow(() -> node.process(new Record<>(KEY, VALUE, TIMESTAMP)));
    }

    @ParameterizedTest
    @CsvSource({
        "FailedProcessingException,java.lang.RuntimeException,Fail processing",
        "TaskCorruptedException,org.apache.kafka.streams.processor.internals.ProcessorNodeTest$IgnoredInternalExceptionsProcessor$1,Invalid offset",
        "TaskMigratedException,java.lang.RuntimeException,Task migrated cause"
    })
    public void shouldNotHandleInternalExceptionsThrownDuringProcessing(final String ignoredExceptionName,
                                                                        final Class<?> ignoredExceptionCause,
                                                                        final String ignoredExceptionCauseMessage) {
        final ProcessingExceptionHandler processingExceptionHandler = mock(ProcessingExceptionHandler.class);

        final ProcessorNode<Object, Object, Object, Object> node =
            new ProcessorNode<>(NAME, new IgnoredInternalExceptionsProcessor(), Collections.emptySet());

        final InternalProcessorContext<Object, Object> internalProcessorContext = mockInternalProcessorContext();
        node.init(internalProcessorContext, processingExceptionHandler);

        final RuntimeException runtimeException = assertThrows(RuntimeException.class,
            () -> node.process(new Record<>(ignoredExceptionName, VALUE, TIMESTAMP)));

        assertEquals(ignoredExceptionCause, runtimeException.getCause().getClass());
        assertEquals(ignoredExceptionCauseMessage, runtimeException.getCause().getMessage());
        verify(processingExceptionHandler, never()).handle(any(), any(), any());
    }

    @Test
    public void shouldThrowFailedProcessingExceptionWhenProcessingExceptionHandlerThrowsAnException() {
        final ProcessorNode<Object, Object, Object, Object> node =
                new ProcessorNode<>(NAME, new IgnoredInternalExceptionsProcessor(), Collections.emptySet());

        final InternalProcessorContext<Object, Object> internalProcessorContext = mockInternalProcessorContext();
        node.init(internalProcessorContext, new ProcessingExceptionHandlerMock(ProcessingExceptionHandler.ProcessingHandlerResponse.CONTINUE, internalProcessorContext, true));

        final FailedProcessingException failedProcessingException = assertThrows(FailedProcessingException.class,
            () -> node.process(new Record<>(KEY, VALUE, TIMESTAMP)));

        assertInstanceOf(RuntimeException.class, failedProcessingException.getCause());
        assertEquals("KABOOM!", failedProcessingException.getCause().getMessage());
        assertEquals(NAME, failedProcessingException.failedProcessorNodeName());
    }

    private static class ExceptionalProcessor implements Processor<Object, Object, Object, Object> {
        @Override
        public void init(final ProcessorContext<Object, Object> context) {
            throw new RuntimeException();
        }

        @Override
        public void process(final Record<Object, Object> record) {
            throw new RuntimeException();
        }

        @Override
        public void close() {
            throw new RuntimeException();
        }
    }

    private static class NoOpProcessor implements Processor<Object, Object, Object, Object> {
        @Override
        public void process(final Record<Object, Object> record) {
        }
    }

    private static class IgnoredInternalExceptionsProcessor implements Processor<Object, Object, Object, Object> {
        @Override
        public void process(final Record<Object, Object> record) {
            if (record.key().equals("FailedProcessingException")) {
                throw new FailedProcessingException(NAME, new RuntimeException("Fail processing"));
            }

            if (record.key().equals("TaskCorruptedException")) {
                final Set<TaskId> tasksIds = new HashSet<>();
                tasksIds.add(new TaskId(0, 0));
                throw new TaskCorruptedException(tasksIds, new InvalidOffsetException("Invalid offset") {
                    @Override
                    public Set<TopicPartition> partitions() {
                        return new HashSet<>(Collections.singletonList(new TopicPartition("topic", 0)));
                    }
                });
            }

            if (record.key().equals("TaskMigratedException")) {
                throw new TaskMigratedException("TaskMigratedException", new RuntimeException("Task migrated cause"));
            }

            throw new RuntimeException("Processing exception should be caught and handled by the processing exception handler.");
        }
    }

    @Test
    public void testMetricsWithBuiltInMetricsVersionLatest() {
        final Metrics metrics = new Metrics();
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test-client", "processId", new MockTime());
        final InternalMockProcessorContext<Object, Object> context = new InternalMockProcessorContext<>(streamsMetrics);
        final ProcessorNode<Object, Object, Object, Object> node =
            new ProcessorNode<>(NAME, new NoOpProcessor(), Collections.emptySet());
        node.init(context);

        final String threadId = Thread.currentThread().getName();
        final String[] latencyOperations = {"process", "punctuate", "create", "destroy"};
        final String groupName = "stream-processor-node-metrics";
        final Map<String, String> metricTags = new LinkedHashMap<>();
        final String threadIdTagKey = "client-id";
        metricTags.put("processor-node-id", node.name());
        metricTags.put("task-id", context.taskId().toString());
        metricTags.put(threadIdTagKey, threadId);

        for (final String opName : latencyOperations) {
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-latency-avg", groupName, metricTags));
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-latency-max", groupName, metricTags));
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-rate", groupName, metricTags));
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-total", groupName, metricTags));
        }

        // test parent sensors
        metricTags.put("processor-node-id", ROLLUP_VALUE);
        for (final String opName : latencyOperations) {
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-latency-avg", groupName, metricTags));
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-latency-max", groupName, metricTags));
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-rate", groupName, metricTags));
            assertFalse(StreamsTestUtils.containsMetric(metrics, opName + "-total", groupName, metricTags));
        }
    }

    @Test
    public void testTopologyLevelClassCastException() {
        // Serdes configuration is missing and no default is set which will trigger an exception
        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream("streams-plaintext-input")
            .flatMapValues(value -> Collections.singletonList(""));
        final Topology topology = builder.build();
        final Properties config = new Properties();
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, config)) {
            final TestInputTopic<String, String> topic = testDriver.createInputTopic("streams-plaintext-input", new StringSerializer(), new StringSerializer());

            final StreamsException se = assertThrows(StreamsException.class, () -> topic.pipeInput(KEY, VALUE));
            final String msg = se.getMessage();
            assertTrue(msg.contains("ClassCastException"), "Error about class cast with serdes");
            assertTrue(msg.contains("Serdes"), "Error about class cast with serdes");
        }
    }

    @Test
    public void testTopologyLevelConfigException() {
        // Serdes configuration is missing and no default is set which will trigger an exception
        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream("streams-plaintext-input")
            .flatMapValues(value -> Collections.singletonList(""));
        final Topology topology = builder.build();

        final StreamsException se = assertThrows(StreamsException.class, () -> new TopologyTestDriver(topology));
        assertTrue(se.getMessage().contains("Failed to initialize key serdes for source node"));
        assertTrue(se.getCause().getMessage().contains("Please specify a key serde or set one through StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG"));
    }

    private static class ClassCastProcessor extends ExceptionalProcessor {

        @Override
        public void init(final ProcessorContext<Object, Object> context) {
        }

        @Override
        public void process(final Record<Object, Object> record) {
            throw new ClassCastException("Incompatible types simulation exception.");
        }
    }

    @Test
    public void testTopologyLevelClassCastExceptionDirect() {
        final Metrics metrics = new Metrics();
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test-client", "processId", new MockTime());
        final InternalMockProcessorContext<Object, Object> context = new InternalMockProcessorContext<>(streamsMetrics);
        final ProcessorNode<Object, Object, Object, Object> node =
            new ProcessorNode<>("pname", new ClassCastProcessor(), Collections.emptySet());
        node.init(context);
        final StreamsException se = assertThrows(
            StreamsException.class,
            () -> node.process(new Record<>(KEY, VALUE, TIMESTAMP))
        );
        assertTrue(se.getCause() instanceof ClassCastException);
        assertTrue(se.getMessage().contains("default Serdes"));
        assertTrue(se.getMessage().contains("input types"));
        assertTrue(se.getMessage().contains("pname"));
    }

    @SuppressWarnings("unchecked")
    private InternalProcessorContext<Object, Object> mockInternalProcessorContext() {
        final InternalProcessorContext<Object, Object> internalProcessorContext = mock(InternalProcessorContext.class, withSettings().strictness(Strictness.LENIENT));

        when(internalProcessorContext.taskId()).thenReturn(TASK_ID);
        when(internalProcessorContext.metrics()).thenReturn(new StreamsMetricsImpl(new Metrics(), "test-client", "processId", new MockTime()));
        when(internalProcessorContext.topic()).thenReturn(TOPIC);
        when(internalProcessorContext.partition()).thenReturn(PARTITION);
        when(internalProcessorContext.offset()).thenReturn(OFFSET);
        when(internalProcessorContext.recordContext()).thenReturn(
            new ProcessorRecordContext(
                TIMESTAMP,
                OFFSET,
                PARTITION,
                TOPIC,
                new RecordHeaders()));
        when(internalProcessorContext.currentNode()).thenReturn(new ProcessorNode<>(NAME));

        return internalProcessorContext;
    }

    public static class ProcessingExceptionHandlerMock implements ProcessingExceptionHandler {
        private final ProcessingExceptionHandler.ProcessingHandlerResponse response;
        private final InternalProcessorContext<Object, Object> internalProcessorContext;

        private final boolean shouldThrowException;

        public ProcessingExceptionHandlerMock(final ProcessingExceptionHandler.ProcessingHandlerResponse response,
                                              final InternalProcessorContext<Object, Object> internalProcessorContext,
                                              final boolean shouldThrowException) {
            this.response = response;
            this.internalProcessorContext = internalProcessorContext;
            this.shouldThrowException = shouldThrowException;
        }

        @Override
        public ProcessingExceptionHandler.ProcessingHandlerResponse handle(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            assertEquals(internalProcessorContext.topic(), context.topic());
            assertEquals(internalProcessorContext.partition(), context.partition());
            assertEquals(internalProcessorContext.offset(), context.offset());
            assertEquals(internalProcessorContext.currentNode().name(), context.processorNodeId());
            assertEquals(internalProcessorContext.taskId(), context.taskId());
            assertEquals(internalProcessorContext.timestamp(), context.timestamp());
            assertEquals(KEY, record.key());
            assertEquals(VALUE, record.value());
            assertInstanceOf(RuntimeException.class, exception);
            assertEquals("Processing exception should be caught and handled by the processing exception handler.", exception.getMessage());

            if (shouldThrowException) {
                throw new RuntimeException("KABOOM!");
            }
            return response;
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            // No-op
        }
    }
}
