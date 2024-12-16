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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.streams.StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RecordDeserializerTest {
    private final String sourceNodeName = "source-node";
    private final TaskId taskId = new TaskId(0, 0);
    private final RecordHeaders headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});
    private final ConsumerRecord<byte[], byte[]> rawRecord = new ConsumerRecord<>("topic",
        1,
        1,
        10,
        TimestampType.LOG_APPEND_TIME,
        3,
        5,
        new byte[0],
        new byte[0],
        headers,
        Optional.of(5));

    private final InternalProcessorContext<Void, Void> context = new InternalMockProcessorContext<>();

    @Test
    public void shouldReturnConsumerRecordWithDeserializedValueWhenNoExceptions() {
        try (final Metrics metrics = new Metrics()) {
            final RecordDeserializer recordDeserializer = new RecordDeserializer(
                    new TheSourceNode(
                            sourceNodeName,
                            false,
                            false,
                            "key",
                            "value"
                    ),
                    null,
                    new LogContext(),
                    metrics.sensor("dropped-records")
            );
            final ConsumerRecord<Object, Object> record = recordDeserializer.deserialize(null, rawRecord);
            assertEquals(rawRecord.topic(), record.topic());
            assertEquals(rawRecord.partition(), record.partition());
            assertEquals(rawRecord.offset(), record.offset());
            assertEquals("key", record.key());
            assertEquals("value", record.value());
            assertEquals(rawRecord.timestamp(), record.timestamp());
            assertEquals(TimestampType.CREATE_TIME, record.timestampType());
            assertEquals(rawRecord.headers(), record.headers());
            assertEquals(rawRecord.leaderEpoch(), record.leaderEpoch());
        }
    }

    @ParameterizedTest
    @CsvSource({
        "true, true",
        "true, false",
        "false, true",
    })
    public void shouldThrowStreamsExceptionWhenDeserializationFailsAndExceptionHandlerRepliesWithFail(final boolean keyThrowsException,
                                                                                                      final boolean valueThrowsException) {
        try (final Metrics metrics = new Metrics()) {
            final RecordDeserializer recordDeserializer = new RecordDeserializer(
                    new TheSourceNode(
                            sourceNodeName,
                            keyThrowsException,
                            valueThrowsException,
                            "key",
                            "value"
                    ),
                    new DeserializationExceptionHandlerMock(
                            Optional.of(DeserializationExceptionHandler.Response.fail()),
                            rawRecord,
                            sourceNodeName,
                            taskId
                    ),
                    new LogContext(),
                    metrics.sensor("dropped-records")
            );

            final StreamsException e = assertThrows(StreamsException.class, () -> recordDeserializer.deserialize(context, rawRecord));
            assertEquals(
                    e.getMessage(),
                    "Deserialization exception handler is set "
                            + "to fail upon a deserialization error. "
                            + "If you would rather have the streaming pipeline "
                            + "continue after a deserialization error, please set the "
                            + DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG + " appropriately."
            );
        }
    }

    @ParameterizedTest
    @CsvSource({
        "true, true",
        "true, false",
        "false, true"
    })
    public void shouldNotThrowStreamsExceptionWhenDeserializationFailsAndExceptionHandlerRepliesWithContinue(final boolean keyThrowsException,
                                                                                                             final boolean valueThrowsException) {
        try (final Metrics metrics = new Metrics()) {
            final RecordDeserializer recordDeserializer = new RecordDeserializer(
                    new TheSourceNode(
                            sourceNodeName,
                            keyThrowsException,
                            valueThrowsException,
                            "key",
                            "value"
                    ),
                    new DeserializationExceptionHandlerMock(
                            Optional.of(DeserializationExceptionHandler.Response.resume()),
                            rawRecord,
                            sourceNodeName,
                            taskId
                    ),
                    new LogContext(),
                    metrics.sensor("dropped-records")
            );

            final ConsumerRecord<Object, Object> record = recordDeserializer.deserialize(context, rawRecord);
            assertNull(record);
        }
    }

    @Test
    public void shouldFailWhenDeserializationFailsAndExceptionHandlerReturnsNull() {
        try (final Metrics metrics = new Metrics()) {
            final RecordDeserializer recordDeserializer = new RecordDeserializer(
                    new TheSourceNode(
                            sourceNodeName,
                            true,
                            false,
                            "key",
                            "value"
                    ),
                    new DeserializationExceptionHandlerMock(
                            Optional.empty(),
                            rawRecord,
                            sourceNodeName,
                            taskId
                    ),
                    new LogContext(),
                    metrics.sensor("dropped-records")
            );

            final StreamsException exception = assertThrows(
                    StreamsException.class,
                    () -> recordDeserializer.deserialize(context, rawRecord)
            );
            assertEquals("Fatal user code error in deserialization error callback", exception.getMessage());
            assertInstanceOf(NullPointerException.class, exception.getCause());
            assertEquals("Invalid DeserializationExceptionResponse response.", exception.getCause().getMessage());
        }
    }

    @Test
    public void shouldFailWhenDeserializationFailsAndExceptionHandlerThrows() {
        try (final Metrics metrics = new Metrics()) {
            final RecordDeserializer recordDeserializer = new RecordDeserializer(
                    new TheSourceNode(
                            sourceNodeName,
                            true,
                            false,
                            "key",
                            "value"
                    ),
                    new DeserializationExceptionHandlerMock(
                            null, // indicate to throw an exception
                            rawRecord,
                            sourceNodeName,
                            taskId
                    ),
                    new LogContext(),
                    metrics.sensor("dropped-records")
            );

            final StreamsException exception = assertThrows(
                    StreamsException.class,
                    () -> recordDeserializer.deserialize(context, rawRecord)
            );
            assertEquals("Fatal user code error in deserialization error callback", exception.getMessage());
            assertEquals("CRASH", exception.getCause().getMessage());
        }
    }


    @Test
    public void shouldBuildDeadLetterQueueRecordsInDefaultDeserializationException() {
        try (Metrics metrics = new Metrics()) {
            final MockRecordCollector collector = new MockRecordCollector();
            final InternalProcessorContext<Object, Object> internalProcessorContext =
                    new InternalMockProcessorContext<>(
                            new StateSerdes<>("sink", Serdes.ByteArray(), Serdes.ByteArray()),
                            collector
                    );
            final DeserializationExceptionHandler deserializationExceptionHandler = new LogAndFailExceptionHandler();
            deserializationExceptionHandler.configure(Collections.singletonMap(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "dlq"));

            assertThrows(StreamsException.class, () -> RecordDeserializer.handleDeserializationFailure(
                    deserializationExceptionHandler,
                    internalProcessorContext,
                    new RuntimeException(new NullPointerException("Oopsie")),
                    new ConsumerRecord<>("source",
                            0,
                            0,
                            123,
                            TimestampType.CREATE_TIME,
                            -1,
                            -1,
                            "hello".getBytes(StandardCharsets.UTF_8),
                            "world".getBytes(StandardCharsets.UTF_8),
                            new RecordHeaders(),
                            Optional.empty()),
                    new LogContext().logger(this.getClass()),
                    metrics.sensor("dropped-records"),
                    "sourceNode"
            ));

            assertEquals(1, collector.collected().size());
            assertEquals("dlq", collector.collected().get(0).topic());
        }
    }


    @Test
    public void shouldBuildDeadLetterQueueRecordsInLogAndContinueDeserializationException() {
        try (Metrics metrics = new Metrics()) {
            final MockRecordCollector collector = new MockRecordCollector();
            final InternalProcessorContext<Object, Object> internalProcessorContext =
                    new InternalMockProcessorContext<>(
                            new StateSerdes<>("sink", Serdes.ByteArray(), Serdes.ByteArray()),
                            collector
                    );
            final DeserializationExceptionHandler deserializationExceptionHandler = new LogAndContinueExceptionHandler();
            deserializationExceptionHandler.configure(Collections.singletonMap(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "dlq"));

            RecordDeserializer.handleDeserializationFailure(
                    deserializationExceptionHandler,
                    internalProcessorContext,
                    new RuntimeException(new NullPointerException("Oopsie")),
                    new ConsumerRecord<>("source",
                            0,
                            0,
                            123,
                            TimestampType.CREATE_TIME,
                            -1,
                            -1,
                            "hello".getBytes(StandardCharsets.UTF_8),
                            "world".getBytes(StandardCharsets.UTF_8),
                            new RecordHeaders(),
                            Optional.empty()),
                    new LogContext().logger(this.getClass()),
                    metrics.sensor("dropped-records"),
                    "sourceNode"
            );

            assertEquals(1, collector.collected().size());
            assertEquals("dlq", collector.collected().get(0).topic());
        }
    }

    static class TheSourceNode extends SourceNode<Object, Object> {
        private final boolean keyThrowsException;
        private final boolean valueThrowsException;
        private final Object key;
        private final Object value;

        TheSourceNode(final String name,
                      final boolean keyThrowsException,
                      final boolean valueThrowsException,
                      final Object key,
                      final Object value) {
            super(name, null, null);
            this.keyThrowsException = keyThrowsException;
            this.valueThrowsException = valueThrowsException;
            this.key = key;
            this.value = value;
        }

        @Override
        public Object deserializeKey(final String topic, final Headers headers, final byte[] data) {
            if (keyThrowsException) {
                throw new RuntimeException("KABOOM!");
            }
            return key;
        }

        @Override
        public Object deserializeValue(final String topic, final Headers headers, final byte[] data) {
            if (valueThrowsException) {
                throw new RuntimeException("KABOOM!");
            }
            return value;
        }
    }

    public static class DeserializationExceptionHandlerMock implements DeserializationExceptionHandler {
        private final Optional<Response> response;
        private final ConsumerRecord<byte[], byte[]> expectedRecord;
        private final String expectedProcessorNodeId;
        private final TaskId expectedTaskId;

        public DeserializationExceptionHandlerMock(final Optional<Response> response,
                                                   final ConsumerRecord<byte[], byte[]> record,
                                                   final String processorNodeId,
                                                   final TaskId taskId) {
            this.response = response;
            this.expectedRecord = record;
            this.expectedProcessorNodeId = processorNodeId;
            this.expectedTaskId = taskId;
        }

        @Override
        public Response handleError(final ErrorHandlerContext context,
                                    final ConsumerRecord<byte[], byte[]> record,
                                    final Exception exception) {
            assertEquals(expectedRecord.topic(), context.topic());
            assertEquals(expectedRecord.partition(), context.partition());
            assertEquals(expectedRecord.offset(), context.offset());
            assertEquals(expectedProcessorNodeId, context.processorNodeId());
            assertEquals(expectedTaskId, context.taskId());
            assertEquals(expectedRecord.timestamp(), context.timestamp());
            assertEquals(expectedRecord, record);
            assertInstanceOf(RuntimeException.class, exception);
            assertEquals("KABOOM!", exception.getMessage());
            if (response == null) {
                throw new RuntimeException("CRASH");
            }
            return response.orElse(null);
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            // do nothing
        }
    }
}
