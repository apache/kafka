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
package org.apache.kafka.streams.errors;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.errors.internals.DefaultErrorHandlerContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
public class ExceptionHandlerUtilsTest {
    @Test
    public void checkDealLetterQueueRecords() {
        final StringSerializer stringSerializer = new StringSerializer();
        final StringDeserializer stringDeserializer = new StringDeserializer();
        final MockRecordCollector collector = new MockRecordCollector();
        final InternalProcessorContext<Object, Object> internalProcessorContext = new InternalMockProcessorContext<>(
                new StateSerdes<>("sink", Serdes.ByteArray(), Serdes.ByteArray()),
                collector
        );
        internalProcessorContext.setRecordContext(new ProcessorRecordContext(
                1L,
                2,
                3,
                "source",
                new RecordHeaders(Collections.singletonList(
                        new RecordHeader("sourceHeader", stringSerializer.serialize(null, "hello world"))))
        ));
        final ErrorHandlerContext errorHandlerContext = getErrorHandlerContext(internalProcessorContext);

        final NullPointerException exception = new NullPointerException("Oopsie!");
        final Iterable<ProducerRecord<byte[], byte[]>> dlqRecords = ExceptionHandlerUtils.maybeBuildDeadLetterQueueRecords("dlq", null, null, errorHandlerContext, exception);
        final Iterator<ProducerRecord<byte[], byte[]>> iterator = dlqRecords.iterator();

        assertTrue(iterator.hasNext());
        final ProducerRecord<byte[], byte[]> dlqRecord = iterator.next();
        final Headers headers = dlqRecord.headers();
        assertFalse(iterator.hasNext()); // There should be only one record

        assertEquals("dlq", dlqRecord.topic());
        assertEquals(errorHandlerContext.timestamp(), dlqRecord.timestamp());
        assertEquals(1, dlqRecord.timestamp());
        assertEquals(exception.toString(), stringDeserializer.deserialize(null, headers.lastHeader(ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_NAME).value()));
        assertEquals(exception.getMessage(), stringDeserializer.deserialize(null, headers.lastHeader(ExceptionHandlerUtils.HEADER_ERRORS_EXCEPTION_MESSAGE_NAME).value()));
        assertEquals("source", stringDeserializer.deserialize(null, headers.lastHeader(ExceptionHandlerUtils.HEADER_ERRORS_TOPIC_NAME).value()));
        assertEquals("3", stringDeserializer.deserialize(null, headers.lastHeader(ExceptionHandlerUtils.HEADER_ERRORS_PARTITION_NAME).value()));
        assertEquals("2", stringDeserializer.deserialize(null, headers.lastHeader(ExceptionHandlerUtils.HEADER_ERRORS_OFFSET_NAME).value()));
    }

    @Test
    public void doNotBuildDeadLetterQueueRecordsIfNotConfigured() {
        final NullPointerException exception = new NullPointerException("Oopsie!");
        final Iterable<ProducerRecord<byte[], byte[]>> dlqRecords = ExceptionHandlerUtils.maybeBuildDeadLetterQueueRecords(null, null, null, null, exception);
        final Iterator<ProducerRecord<byte[], byte[]>> iterator = dlqRecords.iterator();

        assertFalse(iterator.hasNext());
    }

    private static DefaultErrorHandlerContext getErrorHandlerContext(final InternalProcessorContext<Object, Object> internalProcessorContext) {
        return new DefaultErrorHandlerContext(
                null,
                internalProcessorContext.topic(),
                internalProcessorContext.partition(),
                internalProcessorContext.offset(),
                internalProcessorContext.headers(),
                internalProcessorContext.currentNode().name(),
                internalProcessorContext.taskId(),
                internalProcessorContext.timestamp());
    }
}