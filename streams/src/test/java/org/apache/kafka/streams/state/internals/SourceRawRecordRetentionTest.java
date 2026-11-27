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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SourceRawRecordRetentionTest {

    @Test
    public void shouldNotRetainWithNothingConfigured() {
        assertFalse(SourceRawRecordRetention.shouldRetain(Collections.emptyMap()));
    }

    @Test
    public void shouldNotRetainWithStreamsDefaults() {
        // appConfigs() fills in every default, including the built-in handlers as classes
        final Properties props = StreamsTestUtils.getStreamsConfig();
        assertFalse(SourceRawRecordRetention.shouldRetain(appConfigs(props)));
    }

    @Test
    public void shouldRetainWithDeadLetterQueue() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "dlq");
        assertTrue(SourceRawRecordRetention.shouldRetain(appConfigs(props)));
    }

    @Test
    public void shouldNotRetainWithBlankDeadLetterQueue() {
        assertFalse(SourceRawRecordRetention.shouldRetain(
            config(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, " ")));
    }

    @Test
    public void shouldNotRetainWithBuiltInHandlersAsClassesOrNames() {
        assertFalse(SourceRawRecordRetention.shouldRetain(
            config(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueProcessingExceptionHandler.class)));
        assertFalse(SourceRawRecordRetention.shouldRetain(
            config(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndFailProcessingExceptionHandler.class.getName())));
        assertFalse(SourceRawRecordRetention.shouldRetain(
            config(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, DefaultProductionExceptionHandler.class)));
    }

    @Test
    public void shouldRetainWithCustomProcessingHandler() {
        assertTrue(SourceRawRecordRetention.shouldRetain(
            config(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, CustomProcessingExceptionHandler.class)));
        assertTrue(SourceRawRecordRetention.shouldRetain(
            config(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, CustomProcessingExceptionHandler.class.getName())));
    }

    @Test
    public void shouldRetainWithCustomProcessingHandlerInFullConfig() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, CustomProcessingExceptionHandler.class);
        assertTrue(SourceRawRecordRetention.shouldRetain(appConfigs(props)));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldRetainWithCustomProductionHandler() {
        assertTrue(SourceRawRecordRetention.shouldRetain(
            config(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, "com.example.CustomProductionExceptionHandler")));
        assertTrue(SourceRawRecordRetention.shouldRetain(
            config(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, "com.example.CustomProductionExceptionHandler")));
    }

    @Test
    public void shouldIgnoreDeserializationHandlers() {
        // deserialization handlers never run during a cache flush
        assertFalse(SourceRawRecordRetention.shouldRetain(
            config(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, "com.example.CustomDeserializationExceptionHandler")));
        assertFalse(SourceRawRecordRetention.shouldRetain(
            config(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class)));
    }

    @Test
    public void shouldCopyContextForBufferingWithOrWithoutSourceRawRecord() {
        final byte[] rawKey = new byte[] {1};
        final byte[] rawValue = new byte[] {2, 3};
        final RecordHeaders headers = new RecordHeaders();
        final ProcessorRecordContext context = new ProcessorRecordContext(10L, 20L, 3, "topic", headers, rawKey, rawValue);

        final ProcessorRecordContext retained = SourceRawRecordRetention.forBuffering(context, true);
        final ProcessorRecordContext dropped = SourceRawRecordRetention.forBuffering(context, false);

        for (final ProcessorRecordContext copy : new ProcessorRecordContext[] {retained, dropped}) {
            assertNotSame(context, copy);
            assertEquals(10L, copy.timestamp());
            assertEquals(20L, copy.offset());
            assertEquals(3, copy.partition());
            assertEquals("topic", copy.topic());
            assertSame(headers, copy.headers());
        }
        assertSame(rawKey, retained.sourceRawKey());
        assertSame(rawValue, retained.sourceRawValue());
        assertNull(dropped.sourceRawKey());
        assertNull(dropped.sourceRawValue());

        // a held copy is unaffected when the caller's context frees its raw record
        context.freeRawRecord();
        assertSame(rawValue, retained.sourceRawValue());
    }

    // the same map StateStoreContext#appConfigs() builds: originals overlaid with parsed values
    private static Map<String, Object> appConfigs(final Properties props) {
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> combined = new HashMap<>(streamsConfig.originals());
        combined.putAll(streamsConfig.values());
        return combined;
    }

    private static Map<String, Object> config(final String key, final Object value) {
        return Collections.singletonMap(key, value);
    }

    public static class CustomProcessingExceptionHandler implements ProcessingExceptionHandler {
        @Override
        public Response handleError(final ErrorHandlerContext context, final Record<?, ?> record, final Exception exception) {
            return Response.fail();
        }

        @Override
        public void configure(final Map<String, ?> configs) {
        }
    }
}
