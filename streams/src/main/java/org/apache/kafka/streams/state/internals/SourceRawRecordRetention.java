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

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decides whether record caches and suppress buffers keep the source raw key and value of the records they hold.
 * <p>
 * The source raw record is only read by an exception handler that builds a dead-letter queue record, so it is
 * kept if {@link StreamsConfig#ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG} is set or if a processing or production
 * exception handler other than the built-in ones is configured. Otherwise held records keep {@code null}, as
 * records forwarded from a punctuation do.
 */
final class SourceRawRecordRetention {

    // the handlers that can run when a held record is forwarded, including the deprecated production handler config
    @SuppressWarnings("deprecation")
    private static final List<String> FLUSH_TIME_HANDLER_CONFIGS = List.of(
        StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
        StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
        StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG
    );

    private static final Set<String> BUILT_IN_HANDLERS = Set.of(
        LogAndFailProcessingExceptionHandler.class.getName(),
        LogAndContinueProcessingExceptionHandler.class.getName(),
        DefaultProductionExceptionHandler.class.getName()
    );

    private SourceRawRecordRetention() {
    }

    /**
     * @param appConfigs the application configs, as returned by {@code StateStoreContext#appConfigs()}
     * @return whether held records should keep their source raw record
     */
    static boolean shouldRetain(final Map<String, Object> appConfigs) {
        final Object deadLetterQueueTopic = appConfigs.get(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);
        if (deadLetterQueueTopic != null && !deadLetterQueueTopic.toString().isBlank()) {
            return true;
        }
        for (final String handlerConfig : FLUSH_TIME_HANDLER_CONFIGS) {
            final Object handler = appConfigs.get(handlerConfig);
            if (handler != null && !BUILT_IN_HANDLERS.contains(className(handler))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Copies {@code recordContext} for a buffer to hold, with its source raw record only if {@code retain}.
     * The copy is unaffected by {@link ProcessorRecordContext#freeRawRecord()} on the original, so its size
     * estimate is the same when it is added and when it is removed.
     */
    static ProcessorRecordContext forBuffering(final ProcessorRecordContext recordContext, final boolean retain) {
        return new ProcessorRecordContext(
            recordContext.timestamp(),
            recordContext.offset(),
            recordContext.partition(),
            recordContext.topic(),
            recordContext.headers(),
            retain ? recordContext.sourceRawKey() : null,
            retain ? recordContext.sourceRawValue() : null
        );
    }

    // a class config is either a Class or its fully qualified name
    private static String className(final Object handler) {
        return handler instanceof Class<?> ? ((Class<?>) handler).getName() : handler.toString().trim();
    }
}
