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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Duration;

public class SuppressImpl<K, V> implements Suppress<K, V> {
    private IntermediateSuppressionImpl<K, V> intermediateSuppression = null;
    private TimeDefinition<K, V> timeDefinition = (context, k, v) -> context.timestamp();
    private BufferConfig<K, V> finalResultsConfig;

    public static class BufferConfigImpl<K, V> implements Suppress.BufferConfig<K, V> {
        private long numberOfKeysToRemember = Long.MAX_VALUE;
        private long bytesToUseForSuppressionStorage = Long.MAX_VALUE;
        private BufferFullStrategy bufferFullStrategy = BufferFullStrategy.EMIT;

        private Serializer<K> keySerializer;
        private Serializer<V> valueSerializer;

        BufferConfigImpl() {}

        BufferConfigImpl(final BufferConfigImpl<K, V> from) {
            this.numberOfKeysToRemember = from.numberOfKeysToRemember;
            this.bytesToUseForSuppressionStorage = from.bytesToUseForSuppressionStorage;
            this.bufferFullStrategy = from.bufferFullStrategy;
            this.keySerializer = from.keySerializer;
            this.valueSerializer = from.valueSerializer;
        }

        public BufferConfig<K, V> bufferKeys(final long numberOfKeysToRemember) {
            final BufferConfigImpl<K, V> result = new BufferConfigImpl<>(this);
            result.numberOfKeysToRemember = numberOfKeysToRemember;
            return result;
        }

        public BufferConfig<K, V> bufferBytes(final long bytesToUseForSuppressionStorage, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
            final BufferConfigImpl<K, V> result = new BufferConfigImpl<>(this);
            result.keySerializer = keySerializer;
            result.valueSerializer = valueSerializer;
            result.bytesToUseForSuppressionStorage = bytesToUseForSuppressionStorage;
            return result;
        }

        public BufferConfig<K, V> bufferFullStrategy(final BufferFullStrategy bufferFullStrategy) {
            final BufferConfigImpl<K, V> result = new BufferConfigImpl<>(this);
            result.bufferFullStrategy = bufferFullStrategy;
            return result;
        }

        public long getNumberOfKeysToRemember() {
            return numberOfKeysToRemember;
        }

        public long getBytesToUseForSuppressionStorage() {
            return bytesToUseForSuppressionStorage;
        }

        public BufferFullStrategy getBufferFullStrategy() {
            return bufferFullStrategy;
        }

        public Serializer<K> getKeySerializer() {
            return keySerializer;
        }

        public Serializer<V> getValueSerializer() {
            return valueSerializer;
        }
    }

    public static class IntermediateSuppressionImpl<K, V> implements Suppress.IntermediateSuppression<K, V> {
        private BufferConfigImpl<K, V> bufferConfig = new BufferConfigImpl<>();
        private Duration timeToWaitForMoreEvents = null;

        IntermediateSuppressionImpl() {}

        private IntermediateSuppressionImpl(final IntermediateSuppressionImpl<K, V> from) {
            timeToWaitForMoreEvents = from.timeToWaitForMoreEvents;
            this.bufferConfig = from.bufferConfig;
        }

        public IntermediateSuppression<K, V> emitAfter(final Duration timeToWaitForMoreEvents) {
            final IntermediateSuppressionImpl<K, V> result = new IntermediateSuppressionImpl<K, V>(this);
            result.timeToWaitForMoreEvents = timeToWaitForMoreEvents;
            return result;
        }

        public IntermediateSuppression<K, V> bufferConfig(final BufferConfig<K, V> bufferConfig) {
            final IntermediateSuppressionImpl<K, V> result = new IntermediateSuppressionImpl<>(this);
            result.bufferConfig = (BufferConfigImpl<K, V>) bufferConfig;
            return result;
        }

        public Duration getTimeToWaitForMoreEvents() {
            return timeToWaitForMoreEvents;
        }

        public BufferConfigImpl<K, V> getBufferConfig() {
            return bufferConfig;
        }
    }

    private SuppressImpl() {}

    SuppressImpl(final BufferConfig<K, V> finalResultsConfig) {
        this.finalResultsConfig = finalResultsConfig;
    }

    SuppressImpl(final IntermediateSuppression<K, V> intermediateSuppression) {
        this.intermediateSuppression = (IntermediateSuppressionImpl<K, V>) intermediateSuppression;
    }

    public IntermediateSuppressionImpl<K, V> getIntermediateSuppression() {
        return intermediateSuppression;
    }

    public TimeDefinition<K, V> getTimeDefinition() {
        return timeDefinition;
    }

    public boolean isFinalResultsSuppression() {
        return finalResultsConfig != null;
    }

    public static <K extends Windowed, V> Suppress buildFinalResultsSuppression(final Duration windowCloseTime,
                                                                                final SuppressImpl<K, V> suppress) {
        if (suppress.finalResultsConfig == null) {
            throw new IllegalArgumentException();
        }

        final SuppressImpl<K, V> kvSuppress = new SuppressImpl<>();
        kvSuppress.timeDefinition = (ProcessorContext context, K k, V v) -> k.window().end();
        kvSuppress.intermediateSuppression = (IntermediateSuppressionImpl<K, V>) IntermediateSuppression
            .<K, V>withEmitAfter(windowCloseTime)
            .bufferConfig(suppress.finalResultsConfig);
        return kvSuppress;
    }

}
