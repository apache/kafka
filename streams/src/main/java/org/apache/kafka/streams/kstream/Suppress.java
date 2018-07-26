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
import org.apache.kafka.streams.kstream.SuppressImpl.BufferConfigImpl;
import org.apache.kafka.streams.kstream.SuppressImpl.IntermediateSuppressionImpl;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Duration;

@SuppressWarnings("unused") // complaint about the top-level generic parameters, which actually are necessary
public interface Suppress<K, V> {

    interface TimeDefinition<K, V> {
        long time(ProcessorContext context, K k, V v);
    }

    enum BufferFullStrategy {
        EMIT,
        SPILL_TO_DISK,
        SHUT_DOWN
    }

    interface BufferConfig<K, V> {
        static <K, V> BufferConfig<K, V> withBufferKeys(final long numberOfKeysToRemember) {
            final BufferConfig<K, V> bufferConfig = new BufferConfigImpl<>();
            return bufferConfig.bufferKeys(numberOfKeysToRemember);
        }

        BufferConfig<K, V> bufferKeys(final long numberOfKeysToRemember);

        static <K, V> BufferConfig<K, V> withBufferBytes(final long bytesToUseForSuppressionStorage, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
            final BufferConfig<K, V> bufferConfig = new BufferConfigImpl<>();
            return bufferConfig.bufferBytes(bytesToUseForSuppressionStorage, keySerializer, valueSerializer);
        }

        BufferConfig<K, V> bufferBytes(final long bytesToUseForSuppressionStorage, final Serializer<K> keySerializer, final Serializer<V> valueSerializer);

        static <K, V> BufferConfig<K, V> withBufferFullStrategy(final BufferFullStrategy bufferFullStrategy) {
            return new BufferConfigImpl<K, V>().bufferFullStrategy(bufferFullStrategy);
        }

        BufferConfig<K, V> bufferFullStrategy(final BufferFullStrategy bufferFullStrategy);
    }

    interface IntermediateSuppression<K, V> {
        static <K, V> IntermediateSuppression<K, V> withEmitAfter(final Duration timeToWaitForMoreEvents) {
            final IntermediateSuppression<K, V> intermediateSuppression = new IntermediateSuppressionImpl<>();
            return intermediateSuppression.emitAfter(timeToWaitForMoreEvents);
        }

        IntermediateSuppression<K, V> emitAfter(final Duration timeToWaitForMoreEvents);

        static <K, V> IntermediateSuppression<K, V> withBufferConfig(final BufferConfig<K, V> bufferConfig) {
            final IntermediateSuppression<K, V> intermediateSuppression = new IntermediateSuppressionImpl<>();
            return intermediateSuppression.bufferConfig(bufferConfig);
        }

        IntermediateSuppression<K, V> bufferConfig(final BufferConfig<K, V> bufferConfig);
    }

    static <K extends Windowed, V> Suppress<K, V> emitFinalResultsOnly(final BufferConfig<K, V> bufferConfig) {
        if (((BufferConfigImpl<K, V>) bufferConfig).getBufferFullStrategy() == BufferFullStrategy.EMIT) {
            throw new IllegalArgumentException(
                "The EMIT strategy may produce intermediate results. " +
                    "Select either SHUT_DOWN or SPILL_TO_DISK"
            );
        }

        return new SuppressImpl<>(bufferConfig);
    }

    static <K, V> Suppress<K, V> intermediateEvents(final IntermediateSuppression<K, V> intermediateSuppression) {
        return new SuppressImpl<>(intermediateSuppression);
    }
}
