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
package org.apache.kafka.streams.kstream.internals.suppress;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.kstream.internals.metrics.Sensors;
import org.apache.kafka.streams.kstream.internals.suppress.TimeDefinitions.TimeDefinition;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.internals.TimeOrderedKeyValueBuffer;

import static java.util.Objects.requireNonNull;

public class KTableSuppressProcessor<K, V> implements Processor<K, Change<V>> {
    private final long maxRecords;
    private final long maxBytes;
    private final long suppressDurationMillis;
    private final TimeDefinition<K> bufferTimeDefinition;
    private final BufferFullStrategy bufferFullStrategy;
    private final boolean safeToDropTombstones;
    private final String storeName;

    private TimeOrderedKeyValueBuffer<K, Change<V>> buffer;
    private InternalProcessorContext internalProcessorContext;
    private Sensor suppressionEmitSensor;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

    public KTableSuppressProcessor(final SuppressedInternal<K> suppress, final String storeName) {
        this.storeName = storeName;
        requireNonNull(suppress);
        maxRecords = suppress.bufferConfig().maxRecords();
        maxBytes = suppress.bufferConfig().maxBytes();
        suppressDurationMillis = suppress.timeToWaitForMoreEvents().toMillis();
        bufferTimeDefinition = suppress.timeDefinition();
        bufferFullStrategy = suppress.bufferConfig().bufferFullStrategy();
        safeToDropTombstones = suppress.safeToDropTombstones();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context) {
        internalProcessorContext = (InternalProcessorContext) context;
        suppressionEmitSensor = Sensors.suppressionEmitSensor(internalProcessorContext);

        buffer = requireNonNull((TimeOrderedKeyValueBuffer<K, Change<V>>) context.getStateStore(storeName));
        buffer.setSerdesIfNull((Serde<K>) context.keySerde(), FullChangeSerde.castOrWrap((Serde<V>) context.valueSerde()));
    }

    @Override
    public void process(final K key, final Change<V> value) {
        observedStreamTime = Math.max(observedStreamTime, internalProcessorContext.timestamp());
        buffer(key, value);
        enforceConstraints();
    }

    private void buffer(final K key, final Change<V> value) {
        final long bufferTime = bufferTimeDefinition.time(internalProcessorContext, key);
        buffer.put(bufferTime, key, value, internalProcessorContext.recordContext());
    }

    private void enforceConstraints() {
        final long streamTime = observedStreamTime;
        final long expiryTime = streamTime - suppressDurationMillis;

        buffer.evictWhile(() -> buffer.minTimestamp() <= expiryTime, this::emit);

        if (overCapacity()) {
            switch (bufferFullStrategy) {
                case EMIT:
                    buffer.evictWhile(this::overCapacity, this::emit);
                    return;
                case SHUT_DOWN:
                    throw new StreamsException(String.format(
                        "%s buffer exceeded its max capacity. Currently [%d/%d] records and [%d/%d] bytes.",
                        internalProcessorContext.currentNode().name(),
                        buffer.numRecords(), maxRecords,
                        buffer.bufferSize(), maxBytes
                    ));
                default:
                    throw new UnsupportedOperationException(
                        "The bufferFullStrategy [" + bufferFullStrategy +
                            "] is not implemented. This is a bug in Kafka Streams."
                    );
            }
        }
    }

    private boolean overCapacity() {
        return buffer.numRecords() > maxRecords || buffer.bufferSize() > maxBytes;
    }

    private void emit(final TimeOrderedKeyValueBuffer.Eviction<K, Change<V>> toEmit) {
        if (shouldForward(toEmit.value())) {
            final ProcessorRecordContext prevRecordContext = internalProcessorContext.recordContext();
            internalProcessorContext.setRecordContext(toEmit.recordContext());
            try {
                internalProcessorContext.forward(toEmit.key(), toEmit.value());
                suppressionEmitSensor.record();
            } finally {
                internalProcessorContext.setRecordContext(prevRecordContext);
            }
        }
    }

    private boolean shouldForward(final Change<V> value) {
        return value.newValue != null || !safeToDropTombstones;
    }

    @Override
    public void close() {
    }
}
