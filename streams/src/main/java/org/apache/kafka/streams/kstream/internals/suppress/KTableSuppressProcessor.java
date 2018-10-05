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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.kstream.internals.suppress.TimeDefinitions.TimeDefinition;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.internals.ContextualRecord;

import static java.util.Objects.requireNonNull;

public class KTableSuppressProcessor<K, V> implements Processor<K, Change<V>> {
    private final long maxRecords;
    private final long maxBytes;
    private final long suppressDurationMillis;
    private final TimeOrderedKeyValueBuffer buffer;
    private final TimeDefinition<K> bufferTimeDefinition;
    private final BufferFullStrategy bufferFullStrategy;
    private final boolean shouldSuppressTombstones;
    private InternalProcessorContext internalProcessorContext;

    private Serde<K> keySerde;
    private Serde<Change<V>> valueSerde;

    public KTableSuppressProcessor(final SuppressedInternal<K> suppress,
                                   final Serde<K> keySerde,
                                   final FullChangeSerde<V> valueSerde) {
        requireNonNull(suppress);
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        maxRecords = suppress.getBufferConfig().maxRecords();
        maxBytes = suppress.getBufferConfig().maxBytes();
        suppressDurationMillis = suppress.getTimeToWaitForMoreEvents().toMillis();
        buffer = new InMemoryTimeOrderedKeyValueBuffer();
        bufferTimeDefinition = suppress.getTimeDefinition();
        bufferFullStrategy = suppress.getBufferConfig().bufferFullStrategy();
        shouldSuppressTombstones = suppress.shouldSuppressTombstones();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(final ProcessorContext context) {
        internalProcessorContext = (InternalProcessorContext) context;
        this.keySerde = keySerde == null ? (Serde<K>) context.keySerde() : keySerde;
        this.valueSerde = valueSerde == null ? FullChangeSerde.castOrWrap(context.valueSerde()) : valueSerde;
    }

    @Override
    public void process(final K key, final Change<V> value) {
        buffer(key, value);
        enforceConstraints();
    }

    private void buffer(final K key, final Change<V> value) {
        final long bufferTime = bufferTimeDefinition.time(internalProcessorContext, key);
        final ProcessorRecordContext recordContext = internalProcessorContext.recordContext();

        final Bytes serializedKey = Bytes.wrap(keySerde.serializer().serialize(null, key));
        final byte[] serializedValue = valueSerde.serializer().serialize(null, value);

        buffer.put(bufferTime, serializedKey, new ContextualRecord(serializedValue, recordContext));
    }

    private void enforceConstraints() {
        final long streamTime = internalProcessorContext.streamTime();
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
            }
        }
    }

    private boolean overCapacity() {
        return buffer.numRecords() > maxRecords || buffer.bufferSize() > maxBytes;
    }

    private void emit(final KeyValue<Bytes, ContextualRecord> toEmit) {
        final Change<V> value = valueSerde.deserializer().deserialize(null, toEmit.value.value());
        if (shouldForward(value)) {
            final ProcessorRecordContext prevRecordContext = internalProcessorContext.recordContext();
            internalProcessorContext.setRecordContext(toEmit.value.recordContext());
            try {
                final K key = keySerde.deserializer().deserialize(null, toEmit.key.get());
                internalProcessorContext.forward(key, value);
            } finally {
                internalProcessorContext.setRecordContext(prevRecordContext);
            }
        }
    }

    private boolean shouldForward(final Change<V> value) {
        return !(value.newValue == null && shouldSuppressTombstones);
    }

    @Override
    public void close() {
    }
}