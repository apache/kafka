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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.FullChangeSerde;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import java.time.Duration;
import java.util.Iterator;

import static java.util.Objects.requireNonNull;

public class KTableSuppressProcessor<K, V> implements Processor<K, Change<V>> {
    private final SuppressedImpl<K> suppress;
    private final long maxRecords;
    private final long maxBytes;
    private final Duration suppressDuration;
    private InternalProcessorContext internalProcessorContext;
    private TimeOrderedKeyValueBuffer<K, ContextualRecord<Change<V>>> buffer;

    private ProcessorNode myNode;
    private Serde<K> keySerde;
    private Serde<Change<V>> valueSerde;

    public KTableSuppressProcessor(final SuppressedImpl<K> suppress,
                                   final Serde<K> keySerde,
                                   final Serde<Change<V>> valueSerde) {
        this.suppress = requireNonNull(suppress);
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        maxRecords = suppress.getBufferConfig().maxRecords();
        maxBytes = suppress.getBufferConfig().maxBytes();
        suppressDuration = suppress.getTimeToWaitForMoreEvents();
    }

    private void ensureBuffer() {
        if (buffer == null) {
            buffer = new InMemoryTimeOrderedKeyValueBuffer<>();

            internalProcessorContext.schedule(
                1L,
                PunctuationType.STREAM_TIME,
                this::evictOldEnoughRecords
            );
        }
    }

    @Override
    public void init(final ProcessorContext context) {
        internalProcessorContext = (InternalProcessorContext) context;
        myNode = internalProcessorContext.currentNode();
        this.keySerde = keySerde == null ? castSerde(context.keySerde(), "key") : keySerde;
        this.valueSerde =
            valueSerde == null ? new FullChangeSerde<>(castSerde(context.valueSerde(), "value")) : valueSerde;
    }

    @Override
    public void process(final K key, final Change<V> value) {
        if (suppress.getTimeToWaitForMoreEvents() == Duration.ZERO && definedRecordTime(key) <= internalProcessorContext.streamTime()) {
            if (shouldForward(value)) {
                internalProcessorContext.forward(key, value);
            } // else skip
        } else {
            ensureBuffer();
            buffer(key, value);
            enforceSizeBound();
        }
    }

    @SuppressWarnings("unchecked")
    private <T> Serde<T> castSerde(final Serde<?> untyped, final String keyOrValue) {
        try {
            return (Serde<T>) untyped;
        } catch (final ClassCastException e) {
            throw new TopologyException(
                "Unable to use the configured default " + keyOrValue +
                    " serde. Ensure that the correct " + keyOrValue + " serde is set upstream of " + myNode + ".",
                e
            );
        }
    }

    private long definedRecordTime(final K key) {
        return suppress.getTimeDefinition().time(internalProcessorContext, key);
    }

    private void buffer(final K key, final Change<V> value) {
        final long time = suppress.getTimeDefinition().time(internalProcessorContext, key);
        final ProcessorRecordContext recordContext = internalProcessorContext.recordContext();

        buffer.put(time, key, new ContextualRecord<>(time, value, recordContext), computeRecordSize(key, value, recordContext));
    }

    private void evictOldEnoughRecords(final long streamTime) {
        System.out.println("st: " + streamTime);
        final Iterator<KeyValue<TimeKey<K>, ContextualRecord<Change<V>>>> iterator = buffer.iterator();
        while (iterator.hasNext()) {
            final KeyValue<TimeKey<K>, ContextualRecord<Change<V>>> next = iterator.next();
            final long bufferTime = next.key.time();
            if (bufferTime <= streamTime - suppressDuration.toMillis()) {
                final K key = next.key.key();
                final ContextualRecord<Change<V>> contextualRecord = next.value;

                setNodeAndForward(key, contextualRecord);
                iterator.remove();
            } else {
                break;
            }
        }
        System.out.println(buffer);
    }

    private void enforceSizeBound() {
        if (overCapacity()) {
            switch (suppress.getBufferConfig().bufferFullStrategy()) {
                case EMIT:
                    final Iterator<KeyValue<TimeKey<K>, ContextualRecord<Change<V>>>> iterator = buffer.iterator();
                    while (overCapacity() && iterator.hasNext()) {
                        final KeyValue<TimeKey<K>, ContextualRecord<Change<V>>> next = iterator.next();
                        final K key = next.key.key();
                        final ContextualRecord<Change<V>> contextualRecord = next.value;
                        // to be safe, forward before we delete
                        setNodeAndForward(key, contextualRecord);
                        iterator.remove();
                    }
                    return;
                case SHUT_DOWN:
                    throw new RuntimeException("TODO: request graceful shutdown"); // TODO: request graceful shutdown
            }
        }
    }

    private boolean overCapacity() {
        return buffer.numRecords() > maxRecords || buffer.bufferSize() > maxBytes;
    }

    private void setNodeAndForward(final K key, final ContextualRecord<Change<V>> contextualRecord) {
        if (shouldForward(contextualRecord.value())) {
            final ProcessorNode prevNode = internalProcessorContext.currentNode();
            final ProcessorRecordContext prevRecordContext = internalProcessorContext.recordContext();
            internalProcessorContext.setRecordContext(contextualRecord.recordContext());
            internalProcessorContext.setCurrentNode(myNode);
            try {
                internalProcessorContext.forward(key, contextualRecord.value());
            } finally {
                internalProcessorContext.setCurrentNode(prevNode);
                internalProcessorContext.setRecordContext(prevRecordContext);
            }
        }
    }

    private boolean shouldForward(final Change<V> value) {
        return !(value.newValue == null && suppress.shouldSuppressTombstones());
    }

    private long computeRecordSize(final K key, final Change<V> value, final ProcessorRecordContext recordContext) {
        long size = 0L;
        size += keySerde.serializer().serialize(null, key).length;
        size += valueSerde.serializer().serialize(null, value).length;
        size += 8; // timestamp
        size += 8; // offset
        size += 4; // partition
        size += recordContext.topic().toCharArray().length;
        if (recordContext.headers() != null) {
            for (final Header header : recordContext.headers()) {
                size += header.key().toCharArray().length;
                size += header.value().length;
            }
        }
        return size;
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        return "KTableSuppressProcessor{suppress=" + suppress + ", keySerde=" + keySerde + ", valueSerde=" + valueSerde + '}';
    }
}