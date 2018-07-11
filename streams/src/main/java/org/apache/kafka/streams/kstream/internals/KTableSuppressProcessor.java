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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.Suppress;
import org.apache.kafka.streams.kstream.SuppressImpl;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

public class KTableSuppressProcessor<K, V> implements Processor<K, V> {
    private final SuppressImpl<K, V> suppress;

    private final Map<K, TimeKey<K>> index = new HashMap<>();
    private final TreeMap<TimeKey<K>, ContextualRecord<V>> sortedMap = new TreeMap<>();

    private InternalProcessorContext internalProcessorContext;
    private long memBufferSize;
    private ProcessorNode myNode;
    private final Serializer<Change<V>> valueSerializer;

    private static class TimeKey<K> implements Comparable<TimeKey<K>> {
        private final long time;
        private final K key;

        private TimeKey(final long time, final K key) {

            this.time = time;
            this.key = key;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final TimeKey<?> timeKey = (TimeKey<?>) o;
            return time == timeKey.time &&
                Objects.equals(key, timeKey.key);
        }

        @Override
        public int hashCode() {

            return Objects.hash(time, key);
        }

        @Override
        public int compareTo(final TimeKey<K> o) {
            // ordering of keys within a time uses hashCode.
            final int timeComparison = Long.compare(time, o.time);
            return timeComparison == 0 ? Integer.compare(key.hashCode(), o.key.hashCode()) : timeComparison;
        }

        @Override
        public String toString() {
            return "TimeKey{time=" + time + ", key=" + key + '}';
        }
    }

    private static class ContextualRecord<V> {
        private final long time;
        private final V value;
        private final ProcessorRecordContext recordContext;
        private final long size;

        private ContextualRecord(final long time, final V value, final ProcessorRecordContext recordContext, final long size) {
            this.time = time;
            this.value = value;
            this.recordContext = recordContext;
            this.size = size;
        }

        @Override
        public String toString() {
            return "ContextualRecord{value=" + value + ", time=" + time + ", size=" + Objects.toString(size) + '}';
        }
    }

    KTableSuppressProcessor(final Suppress<K, V> suppress) {
        this.suppress = asSuppressImpl(suppress);
        valueSerializer = getValueSerializer(this.suppress);
    }

    @SuppressWarnings("unchecked")
    private SuppressImpl<K, V> asSuppressImpl(final Suppress<K, V> suppress) {
        return (SuppressImpl<K, V>) suppress;
    }

    @Override
    public void init(final ProcessorContext context) {
        internalProcessorContext = (InternalProcessorContext) context;
        myNode = internalProcessorContext.currentNode();


        if (intermediateSuppression()) {
            final Duration timeToWaitForMoreEvents = suppress.getIntermediateSuppression().getTimeToWaitForMoreEvents();
            if (timeToWaitForMoreEvents != null && timeToWaitForMoreEvents.toMillis() > 0) {
                final long evictionTimeout = timeToWaitForMoreEvents.toMillis();

                internalProcessorContext.schedule(
                    1L,
                    PunctuationType.STREAM_TIME,
                    streamTime -> {
                        System.out.println(streamTime + " " + (streamTime - evictionTimeout));
                        final Set<Map.Entry<TimeKey<K>, ContextualRecord<V>>> entries = sortedMap.entrySet();
                        final Iterator<Map.Entry<TimeKey<K>, ContextualRecord<V>>> iterator = entries.iterator();

                        while (iterator.hasNext()) {
                            final Map.Entry<TimeKey<K>, ContextualRecord<V>> next = iterator.next();
                            final long recordTime = next.getValue().time;
                            System.out.println(streamTime + " " + next);
                            if (recordTime < streamTime - evictionTimeout) {
                                setNodeAndForward(next);
                                iterator.remove();
                                index.remove(next.getKey().key);
                            } else {
                                break;
                            }
                        }
                    });
            }
        }
    }

    private void setNodeAndForward(final Map.Entry<TimeKey<K>, ContextualRecord<V>> next) {
        final ProcessorNode prevNode = internalProcessorContext.currentNode();
        final ProcessorRecordContext prevRecordContext = internalProcessorContext.recordContext();
        internalProcessorContext.setRecordContext(next.getValue().recordContext);
        internalProcessorContext.setCurrentNode(myNode);
        try {
            internalProcessorContext.forward(next.getKey().key, next.getValue().value);
        } finally {
            internalProcessorContext.setCurrentNode(prevNode);
            internalProcessorContext.setRecordContext(prevRecordContext);
        }
    }

    @Override
    public void process(final K key, final V value) {
        final long time = suppress.getTimeDefinition().time(internalProcessorContext, key, value);

        if (intermediateSuppression() && (nonTimeBoundSuppression() || nonInstantaneousTimeBoundSuppression())) {
            final TimeKey<K> previousTimeKey = index.remove(key);
            if (previousTimeKey != null) {
                final ContextualRecord<V> previous = sortedMap.remove(previousTimeKey);
                if (previous != null) {
                    memBufferSize = memBufferSize - previous.size;
                }
            }

            final ProcessorRecordContext recordContext = internalProcessorContext.recordContext();
            final long size = computeRecordSize(key, value, recordContext);
            memBufferSize = memBufferSize + size;

            System.out.println(key + ":" + new ContextualRecord<>(time, value, recordContext, size));

            final TimeKey<K> timeKey = new TimeKey<>(time, key);
            index.put(key, timeKey);
            sortedMap.put(timeKey, new ContextualRecord<>(time, value, recordContext, size));

            // adding that key may have put us over the edge...
            enforceSizeBound();
        } else {
            internalProcessorContext.forward(key, value);
        }
    }

    private void enforceSizeBound() {
        if (index.size() > suppress.getIntermediateSuppression().getBufferConfig().getNumberOfKeysToRemember()
            || memBufferSize > suppress.getIntermediateSuppression().getBufferConfig().getBytesToUseForSuppressionStorage()) {

            switch (suppress.getIntermediateSuppression().getBufferConfig().getBufferFullStrategy()) {
                case EMIT:
                    // we only added one, so we only need to remove one.
                    final Map.Entry<TimeKey<K>, ContextualRecord<V>> firstEntry = sortedMap.firstEntry();
                    setNodeAndForward(firstEntry);
                    sortedMap.remove(firstEntry.getKey());
                    index.remove(firstEntry.getKey().key);
                    return;
                case SHUT_DOWN:
                    throw new RuntimeException("TODO: request graceful shutdown"); // TODO: request graceful shutdown
                case SPILL_TO_DISK:
                    throw new UnsupportedOperationException("Spill to Disk is not implemented"); // TODO: implement spillToDisk
            }
        }
    }

    private long computeRecordSize(final K key, final V value, final ProcessorRecordContext recordContext1) {
        long size = 0L;
        final Serializer<K> keySerializer = getKeySerializer(suppress);
        if (keySerializer != null) {
            size += keySerializer.serialize(null, key).length;
        }
        if (valueSerializer != null) {
            size += valueSerializer.serialize(null, valueAsChange(value)).length;
        }
        size += 8; // timestamp
        size += 8; // offset
        size += 4; // partition
        size += recordContext1.topic().toCharArray().length;
        for (final Header header : recordContext1.headers()) {
            size += header.key().toCharArray().length;
            size += header.value().length;
        }
        return size;
    }

    // TODO This reveals that the value type is a lie... This should be fixable.
    @SuppressWarnings("unchecked")
    private Change<V> valueAsChange(final V value) {
        return (Change<V>) value;
    }

    private boolean nonInstantaneousTimeBoundSuppression() {
        return suppress.getIntermediateSuppression().getTimeToWaitForMoreEvents().toMillis() > 0L;
    }

    private boolean nonTimeBoundSuppression() {
        return suppress.getIntermediateSuppression().getTimeToWaitForMoreEvents() == null;
    }

    private boolean intermediateSuppression() {
        return suppress.getIntermediateSuppression() != null;
    }

    private Serializer<Change<V>> getValueSerializer(final SuppressImpl<K, V> suppress) {
        return (suppress.getIntermediateSuppression() == null
            || suppress.getIntermediateSuppression().getBufferConfig() == null
            || suppress.getIntermediateSuppression().getBufferConfig().getValueSerializer() == null)
            ? null
            : new ChangedSerializer<>(suppress.getIntermediateSuppression().getBufferConfig().getValueSerializer());
    }

    private Serializer<K> getKeySerializer(final SuppressImpl<K, V> suppress) {
        return (suppress.getIntermediateSuppression() == null
            || suppress.getIntermediateSuppression().getBufferConfig() == null
            || suppress.getIntermediateSuppression().getBufferConfig().getKeySerializer() == null)
            ? null
            : suppress.getIntermediateSuppression().getBufferConfig().getKeySerializer();
    }

    @Override
    public void close() {
        // TODO: what to do here?
    }

    @Override
    public String toString() {
        return "KTableSuppressProcessor{" +
            "suppress=" + suppress +
            '}';
    }
}
