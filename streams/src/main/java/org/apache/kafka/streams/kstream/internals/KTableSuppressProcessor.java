package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.Suppression;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorNode;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class KTableSuppressProcessor<K, V> implements Processor<K, V> {
    private final Suppression<K, V> suppression;
    private final LinkedHashMap<K, ContextualRecord<V>> priorityQueue;
    private InternalProcessorContext internalProcessorContext;
    private long memBufferSize;
    private ProcessorNode myNode;
    private final Serializer<Change<V>> valueSerializer;

    private static class ContextualRecord<V> {
        private final V value;
        private final ProcessorRecordContext recordContext;
        private final long size;

        private ContextualRecord(final V value, final ProcessorRecordContext recordContext, final long size) {
            this.value = value;
            this.recordContext = recordContext;
            this.size = size;
        }

        @Override
        public String toString() {
            return "ContextualRecord{" +
                "value=" + value +
                ", timestamp=" + recordContext.timestamp() +
                ", size=" + Objects.toString(size) +
                '}';
        }
    }

    KTableSuppressProcessor(final Suppression<K, V> suppression) {
        this.suppression = suppression;
        priorityQueue = new LinkedHashMap<>();
        valueSerializer =
            suppression.getIntermediateSuppression().getValueSerializer() == null
                ? null
                : new ChangedSerializer<V>(suppression.getIntermediateSuppression().getValueSerializer());
    }

    @Override
    public void init(final ProcessorContext context) {
        internalProcessorContext = (InternalProcessorContext) context;
        myNode = internalProcessorContext.currentNode();


        if (intermediateSuppression()) {
            final Duration timeToWaitForMoreEvents = suppression.getIntermediateSuppression().getTimeToWaitForMoreEvents();
            if (timeToWaitForMoreEvents != null && timeToWaitForMoreEvents.toMillis() > 0) {
                final long evictionTimeout = timeToWaitForMoreEvents.toMillis();

                internalProcessorContext.schedule(
                    evictionTimeout,
                    PunctuationType.STREAM_TIME,
                    streamTime -> {
                        final Set<Map.Entry<K, ContextualRecord<V>>> entries = priorityQueue.entrySet();
                        final Iterator<Map.Entry<K, ContextualRecord<V>>> iterator = entries.iterator();
                        while (iterator.hasNext()) {
                            final Map.Entry<K, ContextualRecord<V>> next = iterator.next();
                            if (next.getValue().recordContext.timestamp() <= streamTime - evictionTimeout) {
                                setNodeAndForward(next);
                                iterator.remove();
                            } else {
                                break;
                            }
                        }
                    });
            }
        }
    }

    private void setNodeAndForward(final Map.Entry<K, ContextualRecord<V>> next) {
        final ProcessorNode prevNode = internalProcessorContext.currentNode();
        final ProcessorRecordContext prevRecordContext = internalProcessorContext.recordContext();
        internalProcessorContext.setRecordContext(next.getValue().recordContext);
        internalProcessorContext.setCurrentNode(myNode);
        try {
            internalProcessorContext.forward(next.getKey(), next.getValue().value);
        } finally {
            internalProcessorContext.setCurrentNode(prevNode);
            internalProcessorContext.setRecordContext(prevRecordContext);
        }
    }

    @Override
    public void process(final K key, final V value) {
        if (intermediateSuppression() && (nonTimeBoundSuppression() || nonInstantaneousTimeBoundSuppression())) {
            // intermediate suppression is enabled
            final ContextualRecord<V> previous = priorityQueue.remove(key);
            if (previous != null) { memBufferSize = memBufferSize - previous.size; }

            final ProcessorRecordContext recordContext = internalProcessorContext.recordContext();
            final long size = computeRecordSize(key, value, recordContext);
            memBufferSize = memBufferSize + size;

            priorityQueue.put(key, new ContextualRecord<>(value, recordContext, size));

            // adding that key may have put us over the edge...
            enforceSizeBound();
        } else {
            internalProcessorContext.forward(key, value);
        }
    }

    private void enforceSizeBound() {
        if (priorityQueue.size() > suppression.getIntermediateSuppression().getNumberOfKeysToRemember()
            || memBufferSize > suppression.getIntermediateSuppression().getBytesToUseForSuppressionStorage()) {

            switch (suppression.getIntermediateSuppression().getBufferFullStrategy()) {
                case EMIT:
                    // we only added one, so we only need to remove one.
                    final Iterator<Map.Entry<K, ContextualRecord<V>>> iterator = priorityQueue.entrySet().iterator();
                    final Map.Entry<K, ContextualRecord<V>> next = iterator.next();
                    setNodeAndForward(next);
                    iterator.remove();
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
        final Serializer<K> keySerializer = suppression.getIntermediateSuppression().getKeySerializer();
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
        return suppression.getIntermediateSuppression().getTimeToWaitForMoreEvents().toMillis() > 0L;
    }

    private boolean nonTimeBoundSuppression() {
        return suppression.getIntermediateSuppression().getTimeToWaitForMoreEvents() == null;
    }

    private boolean intermediateSuppression() {
        return suppression.getIntermediateSuppression() != null;
    }


    @Override
    public void close() {
        // TODO: what to do here?
    }

    @Override
    public String toString() {
        return "KTableSuppressProcessor{" +
            "suppression=" + suppression +
            '}';
    }
}
