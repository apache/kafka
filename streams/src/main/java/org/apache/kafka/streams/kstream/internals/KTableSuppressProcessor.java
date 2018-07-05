package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.Suppression;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class KTableSuppressProcessor<K, V> implements Processor<K, V> {
    private final Suppression<K, V> suppression;
    private final LinkedHashMap<K, ContextualRecord<V>> priorityQueue;
    private InternalProcessorContext internalProcessorContext;
    private int memBufferSize;

    private static class ContextualRecord<V> {
        private final V value;
        private final ProcessorRecordContext recordContext;
        private final int size;

        private ContextualRecord(final V value, final ProcessorRecordContext recordContext, final Integer size) {
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
    }

    @Override
    public void init(final ProcessorContext context) {
        internalProcessorContext = (InternalProcessorContext) context;


        if (intermediateSuppression() && suppression.getIntermediateSuppression().getTimeToWaitForMoreEvents().toMillis() > 0) {
            final long evictionTimeout = suppression.getIntermediateSuppression().getTimeToWaitForMoreEvents().toMillis();

            internalProcessorContext.schedule(
                evictionTimeout,
                PunctuationType.STREAM_TIME,
                streamTime -> {
                    final Set<Map.Entry<K, ContextualRecord<V>>> entries = priorityQueue.entrySet();
                    final Iterator<Map.Entry<K, ContextualRecord<V>>> iterator = entries.iterator();
                    while (iterator.hasNext()) {
                        final Map.Entry<K, ContextualRecord<V>> next = iterator.next();
                        final ProcessorRecordContext recordContext = next.getValue().recordContext;
                        if (recordContext.timestamp() <= streamTime - evictionTimeout) {
                            internalProcessorContext.setRecordContext(recordContext);
                            // internalProcessorContext.setCurrentNode(); TODO: need to do this?
                            internalProcessorContext.forward(next.getKey(), next.getValue().value);
                            iterator.remove();
                        } else {
                            break;
                        }
                    }
                });
        }
    }

    @Override
    public void process(final K key, final V value) {
        if (intermediateSuppression() && (nonTimeBoundSuppression() || nonInstantaneousTimeBoundSuppression())) {
            // intermediate suppression is enabled
            final ContextualRecord<V> previous = priorityQueue.remove(key);
            if (previous != null) { memBufferSize = memBufferSize - previous.size; }

            final ProcessorRecordContext recordContext = internalProcessorContext.recordContext();
            final int size = computeRecordSize(key, value, recordContext);
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
                    internalProcessorContext.setRecordContext(next.getValue().recordContext);
                    // internalProcessorContext.setCurrentNode(); TODO: need to do this?
                    internalProcessorContext.forward(next.getKey(), next.getValue().value);
                    iterator.remove();
                    return;
                case SHUT_DOWN:
                    throw new RuntimeException("TODO: request graceful shutdown"); // TODO: request graceful shutdown
                case SPILL_TO_DISK:
                    throw new UnsupportedOperationException("Spill to Disk is not implemented"); // TODO: implement spillToDisk
            }
        }
    }

    private int computeRecordSize(final K key, final V value, final ProcessorRecordContext recordContext1) {
        int size = 0;
        final Serializer<K> keySerializer = suppression.getIntermediateSuppression().getKeySerializer();
        if (keySerializer != null) {
            size += keySerializer.serialize(null, key).length;
        }
        final Serializer<V> valueSerializer = suppression.getIntermediateSuppression().getValueSerializer();
        if (valueSerializer != null) {
            size += valueSerializer.serialize(null, value).length;
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
