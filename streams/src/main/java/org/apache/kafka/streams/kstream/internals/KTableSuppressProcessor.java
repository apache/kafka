package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.Suppression;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class KTableSuppressProcessor<K, V> implements Processor<K, V> {
    private final Suppression<K, V> suppression;
    private final LinkedHashMap<K, ContextualRecord<K, V>> priorityQueue;
    private InternalProcessorContext context;

    private static class ContextualRecord<K, V> {
        private final V value;
        private final ProcessorRecordContext recordContext;

        private ContextualRecord(final V value, final ProcessorRecordContext recordContext) {
            this.value = value;
            this.recordContext = recordContext;
        }

        @Override
        public String toString() {
            return "ContextualRecord{" +
                "value=" + value +
                ", timestamp=" + recordContext.timestamp() +
                '}';
        }
    }

    KTableSuppressProcessor(final Suppression<K, V> suppression) {
        this.suppression = suppression;
        priorityQueue = new LinkedHashMap<>();
    }

    @Override
    public void init(final ProcessorContext context) {
        this.context = (InternalProcessorContext) context;


        if (suppression.getIntermediateSuppression() != null) {
            final long evictionTimeout = suppression.getIntermediateSuppression().getTimeToWaitForMoreEvents().toMillis();

            context.schedule(evictionTimeout, PunctuationType.STREAM_TIME, new Punctuator() {
                @Override
                public void punctuate(final long streamTime) {

                    final Set<Map.Entry<K, ContextualRecord<K, V>>> entries = priorityQueue.entrySet();
                    final Iterator<Map.Entry<K, ContextualRecord<K, V>>> iterator = entries.iterator();
                    while (iterator.hasNext()) {
                        final Map.Entry<K, ContextualRecord<K, V>> next = iterator.next();
                        final ProcessorRecordContext recordContext = next.getValue().recordContext;
                        if (recordContext.timestamp() <= streamTime - evictionTimeout) {
                            ((InternalProcessorContext) context).setRecordContext(recordContext);
                            context.forward(next.getKey(), next.getValue().value);
                            iterator.remove();
                        } else {
                            break;
                        }

                    }
                }
            });
        }
    }

    @Override
    public void process(final K key, final V value) {
        priorityQueue.remove(key);
        priorityQueue.put(key, new ContextualRecord<>(value, context.recordContext()));
    }

    @Override
    public void close() {

    }

    @Override
    public String toString() {
        return "KTableSuppressProcessor{" +
            "suppression=" + suppression +
            '}';
    }
}
