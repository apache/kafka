package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.Suppression;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class KTableSuppressProcessor<K, V> implements Processor<K, V> {
    private final Suppression<K, V> suppression;
    private ProcessorContext context;

    KTableSuppressProcessor(final Suppression<K, V> suppression) {
        this.suppression = suppression;
    }

    @Override
    public void init(final ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(final K key, final V value) {
        context.forward(key, value);
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
