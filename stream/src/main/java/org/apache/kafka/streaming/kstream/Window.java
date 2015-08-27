package org.apache.kafka.streaming.kstream;

import org.apache.kafka.streaming.processor.ProcessorContext;
import org.apache.kafka.streaming.processor.StateStore;

import java.util.Iterator;

public interface Window<K, V> extends StateStore {

    void init(ProcessorContext context);

    Iterator<V> find(K key, long timestamp);

    Iterator<V> findAfter(K key, long timestamp);

    Iterator<V> findBefore(K key, long timestamp);

    void put(K key, V value, long timestamp);
}
