package org.apache.kafka.stream.topology;

import org.apache.kafka.clients.processor.ProcessorContext;
import org.apache.kafka.clients.processor.StateStore;

import java.util.Iterator;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Window<K, V> extends StateStore {

  void init(ProcessorContext context);

  Iterator<V> find(K key, long timestamp);

  Iterator<V> findAfter(K key, long timestamp);

  Iterator<V> findBefore(K key, long timestamp);

  void put(K key, V value, long timestamp);

}
