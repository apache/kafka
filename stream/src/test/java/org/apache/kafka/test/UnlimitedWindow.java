package org.apache.kafka.test;


import org.apache.kafka.stream.KStreamContext;
import org.apache.kafka.stream.topology.KeyValue;
import org.apache.kafka.stream.topology.Window;
import org.apache.kafka.stream.util.FilteredIterator;
import org.apache.kafka.stream.util.Stamped;

import java.util.Iterator;
import java.util.LinkedList;

public class UnlimitedWindow<K, V> implements Window<K, V> {

  private LinkedList<Stamped<KeyValue<K, V>>> list = new LinkedList<>();

  @Override
  public void init(KStreamContext context) {

  }
  @Override
  public Iterator<V> find(final K key, long timestamp) {
    return find(key, Long.MIN_VALUE, timestamp);
  }

  @Override
  public Iterator<V> findAfter(final K key, long timestamp) {
    return find(key, timestamp, Long.MAX_VALUE);
  }

  @Override
  public Iterator<V> findBefore(final K key, long timestamp) {
    return find(key, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  private Iterator<V> find(final K key, final long startTime, final long endTime) {
    return new FilteredIterator<V, Stamped<KeyValue<K, V>>>(list.iterator()) {
      protected V filter(Stamped<KeyValue<K, V>> item) {
        if (item.value.key.equals(key) && startTime <= item.timestamp && item.timestamp <= endTime)
          return item.value.value;
        else
          return null;
      }
    };
  }
  @Override
  public void put(K key, V value, long timestamp) {
    list.add(new Stamped<KeyValue<K, V>>(KeyValue.pair(key, value), timestamp));
  }

  @Override
  public String name() {
    return null;
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
  }

  @Override
  public boolean persistent() {
    return false;
  }
}
