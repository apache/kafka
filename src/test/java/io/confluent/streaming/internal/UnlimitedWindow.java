package io.confluent.streaming.internal;

import io.confluent.streaming.KeyValue;
import io.confluent.streaming.Window;
import io.confluent.streaming.util.FilteredIterator;

import java.util.Iterator;
import java.util.LinkedList;

class UnlimitedWindow<K, V> implements Window<K, V> {

  private LinkedList<KeyValue<K, V>> list = new LinkedList<KeyValue<K, V>>();

  @Override
  public Iterator<V> find(final K key, long timestamp) {
    return new FilteredIterator<V, KeyValue<K, V>>(list.iterator()) {
      protected V filter(KeyValue<K, V> item) {
        if (item.key.equals(key))
          return item.value;
        else
          return null;
      }
    };
  }

  @Override
  public void put(K key, V value, long timestamp) {
    list.add(KeyValue.pair(key, value));
  }

}
