package io.confluent.streaming;

import io.confluent.streaming.util.FilteredIterator;
import io.confluent.streaming.util.Stamped;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by yasuhiro on 6/18/15.
 */
public class WindowByTime<K, V> implements Window<K, V> {

  private final long duration;
  private final int maxCount;
  private LinkedList<K> list = new LinkedList<K>();
  private HashMap<K, LinkedList<Stamped<V>>> map = new HashMap<K, LinkedList<Stamped<V>>>();

  public WindowByTime(long duration, int maxCount) {
    this.duration = duration;
    this.maxCount = maxCount;
  }

  @Override
  public Iterator<V> find(K key, final long timestamp) {
    final LinkedList<Stamped<V>> values = map.get(key);

    if (values == null) {
      return null;
    }
    else {
      return new FilteredIterator<V, Stamped<V>>(values.iterator()) {
        @Override
        protected V filter(Stamped<V> item) {
          if (item.timestamp <= timestamp)
            return item.value;
          else
            return null;
        }
      };
    }
  }

  @Override
  public void put(K key, V value, long timestamp) {
    list.offerLast(key);

    LinkedList<Stamped<V>> values = map.get(key);
    if (values == null) {
      values = new LinkedList<Stamped<V>>();
      map.put(key, values);
    }

    values.offerLast(new Stamped<V>(value, timestamp));

    evictExcess();
    evictExpired(timestamp - duration);
  }

  private void evictExcess() {
    while (list.size() > maxCount) {
      K oldestKey = list.pollFirst();

      LinkedList<Stamped<V>> values = map.get(oldestKey);
      values.removeFirst();

      if (values.isEmpty()) map.remove(oldestKey);
    }
  }

  private void evictExpired(long cutoffTime) {
    while (true) {
      K oldestKey = list.peekFirst();

      LinkedList<Stamped<V>> values = map.get(oldestKey);
      Stamped<V> oldestValue = values.peekFirst();

      if (oldestValue.timestamp < cutoffTime) {
        list.pollFirst();
        values.removeFirst();

        if (values.isEmpty()) map.remove(oldestKey);
      }
      else {
        break;
      }
    }
  }

}
