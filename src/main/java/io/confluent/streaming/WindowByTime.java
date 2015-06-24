package io.confluent.streaming;

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

  public Iterator<V> find(K key, long timestamp) {
    final LinkedList<Stamped<V>> values = map.get(key);

    if (values == null) {
      return null;
    }
    else {
      final Iterator<Stamped<V>> inner = values.iterator();

      return new Iterator<V>() {
        public boolean hasNext() {
          return inner.hasNext();
        }
        public V next() {
          return inner.next().value;
        }
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  public void put(K key, V value, long timestamp) {
    list.offerLast(key);

    LinkedList<Stamped<V>> values = map.get(key);
    if (values == null) {
      values = new LinkedList<Stamped<V>>();
      map.put(key, values);
    }

    values.offerLast(new Stamped(value, timestamp));

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

  public void punctuate(long timestamp) {}

  public void flush() {}

}
