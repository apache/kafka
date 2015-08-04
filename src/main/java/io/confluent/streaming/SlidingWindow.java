package io.confluent.streaming;

import io.confluent.streaming.util.FilteredIterator;
import io.confluent.streaming.util.Stamped;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by yasuhiro on 6/18/15.
 */
public class SlidingWindow<K, V> implements Window<K, V> {

  private String name;
  private final long duration;
  private final int maxCount;
  private LinkedList<K> list = new LinkedList<K>();
  private HashMap<K, LinkedList<Stamped<V>>> map = new HashMap<K, LinkedList<Stamped<V>>>();

  public SlidingWindow(String name, long duration, int maxCount) {
    this.name = name;
    this.duration = duration;
    this.maxCount = maxCount;
  }

  @Override
  public void init(KStreamContext context) {
  }

  @Override
  public Iterator<V> findAfter(K key, final long timestamp) {
    return find(key, timestamp, timestamp + duration);
  }

  @Override
  public Iterator<V> findBefore(K key, final long timestamp) {
    return find(key, timestamp - duration, timestamp);
  }

  @Override
  public Iterator<V> find(K key, final long timestamp) {
    return find(key, timestamp - duration, timestamp + duration);
  }

  /*
   * finds items in the window between startTime and endTime (both inclusive)
   */
  private Iterator<V> find(K key, final long startTime, final long endTime) {
    final LinkedList<Stamped<V>> values = map.get(key);

    if (values == null) {
      return null;
    }
    else {
      return new FilteredIterator<V, Stamped<V>>(values.iterator()) {
        @Override
        protected V filter(Stamped<V> item) {
          if (startTime <= item.timestamp && item.timestamp <= endTime)
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

  @Override
  public String name() {
    return name;
  }

  @Override
  public void flush() {
    // TODO
  }

  @Override
  public void close() {
    // TODO
  }

  @Override
  public boolean persistent() {
    // TODO: should not be persistent, right?
    return false;
  }
}
