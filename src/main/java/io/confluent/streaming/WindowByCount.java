package io.confluent.streaming;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by yasuhiro on 6/18/15.
 */
public class WindowByCount<K, V> implements Window<K, V> {

  private final int maxCount;
  private LinkedList<K> list = new LinkedList<K>();
  private HashMap<K, LinkedList<V>> map = new HashMap<K, LinkedList<V>>();
  private int count = 0;

  public WindowByCount(int maxCount) {
    this.maxCount = maxCount;
  }

  public Iterator<V> find(K key, long timestamp) {
    LinkedList<V> values = map.get(key);
    if (values == null)
      return null;
    else
      return values.iterator();
  }

  public void put(K key, V value, long timestamp) {
    list.offerLast(key);

    LinkedList<V> values = map.get(key);
    if (values == null) {
      values = new LinkedList<V>();
      map.put(key, values);
    }
    values.offerLast(value);
    count++;

    mayEvict();
  }

  private void mayEvict() {
    while (count > maxCount) {
      K oldestKey = list.poll();
      LinkedList<V> values = map.get(oldestKey);
      values.removeFirst();
      if (values.isEmpty()) map.remove(oldestKey);
      count--;
    }
  }

  public void punctuate(long timestamp) {}

  public void flush() {}

}
