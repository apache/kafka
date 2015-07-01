package io.confluent.streaming.internal;

import io.confluent.streaming.Chooser;
import io.confluent.streaming.RecordQueue;

import java.util.ArrayDeque;

/**
 * Created by yasuhiro on 6/25/15.
 */
public class RoundRobinChooser<K, V> implements Chooser<K, V> {

  private final ArrayDeque<RecordQueue<K, V>> deque;

  public RoundRobinChooser() {
    deque = new ArrayDeque<RecordQueue<K, V>>();
  }

  @Override
  public void add(RecordQueue<K, V> queue) {
    deque.offer(queue);
  }

  @Override
  public RecordQueue<K, V> next() {
    return deque.poll();
  }

  @Override
  public void close() {
    deque.clear();
  }

}
