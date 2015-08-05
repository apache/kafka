package org.apache.kafka.stream.internal;

import org.apache.kafka.stream.Chooser;

import java.util.ArrayDeque;

/**
 * Created by yasuhiro on 6/25/15.
 */
public class RoundRobinChooser implements Chooser {

  private final ArrayDeque<RecordQueue> deque;

  public RoundRobinChooser() {
    deque = new ArrayDeque<>();
  }

  @Override
  public void add(RecordQueue queue) {
    deque.offer(queue);
  }

  @Override
  public RecordQueue next() {
    return deque.poll();
  }

  @Override
  public void close() {
    deque.clear();
  }

}
