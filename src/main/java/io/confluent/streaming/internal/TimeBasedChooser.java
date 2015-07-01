package io.confluent.streaming.internal;

import io.confluent.streaming.Chooser;
import io.confluent.streaming.RecordQueue;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Created by yasuhiro on 6/25/15.
 */
public class TimeBasedChooser<K, V> implements Chooser<K, V> {

  private final PriorityQueue<RecordQueue<K, V>> pq;

  public TimeBasedChooser() {
    this(new Comparator<RecordQueue<K, V>>() {
      public int compare(RecordQueue<K, V> queue1, RecordQueue<K, V> queue2) {
        long time1 = queue1.currentStreamTime();
        long time2 = queue2.currentStreamTime();

        if (time1 < time2) return -1;
        if (time1 > time2) return 1;
        return 0;
      }
    });
  }

  private TimeBasedChooser(Comparator<RecordQueue<K, V>> comparator) {
    pq = new PriorityQueue<RecordQueue<K, V>>(3, comparator);
  }

  @Override
  public void add(RecordQueue<K, V> queue) {
    pq.offer(queue);
  }

  @Override
  public RecordQueue<K, V> next() {
    return pq.poll();
  }

  @Override
  public void close() {
    pq.clear();
  }

}
