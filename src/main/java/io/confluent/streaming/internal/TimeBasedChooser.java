package io.confluent.streaming.internal;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Created by yasuhiro on 6/25/15.
 */
public class TimeBasedChooser implements Chooser {

  private final PriorityQueue<RecordQueue> pq;

  public TimeBasedChooser() {
    this(new Comparator<RecordQueue>() {
      public int compare(RecordQueue queue1, RecordQueue queue2) {
        long time1 = queue1.trackedTimestamp();
        long time2 = queue2.trackedTimestamp();

        if (time1 < time2) return -1;
        if (time1 > time2) return 1;
        return 0;
      }
    });
  }

  private TimeBasedChooser(Comparator<RecordQueue> comparator) {
    pq = new PriorityQueue<>(3, comparator);
  }

  @Override
  public void add(RecordQueue queue) {
    pq.offer(queue);
  }

  @Override
  public RecordQueue next() {
    return pq.poll();
  }

  @Override
  public void close() {
    pq.clear();
  }

}
