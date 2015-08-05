package org.apache.kafka.stream.internal;

import java.util.PriorityQueue;

/**
 * Created by yasuhiro on 6/29/15.
 */
public class PunctuationQueue {

  private PriorityQueue<PunctuationSchedule> pq = new PriorityQueue<>();

  public void schedule(PunctuationSchedule sched) {
    synchronized (pq) {
      pq.add(sched);
    }
  }

  public void close() {
    synchronized (pq) {
      pq.clear();
    }
  }

  public void mayPunctuate(long streamTime) {
    synchronized (pq) {
      PunctuationSchedule top = pq.peek();
      while (top != null && top.timestamp <= streamTime) {
        PunctuationSchedule sched = top;
        pq.poll();
        sched.processor().punctuate(streamTime);
        pq.add(sched.next());

        top = pq.peek();
      }
    }
  }

}
