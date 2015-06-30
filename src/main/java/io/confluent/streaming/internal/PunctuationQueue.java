package io.confluent.streaming.internal;

import io.confluent.streaming.util.Stamped;

import java.util.PriorityQueue;

/**
 * Created by yasuhiro on 6/29/15.
 */
public class PunctuationQueue {

  private PriorityQueue<Stamped<PunctuationSchedulerImpl>> pq = new PriorityQueue<Stamped<PunctuationSchedulerImpl>>();

  public Stamped<PunctuationSchedulerImpl> schedule(PunctuationSchedulerImpl scheduler, long time) {
    synchronized (pq) {
      Stamped<PunctuationSchedulerImpl> stamped = new Stamped<PunctuationSchedulerImpl>(scheduler, time);
      pq.add(stamped);
      return stamped;
    }
  }

  public void cancel(Stamped<PunctuationSchedulerImpl> stamped) {
    synchronized (pq) {
      pq.remove(stamped);
    }

  }

  public void close() {
    synchronized (pq) {
      pq.clear();
    }
  }

  public void mayPunctuate(long streamTime) {
    synchronized (pq) {
      Stamped<PunctuationSchedulerImpl> top = pq.peek();
      while (top.timestamp <= streamTime) {
        PunctuationSchedulerImpl scheduler = top.value;
        pq.poll();
        scheduler.processor.punctuate(streamTime);
        scheduler.processed();

        top = pq.peek();
      }
    }
  }

}
