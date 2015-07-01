package io.confluent.streaming.internal;

import io.confluent.streaming.Processor;
import io.confluent.streaming.PunctuationScheduler;
import io.confluent.streaming.util.Stamped;

public class PunctuationSchedulerImpl implements PunctuationScheduler {

  private Stamped<PunctuationSchedulerImpl> scheduled;
  private final PunctuationQueue queue;
  final Processor<?, ?> processor;

  public PunctuationSchedulerImpl(PunctuationQueue queue, Processor<?, ?> processor) {
    this.queue = queue;
    this.processor = processor;
  }

  @Override
  public void schedule(long timestamp) {
    synchronized (this) {
      if (scheduled != null)
        throw new IllegalStateException("punctuation is already scheduled");

      scheduled = queue.schedule(this, timestamp);
    }
  }

  @Override
  public void cancel() {
    synchronized (this) {
      if (scheduled != null) {
        queue.cancel(scheduled);
        scheduled = null;
      }
    }
  }

  public void processed() {
    synchronized (this) {
      scheduled = null;
    }
  }

}
