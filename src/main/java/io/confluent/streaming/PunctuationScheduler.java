package io.confluent.streaming;

/**
 * An interface that allows {@link Processor} to schedule a notification at a specified stream time.
 */
public interface PunctuationScheduler {

  void schedule(long timestamp);

  void cancel();

}
