package io.confluent.streaming;

import io.confluent.streaming.internal.PunctuationQueue;

/**
 * Created by yasuhiro on 6/29/15.
 */
public interface PunctuationScheduler {

  void schedule(long time);

  void cancel();

}
