package io.confluent.streaming.internal;

import io.confluent.streaming.Processor;
import io.confluent.streaming.PunctuationScheduler;

import java.util.ArrayList;

/**
 * Created by yasuhiro on 6/30/15.
 */
class TestProcessor<K, V> implements Processor<K, V> {
  public final ArrayList<String> processed = new ArrayList<String>();

  @Override
  public void apply(K key, V value) {
    processed.add(key + ":" + value);
  }

  @Override
  public void init(PunctuationScheduler punctuationScheduler) {
  }

  @Override
  public void punctuate(long streamTime) {
  }
}
