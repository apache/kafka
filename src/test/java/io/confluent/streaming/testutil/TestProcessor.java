package io.confluent.streaming.testutil;

import io.confluent.streaming.Processor;
import io.confluent.streaming.PunctuationScheduler;

import java.util.ArrayList;

public class TestProcessor<K, V> implements Processor<K, V> {
  public final ArrayList<String> processed = new ArrayList<String>();
  public final ArrayList<Long> punctuated = new ArrayList<Long>();

  @Override
  public void apply(String topic, K key, V value) {
    processed.add(key + ":" + value);
  }

  @Override
  public void init(PunctuationScheduler punctuationScheduler) {
  }

  @Override
  public void punctuate(long streamTime) {
    punctuated.add(streamTime);
  }
}
