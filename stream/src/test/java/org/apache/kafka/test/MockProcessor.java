package org.apache.kafka.test;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.Processor;

import java.util.ArrayList;

public class MockProcessor<K, V> implements Processor<K, V> {
  public final ArrayList<String> processed = new ArrayList<>();
  public final ArrayList<Long> punctuated = new ArrayList<>();

  @Override
  public void process(K key, V value) {
    processed.add(key + ":" + value);
  }

  @Override
  public void init(KStreamContext context) {
  }

  @Override
  public void punctuate(long streamTime) {
    punctuated.add(streamTime);
  }

  @Override
  public void close() {
  }

}
