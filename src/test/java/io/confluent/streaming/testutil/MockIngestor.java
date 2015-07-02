package io.confluent.streaming.testutil;

import io.confluent.streaming.internal.Ingestor;
import io.confluent.streaming.internal.StreamSynchronizer;
import org.apache.kafka.common.TopicPartition;

public class MockIngestor implements Ingestor {
  @Override
  public void poll() {
  }

  @Override
  public void poll(long timeoutMs) {
  }

  @Override
  public void pause(TopicPartition partition) {
  }

  @Override
  public void unpause(TopicPartition partition, long offset) {
  }

  @Override
  public int numPartitions(String topic) {
    return 1;
  }

  @Override
  public void addStreamSynchronizerForPartition(StreamSynchronizer<?, ?> streamSynchronizer, TopicPartition partition) {
  }

  @Override
  public void removeStreamSynchronizerForPartition(TopicPartition partition) {
  }

}
