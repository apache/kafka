package io.confluent.streaming.testutil;

import io.confluent.streaming.internal.Ingestor;
import org.apache.kafka.common.TopicPartition;

public class NoopIngestor implements Ingestor {
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
}
