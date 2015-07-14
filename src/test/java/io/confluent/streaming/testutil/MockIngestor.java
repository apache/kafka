package io.confluent.streaming.testutil;

import io.confluent.streaming.internal.Ingestor;
import io.confluent.streaming.internal.StreamSynchronizer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.HashSet;

public class MockIngestor implements Ingestor {

  private HashMap<TopicPartition, StreamSynchronizer> streamSynchronizers = new HashMap<>();

  public HashSet<TopicPartition> paused = new HashSet<>();

  @Override
  public void poll() {
  }

  @Override
  public void poll(long timeoutMs) {
  }

  @Override
  public void pause(TopicPartition partition) {
    paused.add(partition);
  }

  @Override
  public void unpause(TopicPartition partition, long offset) {
    paused.remove(partition);
  }

  @Override
  public int numPartitions(String topic) {
    return 1;
  }

  @Override
  public void addStreamSynchronizerForPartition(StreamSynchronizer streamSynchronizer, TopicPartition partition) {
    streamSynchronizers.put(partition, streamSynchronizer);
  }

  public void addRecords(TopicPartition partition, Iterable<ConsumerRecord<byte[], byte[]>> records) {
    streamSynchronizers.get(partition).addRecords(partition, records.iterator());
  }

}
