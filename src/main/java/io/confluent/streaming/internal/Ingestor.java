package io.confluent.streaming.internal;

import org.apache.kafka.common.TopicPartition;

/**
 * Created by yasuhiro on 6/30/15.
 */
public interface Ingestor {

  void poll();

  void poll(long timeoutMs);

  void pause(TopicPartition partition);

  void unpause(TopicPartition partition, long offset);

  int numPartitions(String topic);

  void addPartitionStreamToGroup(StreamGroup streamGroup, TopicPartition partition);

}
