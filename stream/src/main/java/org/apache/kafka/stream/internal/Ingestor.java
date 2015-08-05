package org.apache.kafka.stream.internal;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

/**
 * Created by yasuhiro on 6/30/15.
 */
public interface Ingestor {

  Set<String> topics();

  void poll(long timeoutMs);

  void pause(TopicPartition partition);

  void unpause(TopicPartition partition, long offset);

  void commit(Map<TopicPartition, Long> offsets);

  int numPartitions(String topic);

  void addPartitionStreamToGroup(StreamGroup streamGroup, TopicPartition partition);

}
