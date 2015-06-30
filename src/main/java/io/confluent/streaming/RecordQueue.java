package io.confluent.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Created by yasuhiro on 6/25/15.
 */
public interface RecordQueue<K, V>  {

  TopicPartition partition();

  void add(ConsumerRecord<K, V> value, long timestamp);

  ConsumerRecord<K, V> next();

  long offset();

  int size();

  long currentStreamTime();
}
