package io.confluent.streaming.internal;

import io.confluent.streaming.RecordQueue;
import io.confluent.streaming.util.QueueWithMinTimestampTracking;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Created by yasuhiro on 6/25/15.
 */
public class RecordQueueImpl<K, V> extends QueueWithMinTimestampTracking<ConsumerRecord<K, V>> implements RecordQueue<K, V> {

  private final TopicPartition partition;
  private long offset;

  public RecordQueueImpl(TopicPartition partition) {
    this.partition = partition;
  }

  public TopicPartition partition() {
    return partition;
  }

  public void add(ConsumerRecord<K, V> record, long timestamp) {
    offset = record.offset();
  }

  public long offset() {
    return offset;
  }

  public long currentStreamTime() {
    return super.timestamp();
  }

}
