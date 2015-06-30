package io.confluent.streaming.internal;

import io.confluent.streaming.RecordQueue;
import io.confluent.streaming.util.Stamped;
import io.confluent.streaming.util.TimestampTracker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Created by yasuhiro on 6/25/15.
 */
public class RecordQueueImpl<K, V> implements RecordQueue<K, V> {

  private final Deque<Stamped<ConsumerRecord<K, V>>> queue = new ArrayDeque<Stamped<ConsumerRecord<K, V>>>();
  private final TopicPartition partition;
  private TimestampTracker<ConsumerRecord<K, V>> timestampTracker;
  private long offset;

  public RecordQueueImpl(TopicPartition partition, TimestampTracker<ConsumerRecord<K, V>> timestampTracker) {
    this.partition = partition;
    this.timestampTracker = timestampTracker;
  }

  public TopicPartition partition() {
    return partition;
  }

  public void add(ConsumerRecord<K, V> record, long timestamp) {
    Stamped<ConsumerRecord<K, V>> elem = new Stamped<ConsumerRecord<K, V>>(record, timestamp);
    queue.addLast(elem);
    timestampTracker.addStampedElement(elem);
    offset = record.offset();
  }

  public ConsumerRecord<K, V> next() {
    Stamped<ConsumerRecord<K, V>> elem = queue.getFirst();

    if (elem == null) return null;

    timestampTracker.removeStampedElement(elem);

    return elem.value;
  }

  public long offset() {
    return offset;
  }

  public int size() {
    return queue.size();
  }

  public long currentStreamTime() {
    return timestampTracker.get();
  }

}
