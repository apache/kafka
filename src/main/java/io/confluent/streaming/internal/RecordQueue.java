package io.confluent.streaming.internal;

import io.confluent.streaming.util.Stamped;
import io.confluent.streaming.util.TimestampTracker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Created by yasuhiro on 6/25/15.
 */
public class RecordQueue<K, V> {

  private final Deque<StampedRecord<K, V>> queue = new ArrayDeque<StampedRecord<K, V>>();
  public final Receiver<K, V> receiver;
  private final TopicPartition partition;
  private TimestampTracker<ConsumerRecord<K, V>> timestampTracker;
  private long offset;

  public RecordQueue(TopicPartition partition, Receiver<K, V> receiver, TimestampTracker<ConsumerRecord<K, V>> timestampTracker) {
    this.partition = partition;
    this.receiver = receiver;
    this.timestampTracker = timestampTracker;
  }

  public TopicPartition partition() {
    return partition;
  }

  public void add(StampedRecord<K, V> record) {
    queue.addLast(record);
    timestampTracker.addStampedElement(record);
    offset = record.offset();
  }

  public StampedRecord<K, V> next() {
    StampedRecord<K, V> elem = queue.getFirst();

    if (elem == null) return null;

    timestampTracker.removeStampedElement(elem);

    return elem;
  }

  public long offset() {
    return offset;
  }

  public int size() {
    return queue.size();
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  /**
   * Returns a timestamp tracked by the TimestampTracker
   * @return timestamp
   */
  public long trackedTimestamp() {
    return timestampTracker.get();
  }

}
