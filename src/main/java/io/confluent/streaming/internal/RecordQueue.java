package io.confluent.streaming.internal;

import io.confluent.streaming.util.Stamped;
import io.confluent.streaming.util.TimestampTracker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayDeque;

/**
 * Created by yasuhiro on 6/25/15.
 */
public class RecordQueue {

  private final ArrayDeque<StampedRecord> queue = new ArrayDeque<>();
  public final Receiver receiver;
  private final TopicPartition partition;
  private TimestampTracker<ConsumerRecord<Object, Object>> timestampTracker;
  private long offset;

  public RecordQueue(TopicPartition partition, Receiver receiver, TimestampTracker<ConsumerRecord<Object, Object>> timestampTracker) {
    this.partition = partition;
    this.receiver = receiver;
    this.timestampTracker = timestampTracker;
  }

  public TopicPartition partition() {
    return partition;
  }

  public void add(StampedRecord record) {
    queue.addLast(record);
    timestampTracker.addStampedElement(record);
    offset = record.offset();
  }

  public StampedRecord next() {
    StampedRecord elem = queue.pollFirst();

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
