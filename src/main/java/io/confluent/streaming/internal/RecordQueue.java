package io.confluent.streaming.internal;

import io.confluent.streaming.util.Stamped;
import io.confluent.streaming.util.TimestampTracker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayDeque;

/**
 * RecordQueue is a queue of {@link StampedRecord} (ConsumerRecord + timestamp). It is intended to be used in
 * {@link StreamSynchronizer}.
 */
public class RecordQueue {

  private final ArrayDeque<StampedRecord> queue = new ArrayDeque<>();
  public final KStreamSource source;
  private final TopicPartition partition;
  private TimestampTracker<ConsumerRecord<Object, Object>> timestampTracker;
  private long offset;

<<<<<<< HEAD
  public RecordQueue(TopicPartition partition, KStreamSource source, TimestampTracker<ConsumerRecord<Object, Object>> timestampTracker) {
=======
  /**
   * Creates a new instance of RecordQueue
   * @param partition partition
   * @param receiver the receiver of the stream of this partition
   * @param timestampTracker TimestampTracker
   */
  public RecordQueue(TopicPartition partition, Receiver receiver, TimestampTracker<ConsumerRecord<Object, Object>> timestampTracker) {
>>>>>>> javadoc
    this.partition = partition;
    this.source = source;
    this.timestampTracker = timestampTracker;
  }

  /**
   * Returns the partition with which this queue is associated
   * @return TopicPartition
   */
  public TopicPartition partition() {
    return partition;
  }

  /**
   * Adds a StampedRecord to the queue
   * @param record StampedRecord
   */
  public void add(StampedRecord record) {
    queue.addLast(record);
    timestampTracker.addStampedElement(record);
    offset = record.offset();
  }

  /**
   * Returns the next record fro the queue
   * @return StampedRecord
   */
  public StampedRecord next() {
    StampedRecord elem = queue.pollFirst();

    if (elem == null) return null;

    timestampTracker.removeStampedElement(elem);

    return elem;
  }

  /**
   * Returns the highest offset in the queue
   * @return offset
   */
  public long offset() {
    return offset;
  }

  /**
   * Returns the number of records in the queue
   * @return the number of records
   */
  public int size() {
    return queue.size();
  }

  /**
   * Tests if the queue is empty
   * @return true if the queue is empty, otherwise false
   */
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
