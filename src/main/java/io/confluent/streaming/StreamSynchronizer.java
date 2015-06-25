package io.confluent.streaming;

import io.confluent.streaming.internal.RecordQueueImpl;
import io.confluent.streaming.internal.RegulatedConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by yasuhiro on 6/23/15.
 */
public class StreamSynchronizer<K, V> {

  private final RegulatedConsumer<K, V> consumer;
  private final Chooser<K, V> chooser;
  private final TimestampExtractor timestampExtractor;
  private final Map<TopicPartition, RecordQueue<K, V>> stash = new HashMap<TopicPartition, RecordQueue<K, V>>();
  private final int desiredUnprocessed;

  private long streamTime = -1;
  private int buffered = 0;

  public StreamSynchronizer(RegulatedConsumer<K, V> consumer,
                            Chooser<K, V> chooser,
                            TimestampExtractor timestampExtractor,
                            int desiredNumberOfUnprocessedRecords) {
    this.consumer = consumer;
    this.chooser = chooser;
    this.timestampExtractor = timestampExtractor;
    this.desiredUnprocessed = desiredNumberOfUnprocessedRecords;
  }

  public void add(TopicPartition partition, Iterator<ConsumerRecord<K, V>> iterator) {
    RecordQueue<K, V> queue = stash.get(partition);
    if (queue == null) {
      queue = createRecordQueue(partition);
      this.stash.put(partition, queue);
    }

    boolean wasEmpty = (queue.size() == 0);

    while (iterator.hasNext()) {
      ConsumerRecord<K, V> record = iterator.next();
      queue.add(record, timestampExtractor.extract(record.topic(), record.key(), record.value()));
      buffered++;
    }

    if (wasEmpty && queue.size() > 0) chooser.add(queue);

    // if we have buffered enough for this partition, pause
    if (queue.size() > this.desiredUnprocessed) {
      consumer.pause(partition);
    }
  }

  public ConsumerRecord<K, V> next() {
    RecordQueue<K, V> queue = chooser.next();

    if (queue == null) {
      consumer.poll();
      return null;
    }

    if (queue.size() == this.desiredUnprocessed) {
      ConsumerRecord<K, V> record = queue.peekLast();
      if (record != null) {
        consumer.unpause(queue.partition(), record.offset());
      }
    }

    if (queue.size() == 0) return null;

    long timestamp = queue.currentStreamTime();
    ConsumerRecord<K, V> record = queue.next();

    if (streamTime < timestamp) streamTime = timestamp;

    if (queue.size() > 0) chooser.add(queue);
    buffered--;

    return record;
  }

  public long currentStreamTime() {
    return streamTime;
  }

  public int buffered() {
    return buffered;
  }

  public void close() {
    chooser.close();
    stash.clear();
  }

  protected RecordQueue<K, V> createRecordQueue(TopicPartition partition) {
    return new RecordQueueImpl<K, V>(partition);
  }

}
