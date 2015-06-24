package io.confluent.streaming;

import io.confluent.streaming.util.Stamped;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.PriorityQueue;

/**
 * Created by yasuhiro on 6/23/15.
 */
public class TimeBasedStreamSynchronizer implements StreamSynchronizer {

  private final TimestampExtractor timestampExtractor;
  private final PriorityQueue<Stamped<ConsumerRecord<Object, Object>>> pq = new PriorityQueue();
  private long streamTime = -1;

  public TimeBasedStreamSynchronizer(TimestampExtractor timestampExtractor) {
    this.timestampExtractor = timestampExtractor;
  }

  public void add(ConsumerRecord<Object, Object> record) {
    long timestamp = timestampExtractor.extract(record.topic(), record.key(), record.value());

    if (timestamp < 0) timestamp = System.currentTimeMillis();

    Stamped<ConsumerRecord<Object, Object>> stamped = new Stamped(record, timestamp);
    pq.offer(stamped);
  }

  public ConsumerRecord<Object, Object> next() {
    Stamped<ConsumerRecord<Object, Object>> stamped = pq.poll();

    if (stamped == null) return null;

    if (streamTime < stamped.timestamp) streamTime = stamped.timestamp;

    return stamped.value;
  }

  public long currentStreamTime() {
    return streamTime;
  }

  public void close() {
    pq.clear();
  }

}
