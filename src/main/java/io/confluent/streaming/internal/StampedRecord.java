package io.confluent.streaming.internal;

import io.confluent.streaming.util.Stamped;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class StampedRecord<K, V> extends Stamped<ConsumerRecord<K, V>> {

  StampedRecord(ConsumerRecord<K, V> record, long timestamp) {
    super(record, timestamp);
  }

  public K key() {
    return value.key();
  }

  public V value() {
    return value.value();
  }

  public long offset() {
    return value.offset();
  }
}
