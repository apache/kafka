package io.confluent.streaming.internal;

import io.confluent.streaming.util.Stamped;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class StampedRecord extends Stamped<ConsumerRecord<Object, Object>> {

  StampedRecord(ConsumerRecord<Object, Object> record, long timestamp) {
    super(record, timestamp);
  }

  public Object key() {
    return value.key();
  }

  public Object value() {
    return value.value();
  }

  public long offset() {
    return value.offset();
  }
}
