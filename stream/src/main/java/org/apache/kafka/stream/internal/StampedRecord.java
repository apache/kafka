package org.apache.kafka.stream.internal;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.stream.util.Stamped;


// TODO: making this class exposed to user in the lower-level Processor
public class StampedRecord extends Stamped<ConsumerRecord<Object, Object>> {

  StampedRecord(ConsumerRecord<Object, Object> record, long timestamp) {
    super(record, timestamp);
  }

  public String topic() { return value.topic(); }

  public int partition() { return value.partition(); }

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
