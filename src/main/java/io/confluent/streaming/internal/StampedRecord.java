package io.confluent.streaming.internal;

import io.confluent.streaming.util.Stamped;
import org.apache.kafka.clients.consumer.ConsumerRecord;


// TODO: making this class exposed to user in the lower-level Processor
public class StampedRecord extends Stamped<ConsumerRecord<Object, Object>> {

  StampedRecord(ConsumerRecord<Object, Object> record, long timestamp) {
    super(record, timestamp);
  }

  public String topic() {
    if (value.topic().equals(KStreamMetadata.UNKNOWN_TOPICNAME))
      return null;
    else
      return value.topic();
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
