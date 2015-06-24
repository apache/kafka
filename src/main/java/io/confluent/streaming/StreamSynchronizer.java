package io.confluent.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Created by yasuhiro on 6/23/15.
 */
public interface StreamSynchronizer {

  void add(ConsumerRecord<Object, Object> record);

  ConsumerRecord<Object, Object> next();

  long currentStreamTime();

  void close();

}
