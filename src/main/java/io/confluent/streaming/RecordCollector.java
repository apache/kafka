package io.confluent.streaming;

import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by yasuhiro on 6/19/15.
 */
public interface RecordCollector<K, V> {

  void send(ProducerRecord<K, V> record);

}
