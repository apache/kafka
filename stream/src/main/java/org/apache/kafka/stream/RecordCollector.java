package org.apache.kafka.stream;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Created by yasuhiro on 6/19/15.
 */
public interface RecordCollector {

  void send(ProducerRecord<Object, Object> record);

  <K, V> void send(ProducerRecord<K, V> record, Serializer<K> keySerializer, Serializer<V> valueSerializer);

  void flush();
}
