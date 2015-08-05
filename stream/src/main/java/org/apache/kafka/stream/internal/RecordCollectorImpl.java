package org.apache.kafka.stream.internal;

import org.apache.kafka.stream.RecordCollector;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yasuhiro on 6/26/15.
 */
public class RecordCollectorImpl implements RecordCollector {

  private static final Logger log = LoggerFactory.getLogger(RecordCollectorImpl.class);

  private final Producer<byte[], byte[]> producer;
  private final Map<TopicPartition, Long> offsets;
  private final Callback callback = new Callback(){
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if(exception == null) {
        TopicPartition tp = new TopicPartition(metadata.topic(), metadata.partition());
        offsets.put(tp, metadata.offset());
      } else {
        log.error("Error sending record: ", exception);
      }
    }
  };
  private final Serializer<Object> keySerializer;
  private final Serializer<Object> valueSerializer;


  public RecordCollectorImpl(Producer<byte[], byte[]> producer, Serializer<Object> keySerializer, Serializer<Object> valueSerializer) {
    this.producer = producer;
    this.offsets = new HashMap<>();
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  @Override
  public void send(ProducerRecord<Object, Object> record) {
    send(record, this.keySerializer, this.valueSerializer);
  }

  @Override
  public <K, V> void send(ProducerRecord<K, V> record, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    byte[] keyBytes = keySerializer.serialize(record.topic(), record.key());
    byte[] valBytes = valueSerializer.serialize(record.topic(), record.value());
    this.producer.send(new ProducerRecord<>(record.topic(), keyBytes, valBytes), callback);
  }

  @Override
  public void flush() {
    this.producer.flush();
  }

  /**
   * Closes this RecordCollector
   */
  public void close() {
    producer.close();
  }

  /**
   * The last ack'd offset from the producer
   * @return the map from TopicPartition to offset
   */
  Map<TopicPartition, Long> offsets() {
    return this.offsets;
  }
}
