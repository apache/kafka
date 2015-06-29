package io.confluent.streaming.internal;

import io.confluent.streaming.RecordCollector;
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
public class RecordCollectors {

  private static final Logger log = LoggerFactory.getLogger(RecordCollectors.class);

  public static class SimpleRecordCollector implements RecordCollector<byte[], byte[]> {

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

    public SimpleRecordCollector(Producer<byte[], byte[]> producer) {
      this.producer = producer;
      this.offsets = new HashMap<TopicPartition, Long>();
    }

    public void send(ProducerRecord<byte[], byte[]> record) {
      // TODO: need to compute partition
      this.producer.send(record, callback);
    }

    /**
     * The last ack'd offset from the producer
     */
    public Map<TopicPartition, Long> offsets() {
      return this.offsets;
    }
  }

  public static class SerializingRecordCollector<K, V> implements RecordCollector<K, V> {

    private final RecordCollector collector;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public SerializingRecordCollector(RecordCollector collector, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
      super();
      this.keySerializer = keySerializer;
      this.valueSerializer = valueSerializer;
      this.collector = collector;
    }

    public void send(ProducerRecord<K, V> record) {
      byte[] keyBytes = keySerializer.serialize(record.topic(), record.key());
      byte[] valBytes = valueSerializer.serialize(record.topic(), record.value());
      collector.send(new ProducerRecord<byte[], byte[]>(record.topic(), keyBytes, valBytes));
    }
  }

}
