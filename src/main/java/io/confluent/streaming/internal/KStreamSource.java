package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamInitializer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamSource<K, V> extends KStreamImpl<K, V> {

  private Deserializer<K> keyDeserializer;
  private Deserializer<V> valueDeserializer;

  final String[] topics;

  KStreamSource(String[] topics, KStreamInitializer initializer) {
    this(topics, null, null, initializer);
  }

  KStreamSource(String[] topics, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, KStreamInitializer initializer) {
    super(initializer);
    this.topics = topics;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void bind(KStreamContext context, KStreamMetadata metadata) {
    if (keyDeserializer == null) keyDeserializer = (Deserializer<K>) context.keyDeserializer();
    if (valueDeserializer == null) valueDeserializer = (Deserializer<V>) context.valueDeserializer();

    super.bind(context, metadata);
  }

  @Override
  public void receive(Object key, Object value, long timestamp, long streamTime) {
    synchronized(this) {
      // KStream needs to forward the topic name since it is directly from the Kafka source
      forward(key, value, timestamp, streamTime);
    }
  }

  public Deserializer<K> keyDeserializer() {
    return keyDeserializer;
  }

  public Deserializer<V> valueDeserializer() {
    return valueDeserializer;
  }

}
