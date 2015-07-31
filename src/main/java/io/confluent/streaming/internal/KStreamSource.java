package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamInitializer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamSource<K, V> extends KStreamImpl<K, V> {

  public final Deserializer<K> keyDeserializer;
  public final Deserializer<V> valueDeserializer;

  final String[] topics;

  @SuppressWarnings("unchecked")
  KStreamSource(String[] topics, KStreamInitializer initializer) {
    this(topics, (Deserializer<K>) initializer.keyDeserializer(), (Deserializer<V>) initializer.valueDeserializer(), initializer);
  }

  KStreamSource(String[] topics, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer, KStreamInitializer initializer) {
    super(initializer);
    this.topics = topics;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
  }

  @Override
  public void receive(Object key, Object value, long timestamp, long streamTime) {
    synchronized(this) {
      // KStream needs to forward the topic name since it is directly from the Kafka source
      forward(key, value, timestamp, streamTime);
    }
  }

}
