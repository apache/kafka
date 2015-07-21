package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamSource<K, V> extends KStreamImpl<K, V> {

  public final Deserializer<K> keyDeserializer;
  public final Deserializer<V> valueDeserializer;

  KStreamSource(KStreamMetadata streamMetadata, KStreamContext context) {
    this(streamMetadata, context, null, null);
  }

  KStreamSource(KStreamMetadata streamMetadata, KStreamContext context, Deserializer < K > keyDeserializer, Deserializer < V > valueDeserializer) {
    super(streamMetadata, context);

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
