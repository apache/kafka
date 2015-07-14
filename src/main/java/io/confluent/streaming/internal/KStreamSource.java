package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;

import java.util.List;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamSource<K, V> extends KStreamImpl<K, V> {

  KStreamSource(KStreamMetadata streamMetadata, KStreamContext context) {
    super(streamMetadata, context);
  }

  @Override
  public void receive(String topic, Object key, Object value, long timestamp, long streamTime) {
    synchronized(this) {
      // KStream needs to forward the topic name since it is directly from the Kafka source
      forward(topic, key, value, timestamp, streamTime);
    }
  }

}
