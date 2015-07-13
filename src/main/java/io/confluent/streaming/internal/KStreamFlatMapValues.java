package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.ValueMapper;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamFlatMapValues<K, V, V1> extends KStreamImpl<K, V> {

  private final ValueMapper<? extends Iterable<V>, V1> mapper;

  KStreamFlatMapValues(ValueMapper<? extends Iterable<V>, V1> mapper, KStreamMetadata streamMetadata, KStreamContext context) {
    super(streamMetadata, context);
    this.mapper = mapper;
  }

  @Override
  public void receive(String topic, Object key, Object value, long timestamp, long streamTime) {
    synchronized(this) {
      Iterable<V> newValues = mapper.apply((V1)value);
      for (V v : newValues) {
        forward(topic, key, v, timestamp, streamTime);
      }
    }
  }

}
