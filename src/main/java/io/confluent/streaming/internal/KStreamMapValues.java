package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamTopology;
import io.confluent.streaming.ValueMapper;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamMapValues<K, V, V1> extends KStreamImpl<K, V> {

  private final ValueMapper<V, V1> mapper;

  KStreamMapValues(ValueMapper<V, V1> mapper, KStreamTopology topology) {
    super(topology);
    this.mapper = mapper;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp) {
    synchronized (this) {
      V newValue = mapper.apply((V1)value);
      forward(key, newValue, timestamp);
    }
  }

}
