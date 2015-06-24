package io.confluent.streaming.internal;

import io.confluent.streaming.ValueMapper;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamMapValues<K, V, V1> extends KStreamImpl<K, V, K, V1> {

  private final ValueMapper<V, V1> mapper;

  KStreamMapValues(ValueMapper<V, V1> mapper, PartitioningInfo partitioningInfo, KStreamContextImpl context) {
    super(partitioningInfo, context);
    this.mapper = mapper;
  }

  public void receive(K key, V1 value, long timestamp) {
    synchronized (this) {
      V newValue = mapper.apply(value);
      forward(key, newValue, timestamp);
    }
  }

}
