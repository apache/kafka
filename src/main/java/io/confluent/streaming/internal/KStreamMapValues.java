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

  @Override
  public void receive(Object key, Object value, long timestamp, long streamTime) {
    synchronized (this) {
      V newValue = mapper.apply((V1)value);
      forward(key, newValue, timestamp, streamTime);
    }
  }

}
