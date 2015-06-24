package io.confluent.streaming.internal;

import io.confluent.streaming.ValueMapper;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamFlatMapValues<K, V, V1> extends KStreamImpl<K, V, K, V1> {

  private final ValueMapper<? extends Iterable<V>, V1> mapper;

  KStreamFlatMapValues(ValueMapper<? extends Iterable<V>, V1> mapper, PartitioningInfo partitioningInfo, KStreamContextImpl context) {
    super(partitioningInfo, context);
    this.mapper = mapper;
  }

  public void receive(K key, V1 value, long timestamp) {
    synchronized(this) {
      Iterable<V> newValues = mapper.apply(value);
      for (V v : newValues) {
        forward(key, v, timestamp);
      }
    }
  }

}
