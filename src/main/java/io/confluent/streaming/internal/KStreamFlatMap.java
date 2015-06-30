package io.confluent.streaming.internal;

import io.confluent.streaming.KeyValueMapper;
import io.confluent.streaming.KeyValue;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamFlatMap<K, V, K1, V1> extends KStreamImpl<K, V, K1, V1> {

  private final KeyValueMapper<K, ? extends Iterable<V>, K1, V1> mapper;

  KStreamFlatMap(KeyValueMapper<K, ? extends Iterable<V>, K1, V1> mapper, KStreamContextImpl context) {
    super(PartitioningInfo.missing, context);
    this.mapper = mapper;
  }

  @Override
  public void receive(K1 key, V1 value, long timestamp) {
    synchronized(this) {
      KeyValue<K, ? extends Iterable<V>> newPair = mapper.apply(key, value);
      for (V v : newPair.value) {
        forward(newPair.key, v, timestamp);
      }
    }
  }

}
