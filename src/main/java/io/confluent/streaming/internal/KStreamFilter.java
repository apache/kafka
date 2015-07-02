package io.confluent.streaming.internal;

import io.confluent.streaming.Predicate;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamFilter<K, V> extends KStreamImpl<K, V, K, V> {

  private final Predicate<K, V> predicate;

  KStreamFilter(Predicate<K, V> predicate, PartitioningInfo partitioningInfo, KStreamContextImpl context) {
    super(partitioningInfo, context);
    this.predicate = predicate;
  }

  @Override
  public void receive(K key, V value, long timestamp,long streamTime) {
    synchronized(this) {
      if (predicate.apply(key, value)) {
        forward(key, value, timestamp, streamTime);
      }
    }
  }

}
