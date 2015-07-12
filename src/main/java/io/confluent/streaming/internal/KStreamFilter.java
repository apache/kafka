package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.Predicate;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamFilter<K, V> extends KStreamImpl<K, V> {

  private final Predicate<K, V> predicate;

  KStreamFilter(Predicate<K, V> predicate, PartitioningInfo partitioningInfo, KStreamContext context) {
    super(partitioningInfo, context);
    this.predicate = predicate;
  }

  @Override
  public void receive(Object key, Object value, long timestamp,long streamTime) {
    synchronized(this) {
      if (predicate.apply((K)key, (V)value)) {
        forward(key, value, timestamp, streamTime);
      }
    }
  }

}
