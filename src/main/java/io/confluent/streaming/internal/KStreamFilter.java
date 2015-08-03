package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamTopology;
import io.confluent.streaming.Predicate;

/**
 * Created by yasuhiro on 6/17/15.
 */
class KStreamFilter<K, V> extends KStreamImpl<K, V> {

  private final Predicate<K, V> predicate;

  KStreamFilter(Predicate<K, V> predicate, KStreamTopology initializer) {
    super(initializer);
    this.predicate = predicate;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp) {
    synchronized(this) {
      if (predicate.apply((K)key, (V)value)) {
        forward(key, value, timestamp);
      }
    }
  }

}
