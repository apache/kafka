package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.Predicate;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Created by yasuhiro on 6/18/15.
 */
class KStreamBranch<K, V> implements Receiver {

  private final Predicate<K, V>[] predicates;
  final KStreamSource<K, V>[] branches;

  @SuppressWarnings("unchecked")
  KStreamBranch(Predicate<K, V>[] predicates, KStreamMetadata streamMetadata, KStreamContext context) {
    this.predicates = Arrays.copyOf(predicates, predicates.length);
    this.branches = (KStreamSource<K, V>[]) Array.newInstance(KStreamSource.class, predicates.length);
    for (int i = 0; i < branches.length; i++) {
      branches[i] = new KStreamSource<K, V>(streamMetadata, context);
    }
  }

  @Override
  public void receive(String topic, Object key, Object value, long timestamp, long streamTime) {
    synchronized(this) {
      for (int i = 0; i < predicates.length; i++) {
        Predicate<K, V> predicate = predicates[i];
        if (predicate.apply((K)key, (V)value)) {
          branches[i].receive(topic, key, value, timestamp, streamTime);
          return;
        }
      }
      return;
    }
  }

}
