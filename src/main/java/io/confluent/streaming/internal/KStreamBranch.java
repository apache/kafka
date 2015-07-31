package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamInitializer;
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
  KStreamBranch(Predicate<K, V>[] predicates, KStreamInitializer initializer) {
    this.predicates = Arrays.copyOf(predicates, predicates.length);
    this.branches = (KStreamSource<K, V>[]) Array.newInstance(KStreamSource.class, predicates.length);
    for (int i = 0; i < branches.length; i++) {
      branches[i] = new KStreamSource<>(null, initializer);
    }
  }

  @Override
  public void bind(KStreamContext context, KStreamMetadata metadata) {
    for (KStreamSource stream : branches) {
      stream.bind(context, metadata);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp, long streamTime) {
    for (int i = 0; i < predicates.length; i++) {
      Predicate<K, V> predicate = predicates[i];
      if (predicate.apply((K)key, (V)value)) {
        branches[i].receive(key, value, timestamp, streamTime);
        return;
      }
    }
    return;
  }

}
