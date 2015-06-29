package io.confluent.streaming.internal;

import io.confluent.streaming.Predicate;

import java.util.Arrays;

/**
 * Created by yasuhiro on 6/18/15.
 */
class KStreamBranch<K, V> implements Receiver<K, V> {

  private final Predicate<K, V>[] predicates;
  final KStreamSource<K, V>[] branches;

  KStreamBranch(Predicate<K, V>[] predicates, PartitioningInfo partitioningInfo, KStreamContextImpl context) {
    this.predicates = Arrays.copyOf(predicates, predicates.length);
    branches = (KStreamSource<K, V>[]) new Object[predicates.length + 1];
    for (int i = 0; i < branches.length; i++) {
      branches[i] = new KStreamSource<K, V>(partitioningInfo, context);
    }
  }

  public void receive(K key, V value, long timestamp) {
    synchronized(this) {
      for (int i = 0; i < predicates.length; i++) {
        Predicate<K, V> predicate = predicates[i];
        if (predicate.apply(key, value)) {
          branches[i].receive(key, value, timestamp);
          return;
        }
      }
      branches[branches.length - 1].receive(key, value, timestamp);
      return;
    }
  }

  public void flush() {
    for (KStreamSource<K, V> branch : branches) {
      branch.flush();
    }
  }

}
