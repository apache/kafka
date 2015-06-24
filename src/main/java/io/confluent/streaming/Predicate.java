package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Predicate<K, V> {

  boolean apply(K key, V value);

}
