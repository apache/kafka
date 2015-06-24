package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface KeyValueMapper<RK, RV, K, V> {

  KeyValue<RK, RV> apply(K key, V value);

}
