package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/22/15.
 */
public interface TimestampExtractor<K, V> {

  long extract(String topic, K key, V value);

}
